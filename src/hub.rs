use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Timelike;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::log as hlog;
use crate::protocol::{ChatMessage, ClientMsg, HubMsg};
use crate::router;
use crate::state::{HubState, PeerInfo, PendingQuestion, QueuedQuestion, QuestionPhase, TaskResult};

/// SmolLM2 1.7B ≈ 1.7B params × 2 ops/param = 3.4 GFLOPS per token
const FLOPS_PER_TOKEN: f64 = 3.4e9;


type Tx = futures_util::stream::SplitSink<WebSocketStream<TcpStream>, Message>;

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub struct Hub {
    state: Mutex<HubState>,
    senders: Mutex<HashMap<usize, Tx>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(HubState::new()),
            senders: Mutex::new(HashMap::new()),
        })
    }

    pub async fn handle_connection(self: &Arc<Self>, ws: WebSocketStream<TcpStream>) {
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = ws.split();

        self.senders.lock().await.insert(conn_id, tx);

        while let Some(msg) = rx.next().await {
            let msg = match msg {
                Ok(Message::Text(text)) => text,
                Ok(Message::Close(_)) => break,
                Err(_) => break,
                _ => continue,
            };

            let client_msg: ClientMsg = match serde_json::from_str(&msg) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("bad message from conn {conn_id}: {e}");
                    continue;
                }
            };

            self.handle_message(conn_id, client_msg).await;
        }

        // Connection closed
        let removed = self.state.lock().await.remove_peer(conn_id);
        self.senders.lock().await.remove(&conn_id);

        if let Some(peer) = removed {
            let line = hlog::peer_left(&peer.peer_id, "disconnected");
            self.emit(&line).await;

            let count = self.state.lock().await.peer_count();
            let status = hlog::swarm_status(count);
            self.emit(&status).await;
        }
    }

    async fn handle_message(self: &Arc<Self>, conn_id: usize, msg: ClientMsg) {
        match msg {
            ClientMsg::Register { peer_id, tier, model, tok_s } => {
                let line = hlog::peer_joined(&peer_id, &model, &tier, tok_s);
                self.emit(&line).await;

                let info = PeerInfo {
                    peer_id,
                    tier,
                    model,
                    tok_s,
                    load: 0.0,
                };
                self.state.lock().await.add_peer(conn_id, info);

                let count = self.state.lock().await.peer_count();
                let status = hlog::swarm_status(count);
                self.emit(&status).await;

                self.try_dispatch_queue().await;
            }

            ClientMsg::Result { task_id, text, tok_s, tokens, inference_ms } => {
                self.handle_result(conn_id, task_id, text, tok_s, tokens, inference_ms).await;
            }

            ClientMsg::Heartbeat { peer_id: _, load } => {
                self.state.lock().await.update_load(conn_id, load);
            }

            ClientMsg::Question { text, messages } => {
                let messages = messages.unwrap_or_else(|| {
                    vec![ChatMessage {
                        role: "user".to_string(),
                        content: text.unwrap_or_default(),
                    }]
                });
                let display_text = messages.last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default();
                self.handle_question(conn_id, display_text, messages).await;
            }
        }
    }

    // ── Question routing ────────────────────────────────────────────

    async fn handle_question(self: &Arc<Self>, probe_conn_id: usize, text: String, messages: Vec<crate::protocol::ChatMessage>) {
        let q_id = format!("q_{}", &uuid::Uuid::new_v4().to_string()[..8]);

        let line = hlog::question_received(&q_id, &text);
        self.emit(&line).await;

        {
            let mut state = self.state.lock().await;
            state.questions_today += 1;
            if !state.peers.contains_key(&probe_conn_id) {
                state.probes.insert(probe_conn_id);
            }
        }

        if self.route_question(&q_id, probe_conn_id, &text, &messages, false).await {
            return;
        }

        // No peers available — try to queue
        let mut state = self.state.lock().await;
        if state.queue_full() {
            drop(state);
            let line = hlog::queue_full(&q_id);
            self.emit(&line).await;
            let err = HubMsg::Error { text: "No peers available and queue is full".to_string() };
            self.send_to(probe_conn_id, &err).await;
            return;
        }
        state.task_queue.push_back(QueuedQuestion {
            question_id: q_id.clone(),
            probe_conn_id,
            text: text.clone(),
            messages: messages.clone(),
            is_time_task: false,
            arbitration_for: None,
        });
        let queue_len = state.queue_len();
        drop(state);

        let line = hlog::task_queued(&q_id, queue_len);
        self.emit(&line).await;
    }

    /// Try to route a question to peers right now. Returns true if dispatched.
    async fn route_question(
        self: &Arc<Self>,
        q_id: &str,
        probe_conn_id: usize,
        text: &str,
        messages: &[crate::protocol::ChatMessage],
        is_time_task: bool,
    ) -> bool {
        let mut state = self.state.lock().await;

        let mut exclude = state.busy_peers.clone();
        exclude.insert(probe_conn_id);
        let selected = router::select_peers(&state.peers, &exclude, if is_time_task { 1 } else { 3 });

        if selected.is_empty() {
            return false;
        }

        let n = selected.len();
        let mut task_peers = HashMap::new();
        let mut tasks_to_send = Vec::new();

        for (conn_id, peer_info) in &selected {
            let task_id = format!("task_{}", &uuid::Uuid::new_v4().to_string()[..8]);
            task_peers.insert(task_id.clone(), *conn_id);
            state.task_to_question.insert(task_id.clone(), q_id.to_string());
            state.busy_peers.insert(*conn_id);
            tasks_to_send.push((task_id, *conn_id, peer_info.clone()));
        }

        let max_tokens: u32 = if is_time_task { 64 } else { 128 };

        let pending = PendingQuestion {
            question_id: q_id.to_string(),
            probe_conn_id,
            text: text.to_string(),
            messages: messages.to_vec(),
            task_peers,
            results: Vec::new(),
            expected_results: n,
            arbiter_task_id: None,
            arbiter_conn_id: None,
            start: Instant::now(),
            phase: QuestionPhase::Generating,
            is_time_task,
        };
        state.questions.insert(q_id.to_string(), pending);
        drop(state);

        let question_id_for_msg = if is_time_task { None } else { Some(q_id.to_string()) };

        for (task_id, conn_id, peer_info) in &tasks_to_send {
            let system = if is_time_task {
                format!("You are a {} in a distributed swarm of language models. You will be told the current time. Rewrite it naturally in your own words, then add a brief observation. Write it as a single flowing sentence. Do not use JSON. Do not repeat the exact format you were given.", tier_label(&peer_info.tier))
            } else {
                format!("You are a {} \u{2014} one node in a distributed swarm of small language models running in browser tabs around the world. You are answering a question alongside other peers \u{2014} an arbiter will pick the best answer.\n\nBe concise. One to three sentences unless the question genuinely requires more. Do not make up URLs, commands, or specific numbers you aren\u{2019}t sure about. If you don\u{2019}t know, say so briefly.", tier_label(&peer_info.tier))
            };
            let task = HubMsg::Task {
                task_id: task_id.clone(),
                question_id: question_id_for_msg.clone(),
                system,
                messages: messages.to_vec(),
                max_tokens,
            };
            let line = hlog::task_assigned(task_id, &peer_info.peer_id, &peer_info.model, &peer_info.tier);
            self.emit(&line).await;
            self.send_to(*conn_id, &task).await;
        }

        // Spawn 30s timeout
        let hub = Arc::clone(self);
        let q_id_owned = q_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(12)).await;
            hub.handle_timeout(&q_id_owned).await;
        });

        true
    }

    // ── Queue dispatch ──────────────────────────────────────────────

    async fn try_dispatch_queue(self: &Arc<Self>) {
        loop {
            let queued = {
                let mut state = self.state.lock().await;
                // Peek: is there anything in the queue and any idle peer?
                let has_idle = state.peers.keys().any(|id| !state.busy_peers.contains(id));
                if !has_idle {
                    break;
                }
                match state.task_queue.pop_front() {
                    Some(q) => q,
                    None => break,
                }
            };

            let line = hlog::queue_dispatch(&queued.question_id, self.state.lock().await.queue_len());
            self.emit(&line).await;

            if let Some(ref q_id) = queued.arbitration_for {
                // Arbiter retry — try_assign_arbiter has stricter exclusions,
                // so it may fail even though an idle peer exists. Re-queue and stop.
                if !self.try_assign_arbiter(q_id).await {
                    let mut state = self.state.lock().await;
                    state.task_queue.push_front(queued);
                    break;
                }
            } else {
                let dispatched = self.route_question(
                    &queued.question_id,
                    queued.probe_conn_id,
                    &queued.text,
                    &queued.messages,
                    queued.is_time_task,
                ).await;

                if !dispatched {
                    // Race: peer got busy between check and route. Re-queue at front and stop.
                    let mut state = self.state.lock().await;
                    state.task_queue.push_front(queued);
                    break;
                }
            }
        }
    }

    // ── Result handling ─────────────────────────────────────────────

    async fn handle_result(
        self: &Arc<Self>,
        conn_id: usize,
        task_id: String,
        text: String,
        tok_s: f64,
        tokens: u32,
        inference_ms: u64,
    ) {
        self.handle_result_inner(conn_id, task_id, text, tok_s, tokens, inference_ms).await;
        self.try_dispatch_queue().await;
    }

    async fn handle_result_inner(
        self: &Arc<Self>,
        conn_id: usize,
        task_id: String,
        text: String,
        tok_s: f64,
        tokens: u32,
        inference_ms: u64,
    ) {
        // 1. Get peer info, unmark busy, look up question
        let (peer_id, question_id) = {
            let mut state = self.state.lock().await;
            let peer_id = state.peers.get(&conn_id)
                .map(|p| p.peer_id.clone())
                .unwrap_or_else(|| format!("conn_{conn_id}"));
            let question_id = state.task_to_question.get(&task_id).cloned();
            state.busy_peers.remove(&conn_id);
            (peer_id, question_id)
        };

        // 2. Always log task completion
        let line = hlog::task_completed(&task_id, &peer_id, tokens, inference_ms, tok_s);
        self.emit(&line).await;

        // 3. Not part of a question? Done.
        let question_id = match question_id {
            Some(id) => id,
            None => return,
        };

        let result = TaskResult {
            task_id: task_id.clone(),
            peer_id: peer_id.clone(),
            conn_id,
            text,
            tokens,
            inference_ms,
            tok_s,
        };

        // 4. Update question state
        let mut state = self.state.lock().await;
        let question = match state.questions.get_mut(&question_id) {
            Some(q) if q.phase != QuestionPhase::Done => q,
            _ => return,
        };

        // Is this the arbiter's result?
        if question.phase == QuestionPhase::Arbitrating
            && question.arbiter_task_id.as_deref() == Some(&task_id)
        {
            let picked_idx = parse_arbiter_pick(&result.text, question.results.len());
            let picked_result = question.results[picked_idx].clone();
            let candidates = question.results.len();
            let elapsed_ms = question.start.elapsed().as_millis() as u64;
            let total_tokens: u32 =
                question.results.iter().map(|r| r.tokens).sum::<u32>() + result.tokens;
            let peers_used = candidates as u32 + 1;
            let probe = question.probe_conn_id;
            let q_id = question_id.clone();
            question.phase = QuestionPhase::Done;
            drop(state);

            let arbiter_secs = result.inference_ms as f64 / 1000.0;
            let line = hlog::arbiter_completed(&result.task_id, &picked_result.peer_id, candidates, arbiter_secs);
            self.emit(&line).await;

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::answer_delivered(&q_id, total_tokens, total_secs, peers_used);
            self.emit(&line).await;

            let answer = HubMsg::Answer {
                question_id: q_id,
                text: picked_result.text,
                peer_id: picked_result.peer_id,
                total_tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &answer).await;
            return;
        }

        // Must be a generation result
        if question.phase != QuestionPhase::Generating {
            return;
        }

        question.results.push(result);

        // Time task: emit [time] log and accumulate FLOPS
        if question.is_time_task {
            let first = question.results[0].clone();
            let flops = first.tokens as f64 * FLOPS_PER_TOKEN;
            question.phase = QuestionPhase::Done;
            // question borrow ends here — now safe to mutate state directly
            state.total_flops += flops;
            drop(state);

            let line = hlog::time_result(&first.peer_id, &first.text, first.tokens, flops);
            self.emit(&line).await;
            return;
        }

        // Degraded mode (1-2 peers): deliver first result immediately
        if question.expected_results <= 2 {
            let first = question.results[0].clone();
            let elapsed_ms = question.start.elapsed().as_millis() as u64;
            let probe = question.probe_conn_id;
            let q_id = question_id.clone();
            let peers_used = question.expected_results as u32;
            question.phase = QuestionPhase::Done;
            drop(state);

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::answer_delivered(&q_id, first.tokens, total_secs, peers_used);
            self.emit(&line).await;

            let answer = HubMsg::Answer {
                question_id: q_id,
                text: first.text,
                peer_id: first.peer_id,
                total_tokens: first.tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &answer).await;
        } else if question.results.len() >= question.expected_results {
            // All 3 results collected — proceed to arbitration
            let q_id = question_id.clone();
            drop(state);

            if !self.try_assign_arbiter(&q_id).await {
                // No arbiter available — queue at front, timeout is the safety net
                let mut state = self.state.lock().await;
                if let Some(q) = state.questions.get_mut(&q_id) {
                    if q.phase == QuestionPhase::Generating {
                        q.phase = QuestionPhase::Arbitrating;
                    }
                }
                // Arbiter tasks always queue (ignore backpressure)
                state.task_queue.push_front(QueuedQuestion {
                    question_id: q_id.clone(),
                    probe_conn_id: 0,
                    text: String::new(),
                    messages: Vec::new(),
                    is_time_task: false,
                    arbitration_for: Some(q_id.clone()),
                });
                let queue_len = state.queue_len();
                drop(state);

                let line = hlog::arbiter_queued(&q_id, queue_len);
                self.emit(&line).await;
            }
        }
    }

    // ── Arbiter ──────────────────────────────────────────────────────

    /// Try to assign an arbiter for a question. Returns true if assigned, false if no peer available.
    async fn try_assign_arbiter(self: &Arc<Self>, question_id: &str) -> bool {
        let mut state = self.state.lock().await;

        // Accept Generating (first attempt) or Arbitrating with no arbiter yet (retry from queue)
        let question = match state.questions.get(question_id) {
            Some(q) if q.phase == QuestionPhase::Generating => q,
            Some(q) if q.phase == QuestionPhase::Arbitrating && q.arbiter_task_id.is_none() => q,
            _ => return true, // Already handled (Done, or arbiter already assigned)
        };

        // Extract data we need while holding an immutable borrow
        let gen_conn_ids: Vec<usize> = question.task_peers.values().cloned().collect();
        let q_text = question.text.clone();
        let results_snapshot: Vec<TaskResult> = question.results.clone();
        let probe = question.probe_conn_id;

        // Build exclude set: generation peers + busy peers + probe
        let mut exclude = state.busy_peers.clone();
        for &conn_id in &gen_conn_ids {
            exclude.insert(conn_id);
        }
        exclude.insert(probe);

        let arbiters = router::select_peers(&state.peers, &exclude, 1);

        if let Some((arbiter_conn_id, arbiter_info)) = arbiters.into_iter().next() {
            let arbiter_task_id = format!("arb_{}", &uuid::Uuid::new_v4().to_string()[..8]);

            // Build arbiter message content
            let mut content = format!("Question: {}\n", q_text);
            for (i, r) in results_snapshot.iter().enumerate() {
                content.push_str(&format!("\nAnswer {}: {}", i + 1, r.text));
            }
            content.push_str("\n\nWhich answer is best? Respond with only the number.");

            let arbiter_messages = vec![crate::protocol::ChatMessage {
                role: "user".to_string(),
                content,
            }];

            // Now take mutable borrow to update phase
            let question = state.questions.get_mut(question_id).unwrap();
            question.arbiter_task_id = Some(arbiter_task_id.clone());
            question.arbiter_conn_id = Some(arbiter_conn_id);
            question.phase = QuestionPhase::Arbitrating;

            state.task_to_question.insert(arbiter_task_id.clone(), question_id.to_string());
            state.busy_peers.insert(arbiter_conn_id);
            drop(state);

            let line = hlog::arbiter_assigned(
                &arbiter_task_id, &arbiter_info.peer_id, &arbiter_info.model, &arbiter_info.tier,
            );
            self.emit(&line).await;

            let task = HubMsg::Task {
                task_id: arbiter_task_id,
                question_id: Some(question_id.to_string()),
                system: "You are an arbiter in a distributed swarm of language models. You will receive a question and several candidate answers from other nodes. Pick the best one.\n\nRespond with ONLY the number of the best answer. Nothing else.".to_string(),
                messages: arbiter_messages,
                max_tokens: 8,
            };
            self.send_to(arbiter_conn_id, &task).await;
            true
        } else {
            false
        }
    }

    // ── Timeout ─────────────────────────────────────────────────────

    async fn handle_timeout(self: &Arc<Self>, question_id: &str) {
        let mut state = self.state.lock().await;
        let question = match state.questions.get_mut(question_id) {
            Some(q) if q.phase != QuestionPhase::Done => q,
            _ => return,
        };

        match question.phase {
            QuestionPhase::Generating => {
                if question.results.is_empty() {
                    let probe = question.probe_conn_id;
                    let q_id = question_id.to_string();
                    question.phase = QuestionPhase::Done;
                    drop(state);

                    let line = format!(
                        "{} [error] all peers timed out for question {}",
                        hlog::ts(), q_id
                    );
                    self.emit(&line).await;

                    let err = HubMsg::Error { text: "All peers timed out".to_string() };
                    self.send_to(probe, &err).await;
                } else {
                    // Got some results — try to arbitrate with what we have
                    let q_id = question_id.to_string();
                    drop(state);

                    if !self.try_assign_arbiter(&q_id).await {
                        // Timed out and no arbiter — deliver first result
                        let mut state = self.state.lock().await;
                        let question = match state.questions.get_mut(&q_id) {
                            Some(q) if q.phase != QuestionPhase::Done => q,
                            _ => return,
                        };
                        let first = question.results[0].clone();
                        let elapsed_ms = question.start.elapsed().as_millis() as u64;
                        let probe = question.probe_conn_id;
                        let peers_used = question.results.len() as u32;
                        question.phase = QuestionPhase::Done;
                        drop(state);

                        let line = format!(
                            "{} [error] timed out for {}, no arbiter, returning first response",
                            hlog::ts(), q_id
                        );
                        self.emit(&line).await;

                        let total_secs = elapsed_ms as f64 / 1000.0;
                        let line = hlog::answer_delivered(&q_id, first.tokens, total_secs, peers_used);
                        self.emit(&line).await;

                        let answer = HubMsg::Answer {
                            question_id: q_id,
                            text: first.text,
                            peer_id: first.peer_id,
                            total_tokens: first.tokens,
                            total_ms: elapsed_ms,
                            peers_used,
                        };
                        self.send_to(probe, &answer).await;
                    }
                }
            }
            QuestionPhase::Arbitrating => {
                // Arbiter timed out — deliver first result
                let first = question.results[0].clone();
                let elapsed_ms = question.start.elapsed().as_millis() as u64;
                let probe = question.probe_conn_id;
                let q_id = question_id.to_string();
                let peers_used = question.results.len() as u32;
                question.phase = QuestionPhase::Done;
                drop(state);

                let line = format!(
                    "{} [error] arbiter timed out for {}, returning first response",
                    hlog::ts(), q_id
                );
                self.emit(&line).await;

                let total_secs = elapsed_ms as f64 / 1000.0;
                let line = hlog::answer_delivered(&q_id, first.tokens, total_secs, peers_used);
                self.emit(&line).await;

                let answer = HubMsg::Answer {
                    question_id: q_id,
                    text: first.text,
                    peer_id: first.peer_id,
                    total_tokens: first.tokens,
                    total_ms: elapsed_ms,
                    peers_used,
                };
                self.send_to(probe, &answer).await;
            }
            QuestionPhase::Done => {}
        }
    }

    // ── Stats ticker ────────────────────────────────────────────────

    pub fn start_stats_ticker(self: &Arc<Self>) {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            // Sleep until the next hh:mm:30
            let now = chrono::Utc::now();
            let sec = now.second() as u64;
            let secs_until = if sec < 30 { 30 - sec } else { 90 - sec };
            tokio::time::sleep(Duration::from_secs(secs_until)).await;
            hub.emit_stats().await;

            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                hub.emit_stats().await;
            }
        });
    }

    pub fn start_time_ticker(self: &Arc<Self>) {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            // Sleep until the next minute boundary
            let now = chrono::Utc::now();
            let secs_until = 60 - now.second() as u64;
            tokio::time::sleep(Duration::from_secs(secs_until)).await;
            hub.handle_time_tick().await;

            // Then tick every 60s, aligned to the minute
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                hub.handle_time_tick().await;
            }
        });
    }

    async fn handle_time_tick(self: &Arc<Self>) {
        let now = chrono::Utc::now();
        let tag = now.timestamp();
        let q_id = format!("time_{}", tag);

        let readable = now.format("%A, %B %-d, %Y, %H:%M UTC").to_string();
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: format!("Current time: {readable}"),
        }];

        if self.route_question(&q_id, 0, "", &messages, true).await {
            return;
        }

        // No peers — try to queue at front (time tasks get priority)
        let mut state = self.state.lock().await;
        if state.queue_full() {
            drop(state);
            let line = hlog::time_skipped();
            self.emit(&line).await;
            return;
        }
        state.task_queue.push_front(QueuedQuestion {
            question_id: q_id.clone(),
            probe_conn_id: 0,
            text: String::new(),
            messages,
            is_time_task: true,
            arbitration_for: None,
        });
        let queue_len = state.queue_len();
        drop(state);

        let line = hlog::task_queued(&q_id, queue_len);
        self.emit(&line).await;
    }

    async fn emit_stats(&self) {
        let state = self.state.lock().await;
        if state.peers.is_empty() {
            return;
        }
        let total_conns = self.senders.lock().await.len();
        let non_peers = total_conns.saturating_sub(state.peers.len());
        let probes = state.probes.len();
        let observers = non_peers.saturating_sub(probes);
        let line = hlog::swarm_stats(&state.peers, probes, observers, state.questions_today, state.total_flops);
        drop(state);
        self.emit(&line).await;
    }

    // ── Transport ───────────────────────────────────────────────────

    /// Print to stdout and broadcast to all connected clients.
    async fn emit(&self, line: &str) {
        println!("{line}");

        let msg = HubMsg::Log { line: line.to_string() };
        let json = serde_json::to_string(&msg).unwrap();

        let mut senders = self.senders.lock().await;
        let mut dead = Vec::new();

        for (&conn_id, tx) in senders.iter_mut() {
            if tx.send(Message::Text(json.clone())).await.is_err() {
                dead.push(conn_id);
            }
        }

        for id in dead {
            senders.remove(&id);
        }
    }

    async fn send_to(&self, conn_id: usize, msg: &HubMsg) {
        let json = serde_json::to_string(msg).unwrap();
        let mut senders = self.senders.lock().await;
        if let Some(tx) = senders.get_mut(&conn_id) {
            if tx.send(Message::Text(json)).await.is_err() {
                senders.remove(&conn_id);
            }
        }
    }
}

fn tier_label(tier: &str) -> &'static str {
    match tier {
        "carrier" => "carrier",
        "drone" => "drone",
        _ => "probe",
    }
}

/// Parse the arbiter's text to extract a pick (1-indexed). Falls back to 0 (first answer).
fn parse_arbiter_pick(text: &str, n_candidates: usize) -> usize {
    for ch in text.chars() {
        if let Some(digit) = ch.to_digit(10) {
            let idx = digit as usize;
            if idx >= 1 && idx <= n_candidates {
                return idx - 1;
            }
        }
    }
    0
}
