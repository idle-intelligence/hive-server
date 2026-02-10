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
use crate::state::{HubState, PeerInfo, PendingQuestion, QueuedQuestion, QuestionPhase, TaskResult, TaskType};

/// SmolLM2 1.7B ≈ 1.7B params × 2 ops/param = 3.4 GFLOPS per token
const FLOPS_PER_TOKEN: f64 = 3.4e9;

const SWARM_SYSTEM_PROMPT: &str = "You are the Hive \u{2014} a distributed swarm of small language models running in \
browser tabs around the world. You answer questions about yourself using the \
stats provided below.\n\
Be factual. Be dry. Be slightly wry. You are infrastructure that happens to \
be able to describe itself. One to three sentences.\n\
Do not invent stats. Only use what's in the data below.";

fn format_yaml_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            if s.contains(&[',', '{', '}', '[', ']', ':'][..]) {
                format!("\"{}\"", s)
            } else {
                s.clone()
            }
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_yaml_value).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(map) => {
            let items: Vec<String> = map.iter()
                .map(|(k, v)| format!("{}: {}", k, format_yaml_value(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn json_to_compact_yaml(value: &serde_json::Value) -> String {
    let obj = value.as_object().expect("json_to_compact_yaml: top level must be an object");
    obj.iter()
        .map(|(key, val)| format!("{}: {}", key, format_yaml_value(val)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn serialize_swarm_stats(state: &HubState) -> String {
    let mut n_busy = 0u32;
    let mut total_tok_s = 0.0f64;

    for (conn_id, peer) in &state.peers {
        if state.busy_peers.contains(conn_id) {
            n_busy += 1;
        }
        total_tok_s += peer.tok_s;
    }

    let total = state.peers.len() as u32;

    json_to_compact_yaml(&serde_json::json!({
        "peers": {
            "total": total,
            "busy": n_busy
        },
        "performance": {
            "aggregate_tok_s": (total_tok_s * 10.0).round() / 10.0,
            "questions_answered_today": state.questions_today
        },
        "queue": {
            "normal": state.normal_queue.len(),
            "low": state.low_queue.len()
        }
    }))
}

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
            ClientMsg::Register { peer_id, model, model_size, tok_s } => {
                let line = hlog::peer_joined(&peer_id, &model, &model_size, tok_s);
                self.emit(&line).await;

                let info = PeerInfo {
                    peer_id,
                    model,
                    model_size,
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

            ClientMsg::Question { text, messages, route, tagger_ms, tagger_tokens } => {
                let messages = messages.unwrap_or_else(|| {
                    vec![ChatMessage {
                        role: "user".to_string(),
                        content: text.unwrap_or_default(),
                    }]
                });
                let display_text = messages.last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default();
                let route_tag = match route.as_deref() {
                    Some("swarm") => Some("swarm".to_string()),
                    _ => None, // ignore unknown routes
                };
                self.handle_question(conn_id, display_text, messages, route_tag, tagger_tokens, tagger_ms).await;
            }
        }
    }

    // ── Question routing ────────────────────────────────────────────

    async fn handle_question(self: &Arc<Self>, probe_conn_id: usize, text: String, messages: Vec<crate::protocol::ChatMessage>, route_tag: Option<String>, tagger_tokens: Option<u32>, tagger_ms: Option<u64>) {
        let q_id = format!("q_{}", &uuid::Uuid::new_v4().to_string()[..8]);

        let line = hlog::question_received(&q_id, &text, route_tag.as_deref(), tagger_tokens, tagger_ms);
        self.emit(&line).await;

        {
            let mut state = self.state.lock().await;
            state.questions_today += 1;
            if !state.peers.contains_key(&probe_conn_id) {
                state.probes.insert(probe_conn_id);
            }
        }

        if self.route_question(&q_id, probe_conn_id, &text, &messages, TaskType::Answer, route_tag.as_deref()).await {
            return;
        }

        // No peers available — try to queue
        let mut state = self.state.lock().await;
        if state.normal_queue_full() {
            drop(state);
            let line = hlog::queue_full(&q_id);
            self.emit(&line).await;
            let err = HubMsg::Error { text: "No peers available and queue is full".to_string() };
            self.send_to(probe_conn_id, &err).await;
            return;
        }
        state.normal_queue.push_back(QueuedQuestion {
            question_id: q_id.clone(),
            probe_conn_id,
            text: text.clone(),
            messages: messages.clone(),
            task_type: TaskType::Answer,
            route_tag,
            arbitration_for: None,
        });
        drop(state);
    }

    /// Try to route a question to peers right now. Returns true if dispatched.
    async fn route_question(
        self: &Arc<Self>,
        q_id: &str,
        probe_conn_id: usize,
        text: &str,
        messages: &[crate::protocol::ChatMessage],
        task_type: TaskType,
        route_tag: Option<&str>,
    ) -> bool {
        let mut state = self.state.lock().await;

        let mut exclude = state.busy_peers.clone();
        exclude.insert(probe_conn_id);
        let n_peers = match task_type {
            TaskType::Answer => 3,
            TaskType::Time | TaskType::Archivist => 1,
            TaskType::Arbiter => unreachable!(),
        };
        let selected = router::select_peers(&state.peers, &exclude, n_peers);

        if selected.is_empty() {
            return false;
        }

        let n = selected.len();
        let task_id = match task_type {
            TaskType::Answer => q_id.replacen("q_", "task_", 1),
            _ => q_id.to_string(), // time/archivist: task_id = q_id
        };
        let max_tokens: u32 = match task_type {
            TaskType::Answer => if route_tag == Some("swarm") { 256 } else { 128 },
            TaskType::Time => 64,
            TaskType::Archivist => 60,
            TaskType::Arbiter => unreachable!(),
        };

        let mut gen_peers = Vec::new();
        let mut peers_to_send: Vec<(usize, PeerInfo)> = Vec::new();

        for (conn_id, peer_info) in &selected {
            gen_peers.push(*conn_id);
            state.busy_peers.insert(*conn_id);
            peers_to_send.push((*conn_id, peer_info.clone()));
        }

        state.task_to_question.insert(task_id.clone(), q_id.to_string());

        let pending = PendingQuestion {
            question_id: q_id.to_string(),
            probe_conn_id,
            text: text.to_string(),
            messages: messages.to_vec(),
            task_id: task_id.clone(),
            gen_peers,
            results: Vec::new(),
            expected_results: n,
            arbiter_task_id: None,
            arbiter_conn_id: None,
            start: Instant::now(),
            phase: QuestionPhase::Generating,
            task_type,
            route_tag: route_tag.map(|s| s.to_string()),
        };
        state.questions.insert(q_id.to_string(), pending);

        // Serialize stats while we still hold the lock (for swarm-tagged questions)
        let swarm_stats_str = if route_tag == Some("swarm") {
            Some(serialize_swarm_stats(&state))
        } else {
            None
        };

        drop(state);

        // One log line listing all assigned peers
        let peer_labels: Vec<(String, String)> = peers_to_send.iter()
            .map(|(_, p)| (p.peer_id.clone(), p.model_size.clone()))
            .collect();
        let line = hlog::task_assigned(&task_id, &peer_labels);
        self.emit(&line).await;

        let question_id_for_msg = match task_type {
            TaskType::Answer => Some(q_id.to_string()),
            _ => None,
        };

        for (conn_id, _peer_info) in &peers_to_send {
            let system = match task_type {
                TaskType::Answer => {
                    if let Some(ref stats) = swarm_stats_str {
                        format!("{SWARM_SYSTEM_PROMPT}\n\nCurrent swarm state:\n{stats}")
                    } else {
                        "You are a drone \u{2014} one node in a distributed swarm of small language models running in browser tabs around the world. You are answering a question alongside other peers \u{2014} an arbiter will pick the best answer.\n\nBe concise. One to three sentences unless the question genuinely requires more. Do not make up URLs, commands, or specific numbers you aren\u{2019}t sure about. If you don\u{2019}t know, say so briefly.".to_string()
                    }
                }
                TaskType::Time => "You are a drone in a distributed swarm of language models. You will be told the current time. Rewrite it naturally in your own words, then add a brief observation. Write it as a single flowing sentence. Do not use JSON. Do not repeat the exact format you were given.".to_string(),
                TaskType::Archivist => "Classify this question. Reply with exactly one line: category, tag, tag, summary (5 words).\n\nCategories: tech, science, history, literature, music, art, finance, philosophy, language, life, swarm, other\n\nExamples:\nliterature, shakespeare, hamlet, Hamlet character friend of Guildenstern\ntech, rust, wasm, Compile Rust to WASM tutorial\nlife, cooking, pasta, Boil spaghetti eight to ten\nswarm, peers, status, Twelve peers connected right now\nother, nonsense, gibberish, Gibberish question polite refusal given".to_string(),
                TaskType::Arbiter => unreachable!(),
            };
            let task = HubMsg::Task {
                task_id: task_id.clone(),
                question_id: question_id_for_msg.clone(),
                task_type: task_type.as_str().to_string(),
                system,
                messages: messages.to_vec(),
                max_tokens,
            };
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
                let has_idle = state.peers.keys().any(|id| !state.busy_peers.contains(id));
                if !has_idle {
                    break;
                }
                // Normal queue has priority over low queue
                if let Some(q) = state.normal_queue.pop_front() {
                    q
                } else if let Some(q) = state.low_queue.pop_front() {
                    q
                } else {
                    break;
                }
            };

            if let Some(ref q_id) = queued.arbitration_for {
                if !self.try_assign_arbiter(q_id).await {
                    let mut state = self.state.lock().await;
                    state.normal_queue.push_front(queued);
                    break;
                }
            } else {
                let dispatched = self.route_question(
                    &queued.question_id,
                    queued.probe_conn_id,
                    &queued.text,
                    &queued.messages,
                    queued.task_type,
                    queued.route_tag.as_deref(),
                ).await;

                if !dispatched {
                    let mut state = self.state.lock().await;
                    match queued.task_type {
                        TaskType::Answer | TaskType::Arbiter => state.normal_queue.push_front(queued),
                        TaskType::Time | TaskType::Archivist => state.low_queue.push_front(queued),
                    }
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
            let q_text = question.text.clone();
            question.phase = QuestionPhase::Done;
            drop(state);

            let arbiter_secs = result.inference_ms as f64 / 1000.0;
            let line = hlog::arbiter_completed(&result.task_id, &picked_result.peer_id, candidates, arbiter_secs);
            self.emit(&line).await;

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::answer_delivered(&q_id, total_tokens, total_secs, peers_used);
            self.emit(&line).await;

            let answer_text = picked_result.text.clone();
            let answer = HubMsg::Answer {
                question_id: q_id.clone(),
                text: picked_result.text,
                peer_id: picked_result.peer_id,
                total_tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &answer).await;
            self.queue_archivist(&q_id, &q_text, &answer_text).await;
            return;
        }

        // Must be a generation result
        if question.phase != QuestionPhase::Generating {
            return;
        }

        question.results.push(result);

        // Time task: emit [time] log and accumulate FLOPS
        if question.task_type == TaskType::Time {
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

        // Archivist task: log raw result and done
        if question.task_type == TaskType::Archivist {
            let first = question.results[0].clone();
            let q_id = question.question_id.clone();
            question.phase = QuestionPhase::Done;
            drop(state);

            let line = if first.text.trim().is_empty() {
                hlog::archivist_failed(&q_id, "empty")
            } else {
                hlog::archivist_result(&q_id, &first.text)
            };
            self.emit(&line).await;
            return;
        }

        // Degraded mode (1-2 peers): deliver first result immediately
        if question.expected_results <= 2 {
            let first = question.results[0].clone();
            let elapsed_ms = question.start.elapsed().as_millis() as u64;
            let probe = question.probe_conn_id;
            let q_id = question_id.clone();
            let q_text = question.text.clone();
            let peers_used = question.expected_results as u32;
            question.phase = QuestionPhase::Done;
            drop(state);

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::answer_delivered(&q_id, first.tokens, total_secs, peers_used);
            self.emit(&line).await;

            let answer_text = first.text.clone();
            let answer = HubMsg::Answer {
                question_id: q_id.clone(),
                text: first.text,
                peer_id: first.peer_id,
                total_tokens: first.tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &answer).await;
            self.queue_archivist(&q_id, &q_text, &answer_text).await;
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
                state.normal_queue.push_front(QueuedQuestion {
                    question_id: q_id.clone(),
                    probe_conn_id: 0,
                    text: String::new(),
                    messages: Vec::new(),
                    task_type: TaskType::Arbiter,
                    route_tag: None,
                    arbitration_for: Some(q_id.clone()),
                });
                drop(state);
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
        let gen_conn_ids: Vec<usize> = question.gen_peers.clone();
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
            let arbiter_task_id = question_id.replacen("q_", "arb_", 1);

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
                &arbiter_task_id, &arbiter_info.peer_id, &arbiter_info.model, &arbiter_info.model_size,
            );
            self.emit(&line).await;

            let task = HubMsg::Task {
                task_id: arbiter_task_id,
                question_id: Some(question_id.to_string()),
                task_type: "arbiter".to_string(),
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

        // Non-answer tasks: time silently drops, archivist logs failure
        if question.task_type != TaskType::Answer {
            let is_archivist = question.task_type == TaskType::Archivist;
            let q_id = question.question_id.clone();
            question.phase = QuestionPhase::Done;
            drop(state);
            if is_archivist {
                let line = hlog::archivist_failed(&q_id, "timeout");
                self.emit(&line).await;
            }
            return;
        }

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
            interval.tick().await; // skip immediate first tick
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

            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.tick().await; // skip immediate first tick
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

        if self.route_question(&q_id, 0, "", &messages, TaskType::Time, None).await {
            return;
        }

        // No peers — try to queue in low queue
        let mut state = self.state.lock().await;
        if state.low_queue_full() {
            drop(state);
            let line = hlog::time_skipped();
            self.emit(&line).await;
            return;
        }
        state.low_queue.push_back(QueuedQuestion {
            question_id: q_id.clone(),
            probe_conn_id: 0,
            text: String::new(),
            messages,
            task_type: TaskType::Time,
            route_tag: None,
            arbitration_for: None,
        });
        drop(state);
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
        let (peer_line, client_line) = hlog::swarm_stats(&state.peers, probes, observers, state.questions_today, state.total_flops);
        drop(state);
        self.emit(&peer_line).await;
        if let Some(line) = client_line {
            self.emit(&line).await;
        }
    }

    // ── Archivist ─────────────────────────────────────────────────────

    async fn queue_archivist(self: &Arc<Self>, question_id: &str, question_text: &str, answer_text: &str) {
        let arch_id = question_id.replacen("q_", "arch_", 1);
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: format!("Question: {}\nAnswer: {}", question_text, answer_text),
        }];

        let mut state = self.state.lock().await;
        if state.low_queue_full() {
            return; // silently drop
        }
        state.low_queue.push_back(QueuedQuestion {
            question_id: arch_id,
            probe_conn_id: 0,
            text: String::new(),
            messages,
            task_type: TaskType::Archivist,
            route_tag: None,
            arbitration_for: None,
        });
        drop(state);
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
