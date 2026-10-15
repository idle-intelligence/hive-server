use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Timelike;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::log as hlog;
use crate::protocol::{ChatMessage, ClientMsg, HubMsg};
use crate::router;
use crate::state::{
    HubState, MessagePhase, PeerInfo, PendingMessage, QueuedMessage, TaskResult, TaskType,
    MAX_LIVE_MESSAGES,
};

/// SmolLM2 1.7B ≈ 1.7B params × 2 ops/param = 3.4 GFLOPS per token
const FLOPS_PER_TOKEN: f64 = 3.4e9;

const SWARM_SYSTEM_PROMPT: &str =
    "You are one node in a distributed swarm of small language models running in \
browser tabs. You have been given real-time data about the swarm's current state.\n\
\n\
Answer ONLY using the data provided below. Do not make up numbers. If the data \
doesn't contain what's being asked, say you don't have that information. Be concise \
\u{2014} one to three sentences.";

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
            let items: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_yaml_value(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn json_to_compact_yaml(value: &serde_json::Value) -> String {
    let obj = value
        .as_object()
        .expect("json_to_compact_yaml: top level must be an object");
    obj.iter()
        .map(|(key, val)| format!("{}: {}", key, format_yaml_value(val)))
        .collect::<Vec<_>>()
        .join("\n")
}

fn serialize_swarm_stats(state: &HubState) -> String {
    let mut n_busy = 0u32;
    let mut total_tok_s = 0.0f64;

    for (conn_id, peer) in &state.peers {
        if state.busy_answerers.contains(conn_id) {
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
            "messages_answered_today": state.messages_today
        },
        "queue": {
            "normal": state.normal_queue.len(),
            "low": state.low_queue.len()
        }
    }))
}

fn build_time_query_messages(user_text: &str) -> Vec<ChatMessage> {
    let now = chrono::Utc::now();
    let readable = now.format("%A, %B %-d, %Y, %H:%M UTC").to_string();
    vec![ChatMessage {
        role: "user".to_string(),
        content: format!("Current time: {readable}\n\nUser asked: {user_text}"),
    }]
}

/// Messages waiting to be written to one connection. A connection that falls this far
/// behind is dropped.
const SEND_QUEUE: usize = 256;

/// The way to one connection: a queue drained by that connection's writer task.
struct Conn {
    tx: mpsc::Sender<Message>,
    writer: tokio::task::AbortHandle,
}

static NEXT_CONN_ID: AtomicUsize = AtomicUsize::new(1);

pub struct Hub {
    state: Mutex<HubState>,
    senders: std::sync::Mutex<HashMap<usize, Conn>>,
}

impl Hub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(HubState::new()),
            senders: std::sync::Mutex::new(HashMap::new()),
        })
    }

    pub async fn handle_connection(self: &Arc<Self>, ws: WebSocketStream<TcpStream>) {
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let (mut sink, mut rx) = ws.split();

        let (tx, mut queue) = mpsc::channel(SEND_QUEUE);
        let mut writer = tokio::spawn(async move {
            while let Some(msg) = queue.recv().await {
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        });
        self.senders.lock().unwrap().insert(
            conn_id,
            Conn {
                tx,
                writer: writer.abort_handle(),
            },
        );

        loop {
            let msg = tokio::select! {
                msg = rx.next() => msg,
                _ = &mut writer => None,
            };
            let Some(msg) = msg else { break };
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
        if let Some(conn) = self.senders.lock().unwrap().remove(&conn_id) {
            conn.writer.abort();
        }

        // Handle classify disconnect: find messages stuck in Classifying phase with this peer
        let classify_fallback_messages: Vec<(
            String,
            usize,
            String,
            Vec<crate::protocol::ChatMessage>,
        )> = {
            let mut state = self.state.lock().await;
            let mut fallbacks = Vec::new();
            for (m_id, msg) in state.messages.iter_mut() {
                if msg.phase == MessagePhase::Classifying && msg.classify_conn_id == Some(conn_id) {
                    msg.phase = MessagePhase::Generating; // Prevent timeout race
                    fallbacks.push((
                        m_id.clone(),
                        msg.probe_conn_id,
                        msg.text.clone(),
                        msg.messages.clone(),
                    ));
                }
            }
            fallbacks
        };

        for (m_id, probe_conn_id, text, messages) in classify_fallback_messages {
            let line = format!(
                "{} [classify] tagger disconnected, proceeding unrouted",
                hlog::ts()
            );
            self.emit(&line).await;

            // Transition to Generating with route_tag=None
            if self
                .route_message(
                    &m_id,
                    probe_conn_id,
                    &text,
                    &messages,
                    TaskType::Answer,
                    None,
                )
                .await
            {
                continue;
            }

            // Queue or reject
            let mut state = self.state.lock().await;
            if state.normal_queue_full() {
                drop(state);
                let line = hlog::queue_full(&m_id);
                self.emit(&line).await;
                let err = HubMsg::Error {
                    text: "No peers available and queue is full".to_string(),
                };
                self.send_to(probe_conn_id, &err).await;
            } else {
                state.normal_queue.push_back(QueuedMessage {
                    message_id: m_id.clone(),
                    probe_conn_id,
                    text,
                    messages,
                    task_type: TaskType::Answer,
                    route_tag: None,
                    arbitration_for: None,
                });
            }
        }

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
            ClientMsg::Register {
                peer_id,
                model,
                model_size,
                tok_s,
                capabilities,
            } => {
                let peer_id = crate::validate::sanitize_field(&peer_id, "peer");
                let model = crate::validate::sanitize_field(&model, "unknown");
                let model_size = crate::validate::sanitize_field(&model_size, "unknown");
                // Parse capabilities (default to [Answerer] for backward compat)
                let parsed_capabilities: std::collections::HashSet<crate::state::Capability> =
                    capabilities
                        .as_ref()
                        .map(|caps| {
                            caps.iter()
                                .filter_map(|s| match s.as_str() {
                                    "tagger" => Some(crate::state::Capability::Tagger),
                                    "answerer" => Some(crate::state::Capability::Answerer),
                                    _ => None,
                                })
                                .collect()
                        })
                        .unwrap_or_else(|| {
                            let mut caps = std::collections::HashSet::new();
                            caps.insert(crate::state::Capability::Answerer);
                            caps
                        });

                let has_answerer =
                    parsed_capabilities.contains(&crate::state::Capability::Answerer);

                // Emit join log
                let line = if has_answerer {
                    hlog::peer_joined_benchmarking(&peer_id, &model, &model_size)
                } else {
                    hlog::peer_joined_tagger(&peer_id, &model, &model_size)
                };
                self.emit(&line).await;

                let info = PeerInfo {
                    peer_id: peer_id.clone(),
                    model: model.clone(),
                    model_size: model_size.clone(),
                    tok_s: tok_s.unwrap_or(0.0),
                    load: 0.0,
                    capabilities: parsed_capabilities,
                };

                let stale_conn_id;
                {
                    let mut state = self.state.lock().await;

                    // Check for duplicate peer_id and evict stale entry
                    stale_conn_id = state
                        .peers
                        .iter()
                        .find(|(_, info)| info.peer_id == peer_id)
                        .map(|(conn_id, _)| *conn_id);

                    if let Some(old_id) = stale_conn_id {
                        // Remove stale entry from peers and busy sets
                        state.peers.remove(&old_id);
                        state.busy_answerers.remove(&old_id);
                        state.busy_taggers.remove(&old_id);

                        // Clean up any pending tasks assigned to the old conn_id
                        let mut tasks_to_remove = Vec::new();
                        for (task_id, message_id) in &state.task_to_message {
                            if let Some(q) = state.messages.get(message_id) {
                                if q.gen_peers.contains(&old_id)
                                    || q.arbiter_conn_id == Some(old_id)
                                {
                                    tasks_to_remove.push((task_id.clone(), message_id.clone()));
                                }
                            }
                        }
                        for (task_id, message_id) in tasks_to_remove {
                            state.task_to_message.remove(&task_id);
                            state.messages.remove(&message_id);
                        }
                    }

                    state.add_peer(conn_id, info);
                }

                // Remove old connection's sender if we evicted a stale entry
                if let Some(old_id) = stale_conn_id {
                    if let Some(conn) = self.senders.lock().unwrap().remove(&old_id) {
                        conn.writer.abort();
                    }
                    let line = format!(
                        "{} [peer] {} reconnected (replaced stale connection)",
                        hlog::ts(),
                        peer_id
                    );
                    self.emit(&line).await;
                }

                let count = self.state.lock().await.peer_count();
                let status = hlog::swarm_status(count);
                self.emit(&status).await;

                // Tagger-only peers skip benchmark
                if !has_answerer {
                    let line = hlog::peer_ready_tagger(&peer_id);
                    self.emit(&line).await;
                    return;
                }

                // Answerer peers: run benchmark
                let bench_task_id = format!("bench_{}", peer_id);
                let bench_m_id = format!("benchm_{}", peer_id);
                let stats_str;
                {
                    let mut state = self.state.lock().await;
                    state.busy_answerers.insert(conn_id);
                    stats_str = serialize_swarm_stats(&state);
                    state
                        .task_to_message
                        .insert(bench_task_id.clone(), bench_m_id.clone());
                    state.messages.insert(
                        bench_m_id.clone(),
                        PendingMessage {
                            message_id: bench_m_id.clone(),
                            probe_conn_id: 0,
                            text: String::new(),
                            messages: Vec::new(),
                            task_id: bench_task_id.clone(),
                            gen_peers: vec![conn_id],
                            results: Vec::new(),
                            expected_results: 1,
                            arbiter_task_id: None,
                            arbiter_conn_id: None,
                            start: Instant::now(),
                            phase: MessagePhase::Generating,
                            task_type: TaskType::Benchmark,
                            route_tag: None,
                            classify_task_id: None,
                            classify_conn_id: None,
                        },
                    );
                }

                let system = format!("{SWARM_SYSTEM_PROMPT}\n\nCurrent swarm state:\n{stats_str}");
                let task = HubMsg::Task {
                    task_id: bench_task_id.clone(),
                    message_id: None,
                    task_type: "benchmark".to_string(),
                    system,
                    messages: vec![ChatMessage {
                        role: "user".to_string(),
                        content: "How many peers are there in the swarm?".to_string(),
                    }],
                    max_tokens: 128,
                    text: None,
                };
                // m5 fix: check send result and remove from busy on failure
                if !self.send_to(conn_id, &task).await {
                    let mut state = self.state.lock().await;
                    state.busy_answerers.remove(&conn_id);
                    state.task_to_message.remove(&bench_task_id);
                    state.messages.remove(&bench_m_id);
                }
            }

            ClientMsg::Result {
                task_id,
                text,
                tok_s,
                tokens,
                inference_ms,
            } => {
                self.handle_result(conn_id, task_id, text, tok_s, tokens, inference_ms)
                    .await;
            }

            ClientMsg::Heartbeat { peer_id: _, load } => {
                self.state.lock().await.update_load(conn_id, load);
            }

            ClientMsg::Message {
                text,
                messages,
                route: _,
                classifier_ms: _,
                classifier_score: _,
            } => {
                let messages = messages.unwrap_or_else(|| {
                    vec![ChatMessage {
                        role: "user".to_string(),
                        content: text.unwrap_or_default(),
                    }]
                });
                let display_text = messages
                    .last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default();

                // Enforce 1000-character limit (v1-protocol.md)
                if display_text.chars().count() > 1000 {
                    let err = HubMsg::Error {
                        text: "Message too long (max 1000 characters)".to_string(),
                    };
                    self.send_to(conn_id, &err).await;
                    return;
                }

                self.process_incoming_message(conn_id, display_text, messages)
                    .await;
            }
        }
    }

    // ── Message truncation ──────────────────────────────────────────

    /// Truncate messages array to fit within context window.
    /// Always keeps the last user message. Drops oldest turns first.
    /// Estimates tokens as chars / 4.
    fn truncate_messages(
        messages: &[ChatMessage],
        system: &str,
        max_tokens: u32,
    ) -> Vec<ChatMessage> {
        const CONTEXT_WINDOW: usize = 2048; // SmolLM2-1.7B context window
        const SAFETY_MARGIN: usize = 50;

        let estimate_tokens = |text: &str| text.chars().count() / 4;

        let system_tokens = estimate_tokens(system);
        let budget = CONTEXT_WINDOW
            .saturating_sub(system_tokens)
            .saturating_sub(max_tokens as usize)
            .saturating_sub(SAFETY_MARGIN);

        if messages.is_empty() {
            return Vec::new();
        }

        // Always keep the last message
        let last_msg = &messages[messages.len() - 1];
        let last_tokens = estimate_tokens(&last_msg.content);

        if last_tokens >= budget {
            // Even the last message doesn't fit — return it anyway (will be truncated by model)
            return vec![last_msg.clone()];
        }

        let mut total_tokens = last_tokens;
        let mut keep_from = messages.len() - 1;

        // Walk backwards, adding messages until we exceed budget
        for i in (0..messages.len() - 1).rev() {
            let msg_tokens = estimate_tokens(&messages[i].content);
            if total_tokens + msg_tokens > budget {
                break;
            }
            total_tokens += msg_tokens;
            keep_from = i;
        }

        messages[keep_from..].to_vec()
    }

    // ── Question routing ────────────────────────────────────────────

    async fn process_incoming_message(
        self: &Arc<Self>,
        probe_conn_id: usize,
        text: String,
        messages: Vec<crate::protocol::ChatMessage>,
    ) {
        if self.state.lock().await.messages.len() >= MAX_LIVE_MESSAGES {
            let err = HubMsg::Error {
                text: "The hub is full, try again later".to_string(),
            };
            self.send_to(probe_conn_id, &err).await;
            return;
        }

        let m_id = format!("m_{}", &uuid::Uuid::new_v4().simple().to_string()[..16]);

        let line = hlog::message_received(&m_id, &text);
        self.emit(&line).await;

        let is_peer = {
            let mut state = self.state.lock().await;
            state.messages_today += 1;
            let is_peer = state.peers.contains_key(&probe_conn_id);
            if !is_peer {
                state.probes.insert(probe_conn_id);
            }
            is_peer
        };

        // All messages go through swarm classification
        if self
            .try_dispatch_classify(&m_id, probe_conn_id, &text, &messages)
            .await
        {
            return; // Classify dispatched, will continue after result
        }

        // No tagger available: proceed with route_tag=None (default Answer flow)
        if self
            .route_message(
                &m_id,
                probe_conn_id,
                &text,
                &messages,
                TaskType::Answer,
                None,
            )
            .await
        {
            return;
        }

        // No peers available — try to queue
        let mut state = self.state.lock().await;
        if state.normal_queue_full() {
            drop(state);
            let line = hlog::queue_full(&m_id);
            self.emit(&line).await;
            let err = HubMsg::Error {
                text: "No peers available and queue is full".to_string(),
            };
            self.send_to(probe_conn_id, &err).await;
            return;
        }
        state.normal_queue.push_back(QueuedMessage {
            message_id: m_id.clone(),
            probe_conn_id,
            text: text.clone(),
            messages: messages.clone(),
            task_type: TaskType::Answer,
            route_tag: None,
            arbitration_for: None,
        });
        drop(state);
    }

    /// Try to route a question to peers right now. Returns true if dispatched.
    async fn route_message(
        self: &Arc<Self>,
        m_id: &str,
        probe_conn_id: usize,
        text: &str,
        messages: &[crate::protocol::ChatMessage],
        task_type: TaskType,
        route_tag: Option<&str>,
    ) -> bool {
        let mut state = self.state.lock().await;

        let mut exclude = state.busy_answerers.clone();
        exclude.insert(probe_conn_id);
        let n_peers = match task_type {
            TaskType::Answer => 3,
            TaskType::Time | TaskType::Archivist => 1,
            TaskType::Arbiter | TaskType::Benchmark | TaskType::Classify => unreachable!(),
        };
        let selected = router::select_peers(
            &state.peers,
            &exclude,
            &state.busy_answerers,
            n_peers,
            crate::state::Capability::Answerer,
            false,
        );

        if selected.is_empty() {
            return false;
        }

        let n = selected.len();
        let task_id = match task_type {
            TaskType::Answer => m_id.replacen("m_", "task_", 1),
            _ => m_id.to_string(), // time/archivist: task_id = m_id
        };
        let max_tokens: u32 = match task_type {
            TaskType::Answer => {
                if route_tag == Some("swarm") {
                    256
                } else {
                    128
                }
            }
            TaskType::Time => 64,
            TaskType::Archivist => 60,
            TaskType::Arbiter | TaskType::Benchmark | TaskType::Classify => unreachable!(),
        };

        let mut gen_peers = Vec::new();
        let mut peers_to_send: Vec<(usize, PeerInfo)> = Vec::new();

        for (conn_id, peer_info) in &selected {
            gen_peers.push(*conn_id);
            state.busy_answerers.insert(*conn_id);
            peers_to_send.push((*conn_id, peer_info.clone()));
        }

        state
            .task_to_message
            .insert(task_id.clone(), m_id.to_string());

        let pending = PendingMessage {
            message_id: m_id.to_string(),
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
            phase: MessagePhase::Generating,
            task_type,
            route_tag: route_tag.map(|s| s.to_string()),
            classify_task_id: None,
            classify_conn_id: None,
        };
        state.messages.insert(m_id.to_string(), pending);

        // Serialize stats while we still hold the lock (for swarm-tagged questions)
        let swarm_stats_str = if route_tag == Some("swarm") {
            Some(serialize_swarm_stats(&state))
        } else {
            None
        };

        drop(state);

        // One log line listing all assigned peers
        let peer_labels: Vec<(String, String)> = peers_to_send
            .iter()
            .map(|(_, p)| (p.peer_id.clone(), p.model_size.clone()))
            .collect();
        let line = hlog::task_assigned(&task_id, &peer_labels);
        self.emit(&line).await;

        let message_id_for_msg = match task_type {
            TaskType::Answer => Some(m_id.to_string()),
            _ => None,
        };

        for (conn_id, _peer_info) in &peers_to_send {
            let system = match task_type {
                TaskType::Answer => {
                    if let Some(ref stats) = swarm_stats_str {
                        format!("{SWARM_SYSTEM_PROMPT}\n\nCurrent swarm state:\n{stats}")
                    } else {
                        "You are a drone \u{2014} one node in a distributed swarm of small language models running in browser tabs around the world. You are answering a question alongside other peers \u{2014} the swarm will pick the best answer.\n\nBe concise. One to three sentences unless the question genuinely requires more. Do not make up URLs, commands, or specific numbers you aren\u{2019}t sure about. If you don\u{2019}t know, say so briefly.".to_string()
                    }
                }
                TaskType::Time => "You are a drone in a distributed swarm of language models. You will be told the current time. Rewrite it naturally in your own words, then add a brief observation. Write it as a single flowing sentence. Do not use JSON. Do not repeat the exact format you were given.".to_string(),
                TaskType::Archivist => "You are a summarizer. Summarize the question in exactly 5 words. Output ONLY the summary, nothing else.".to_string(),
                TaskType::Arbiter | TaskType::Benchmark | TaskType::Classify => unreachable!(),
            };
            // Truncate messages to fit peer's context window
            let truncated_messages = Self::truncate_messages(messages, &system, max_tokens);
            let task = HubMsg::Task {
                task_id: task_id.clone(),
                message_id: message_id_for_msg.clone(),
                task_type: task_type.as_str().to_string(),
                system,
                messages: truncated_messages,
                max_tokens,
                text: None,
            };
            self.send_to(*conn_id, &task).await;
        }

        // Spawn 12s timeout
        let hub = Arc::clone(self);
        let m_id_owned = m_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(12)).await;
            hub.handle_timeout(&m_id_owned).await;
        });

        true
    }

    // ── Classify dispatch ───────────────────────────────────────────

    async fn try_dispatch_classify(
        self: &Arc<Self>,
        m_id: &str,
        probe_conn_id: usize,
        text: &str,
        messages: &[crate::protocol::ChatMessage],
    ) -> bool {
        let mut state = self.state.lock().await;

        let mut exclude = HashSet::new();
        exclude.insert(probe_conn_id);

        // Select 1 tagger, prefer tagger-only peers
        let selected = router::select_peers(
            &state.peers,
            &exclude,
            &state.busy_taggers,
            1,
            crate::state::Capability::Tagger,
            true, // prefer_exclusive
        );

        if selected.is_empty() {
            drop(state);
            let line = hlog::classify_no_tagger();
            self.emit(&line).await;
            return false;
        }

        let (tagger_conn_id, tagger_info) = selected[0].clone();
        let cls_task_id = m_id.replacen("m_", "cls_", 1);

        // Create PendingMessage with phase=Classifying
        let pending = PendingMessage {
            message_id: m_id.to_string(),
            probe_conn_id,
            text: text.to_string(),
            messages: messages.to_vec(),
            task_id: String::new(), // Will be set when transitioning to Generating
            gen_peers: Vec::new(),
            results: Vec::new(),
            expected_results: 0,
            arbiter_task_id: None,
            arbiter_conn_id: None,
            start: Instant::now(),
            phase: MessagePhase::Classifying,
            task_type: TaskType::Answer,
            route_tag: None,
            classify_task_id: Some(cls_task_id.clone()),
            classify_conn_id: Some(tagger_conn_id),
        };

        state.messages.insert(m_id.to_string(), pending);
        state
            .task_to_message
            .insert(cls_task_id.clone(), m_id.to_string());
        state.busy_taggers.insert(tagger_conn_id);

        drop(state);

        // Log
        let line = hlog::classify_assigned(&cls_task_id, &tagger_info.peer_id);
        self.emit(&line).await;

        // Send classify task
        let task = HubMsg::Task {
            task_id: cls_task_id.clone(),
            message_id: None,
            task_type: "classify".to_string(),
            system: String::new(),
            messages: Vec::new(),
            max_tokens: 0,
            text: Some(text.to_string()),
        };
        self.send_to(tagger_conn_id, &task).await;

        // Spawn 3s timeout
        let hub = Arc::clone(self);
        let m_id_owned = m_id.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            hub.handle_classify_timeout(&m_id_owned).await;
        });

        true
    }

    async fn handle_classify_timeout(self: &Arc<Self>, m_id: &str) {
        let (cls_task_id, probe_conn_id, text, messages) = {
            let mut state = self.state.lock().await;

            let message = match state.messages.get_mut(m_id) {
                Some(msg) if msg.phase == MessagePhase::Classifying => msg,
                _ => return, // Already handled or moved on
            };

            message.phase = MessagePhase::Generating; // Prevent re-entry

            // Extract all data before releasing the mutable borrow on message
            let cls_task_id = message.classify_task_id.clone();
            let probe_conn_id = message.probe_conn_id;
            let text = message.text.clone();
            let messages = message.messages.clone();

            // Clear classify tracking (now safe — message borrow is done)
            if let Some(ref task_id) = cls_task_id {
                state.task_to_message.remove(task_id);
            }
            (cls_task_id, probe_conn_id, text, messages)
        };

        // Log timeout
        if let Some(ref task_id) = cls_task_id {
            let line = hlog::classify_timeout(task_id);
            self.emit(&line).await;
        }

        // Transition to Generating and route
        if self
            .route_message(
                m_id,
                probe_conn_id,
                &text,
                &messages,
                TaskType::Answer,
                None,
            )
            .await
        {
            return;
        }

        // Queue or reject
        let mut state = self.state.lock().await;
        if state.normal_queue_full() {
            drop(state);
            let line = hlog::queue_full(m_id);
            self.emit(&line).await;
            let err = HubMsg::Error {
                text: "No peers available and queue is full".to_string(),
            };
            self.send_to(probe_conn_id, &err).await;
        } else {
            state.normal_queue.push_back(QueuedMessage {
                message_id: m_id.to_string(),
                probe_conn_id,
                text,
                messages,
                task_type: TaskType::Answer,
                route_tag: None,
                arbitration_for: None,
            });
        }
    }

    // ── Queue dispatch ──────────────────────────────────────────────

    async fn try_dispatch_queue(self: &Arc<Self>) {
        loop {
            let queued = {
                let mut state = self.state.lock().await;
                let has_idle = state.peers.iter().any(|(id, peer)| {
                    !state.busy_answerers.contains(id)
                        && peer
                            .capabilities
                            .contains(&crate::state::Capability::Answerer)
                });
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

            if let Some(ref m_id) = queued.arbitration_for {
                if !self.try_assign_arbiter(m_id).await {
                    let mut state = self.state.lock().await;
                    state.normal_queue.push_front(queued);
                    break;
                }
            } else {
                let dispatched = self
                    .route_message(
                        &queued.message_id,
                        queued.probe_conn_id,
                        &queued.text,
                        &queued.messages,
                        queued.task_type,
                        queued.route_tag.as_deref(),
                    )
                    .await;

                if !dispatched {
                    let mut state = self.state.lock().await;
                    match queued.task_type {
                        TaskType::Answer | TaskType::Arbiter => {
                            state.normal_queue.push_front(queued)
                        }
                        TaskType::Time | TaskType::Archivist => state.low_queue.push_front(queued),
                        TaskType::Benchmark | TaskType::Classify => unreachable!(),
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
        self.handle_result_inner(conn_id, task_id, text, tok_s, tokens, inference_ms)
            .await;
        self.try_dispatch_queue().await;
    }

    async fn handle_result_inner(
        self: &Arc<Self>,
        conn_id: usize,
        task_id: String,
        mut text: String,
        tok_s: f64,
        tokens: u32,
        inference_ms: u64,
    ) {
        let task_id = crate::validate::sanitize_field(&task_id, "task");
        // Enforce 2000-character limit on result text (v1-protocol.md)
        if text.chars().count() > 2000 {
            let line = format!(
                "{} [error] result for task {} exceeds 2000 chars, truncating",
                hlog::ts(),
                task_id
            );
            self.emit(&line).await;
            // Truncate to 2000 chars (note: we truncate rather than reject since the peer already did the work)
            text = text.chars().take(2000).collect();
        }

        // 1. Check the task was given to this connection, get peer info, unmark busy, look up
        // message. Handle benchmarks and classify inline.
        let checked = {
            let mut state = self.state.lock().await;
            let peer_id = state
                .peers
                .get(&conn_id)
                .map(|p| p.peer_id.clone())
                .unwrap_or_else(|| format!("conn_{conn_id}"));
            let message_id = state.task_to_message.get(&task_id).cloned();

            let expected = message_id
                .as_ref()
                .and_then(|qid| state.messages.get(qid))
                .is_some_and(|q| q.expects_result_from(&task_id, conn_id));
            if !expected {
                Err(peer_id)
            } else {
                let is_classify = task_id.starts_with("cls_");
                if is_classify {
                    state.busy_taggers.remove(&conn_id);
                } else {
                    state.busy_answerers.remove(&conn_id);
                }

                let is_benchmark = message_id
                    .as_ref()
                    .and_then(|qid| state.messages.get(qid))
                    .map(|q| q.task_type == TaskType::Benchmark)
                    .unwrap_or(false);

                if is_benchmark {
                    if let Some(peer) = state.peers.get_mut(&conn_id) {
                        peer.tok_s = tok_s;
                    }
                    if let Some(ref qid) = message_id {
                        state.messages.remove(qid);
                        state.task_to_message.remove(&task_id);
                    }
                }

                Ok((peer_id, message_id, is_benchmark, is_classify))
            }
        };
        let (peer_id, message_id, is_benchmark, is_classify) = match checked {
            Ok(found) => found,
            Err(peer_id) => {
                let line = hlog::result_ignored(&task_id, &peer_id);
                self.emit(&line).await;
                return;
            }
        };

        if is_benchmark {
            let line = hlog::peer_ready(&peer_id, tok_s);
            self.emit(&line).await;
            return;
        }

        // Handle classify result
        if is_classify {
            let message_id = match message_id {
                Some(id) => id,
                None => return,
            };

            // Parse JSON result: {label, score, scores}
            let (route_label, score, parse_error) =
                match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(json) => {
                        let label = json
                            .get("label")
                            .and_then(|v| v.as_str())
                            .unwrap_or("other");
                        let score = json.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        (crate::validate::known_label(label).to_string(), score, None)
                    }
                    Err(e) => {
                        // Parse failure → fallback to unrouted
                        ("other".to_string(), 0.0, Some(e.to_string()))
                    }
                };

            // Log parse failure if it occurred
            if parse_error.is_some() {
                let line = format!(
                    "{} [classify {}] JSON parse failed, defaulting to other",
                    hlog::ts(),
                    task_id
                );
                self.emit(&line).await;
            }

            // Apply threshold and extract route_tag
            let route_tag = if score < 0.6 {
                None
            } else {
                match route_label.as_str() {
                    "swarm" => Some("swarm".to_string()),
                    "time" => Some("time".to_string()),
                    "weather" => Some("weather".to_string()),
                    _ => None,
                }
            };

            // Log classify completion
            let line = hlog::classify_completed(
                &task_id,
                &peer_id,
                &route_label,
                score,
                inference_ms,
                route_tag.is_some(),
            );
            self.emit(&line).await;

            // Get message details for observer gating and routing
            let (probe_conn_id, text, messages, is_peer) = {
                let mut state = self.state.lock().await;
                let msg = match state.messages.get_mut(&message_id) {
                    Some(m) if m.phase == MessagePhase::Classifying => m,
                    _ => return, // Message already moved on or done
                };
                msg.phase = MessagePhase::Generating;
                msg.route_tag = route_tag.clone();
                let probe_conn_id = msg.probe_conn_id;
                let text = msg.text.clone();
                let messages = msg.messages.clone();
                let is_peer = state.peers.contains_key(&probe_conn_id);
                (probe_conn_id, text, messages, is_peer)
            };

            // Observer gating: reject swarm-tagged messages from non-peers
            if route_tag.as_deref() == Some("swarm") && !is_peer {
                let line = format!(
                    "{} [error] {} attempted swarm message without joining",
                    hlog::ts(),
                    message_id
                );
                self.emit(&line).await;
                let err = HubMsg::Error {
                    text: "Join the swarm to know the swarm.".to_string(),
                };
                self.send_to(probe_conn_id, &err).await;

                // Mark message as done and clean up
                let mut state = self.state.lock().await;
                if let Some(msg) = state.messages.get_mut(&message_id) {
                    msg.phase = MessagePhase::Done;
                }
                state.task_to_message.remove(&task_id);
                return;
            }

            // Clean up classify task tracking
            {
                let mut state = self.state.lock().await;
                state.task_to_message.remove(&task_id);
            }

            // Determine task type and messages based on route
            let (task_type, routed_messages) = if route_tag.as_deref() == Some("time") {
                (TaskType::Time, build_time_query_messages(&text))
            } else {
                (TaskType::Answer, messages)
            };

            // Transition to Generating and route
            if self
                .route_message(
                    &message_id,
                    probe_conn_id,
                    &text,
                    &routed_messages,
                    task_type,
                    route_tag.as_deref(),
                )
                .await
            {
                return;
            }

            // Queue or reject
            let mut state = self.state.lock().await;
            if state.normal_queue_full() {
                drop(state);
                let line = hlog::queue_full(&message_id);
                self.emit(&line).await;
                let err = HubMsg::Error {
                    text: "No peers available and queue is full".to_string(),
                };
                self.send_to(probe_conn_id, &err).await;
            } else {
                state.normal_queue.push_back(QueuedMessage {
                    message_id: message_id.clone(),
                    probe_conn_id,
                    text,
                    messages: routed_messages,
                    task_type,
                    route_tag,
                    arbitration_for: None,
                });
            }
            return;
        }

        // 2. Always log task completion
        let line = hlog::task_completed(&task_id, &peer_id, tokens, inference_ms, tok_s);
        self.emit(&line).await;

        // 3. Not part of a question? Done.
        let message_id = match message_id {
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
        let message = match state.messages.get_mut(&message_id) {
            Some(q) if q.phase != MessagePhase::Done => q,
            _ => return,
        };

        // Is this the arbiter's result?
        if message.phase == MessagePhase::Arbitrating
            && message.arbiter_task_id.as_deref() == Some(&task_id)
        {
            let picked_idx = parse_arbiter_pick(&result.text, message.results.len());
            let picked_result = message.results[picked_idx].clone();
            let candidates = message.results.len();
            let elapsed_ms = message.start.elapsed().as_millis() as u64;
            let total_tokens: u32 =
                message.results.iter().map(|r| r.tokens).sum::<u32>() + result.tokens;
            let peers_used = candidates as u32 + 1;
            let probe = message.probe_conn_id;
            let m_id = message_id.clone();
            let m_text = message.text.clone();
            message.phase = MessagePhase::Done;
            drop(state);

            let arbiter_secs = result.inference_ms as f64 / 1000.0;
            let line = hlog::arbiter_completed(
                &result.task_id,
                &picked_result.peer_id,
                candidates,
                arbiter_secs,
            );
            self.emit(&line).await;

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::response_delivered(&m_id, total_tokens, total_secs, peers_used);
            self.emit(&line).await;

            let response_text = picked_result.text.clone();
            let response = HubMsg::Response {
                message_id: m_id.clone(),
                text: picked_result.text,
                peer_id: picked_result.peer_id,
                total_tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &response).await;
            self.queue_archivist(&m_id, &m_text, &response_text).await;
            return;
        }

        // Must be a generation result
        if message.phase != MessagePhase::Generating {
            return;
        }

        message.results.push(result);

        // Time task: emit [time] log and accumulate FLOPS
        if message.task_type == TaskType::Time {
            let first = message.results[0].clone();
            let flops = first.tokens as f64 * FLOPS_PER_TOKEN;
            let probe = message.probe_conn_id;
            let m_id = message.message_id.clone();
            let m_text = message.text.clone();
            let elapsed_ms = message.start.elapsed().as_millis() as u64;
            message.phase = MessagePhase::Done;
            state.total_flops += flops;
            drop(state);

            let line = hlog::time_result(
                &first.peer_id,
                (probe == 0).then_some(first.text.as_str()),
                first.tokens,
                flops,
            );
            self.emit(&line).await;

            // User-initiated time query: send response back
            if probe != 0 {
                let total_secs = elapsed_ms as f64 / 1000.0;
                let line = hlog::response_delivered(&m_id, first.tokens, total_secs, 1);
                self.emit(&line).await;

                let response = HubMsg::Response {
                    message_id: m_id.clone(),
                    text: first.text.clone(),
                    peer_id: first.peer_id.clone(),
                    total_tokens: first.tokens,
                    total_ms: elapsed_ms,
                    peers_used: 1,
                };
                self.send_to(probe, &response).await;
                self.queue_archivist(&m_id, &m_text, &first.text).await;
            }
            return;
        }

        // Archivist task: log raw result and done
        if message.task_type == TaskType::Archivist {
            let first = message.results[0].clone();
            let m_id = message.message_id.clone();
            message.phase = MessagePhase::Done;
            drop(state);

            let line = if first.text.trim().is_empty() {
                hlog::archivist_failed(&m_id, "empty")
            } else {
                hlog::archivist_result(&m_id, &first.text)
            };
            self.emit(&line).await;
            return;
        }

        // Degraded mode (1-2 peers): deliver first result immediately
        if message.expected_results <= 2 {
            let first = message.results[0].clone();
            let elapsed_ms = message.start.elapsed().as_millis() as u64;
            let probe = message.probe_conn_id;
            let m_id = message_id.clone();
            let m_text = message.text.clone();
            let peers_used = message.expected_results as u32;
            message.phase = MessagePhase::Done;
            drop(state);

            let total_secs = elapsed_ms as f64 / 1000.0;
            let line = hlog::response_delivered(&m_id, first.tokens, total_secs, peers_used);
            self.emit(&line).await;

            let response_text = first.text.clone();
            let response = HubMsg::Response {
                message_id: m_id.clone(),
                text: first.text,
                peer_id: first.peer_id,
                total_tokens: first.tokens,
                total_ms: elapsed_ms,
                peers_used,
            };
            self.send_to(probe, &response).await;
            self.queue_archivist(&m_id, &m_text, &response_text).await;
        } else if message.results.len() >= message.expected_results {
            // All 3 results collected — proceed to arbitration
            let m_id = message_id.clone();
            drop(state);

            if !self.try_assign_arbiter(&m_id).await {
                // No arbiter available — queue at front, timeout is the safety net
                let mut state = self.state.lock().await;
                if let Some(q) = state.messages.get_mut(&m_id) {
                    if q.phase == MessagePhase::Generating {
                        q.phase = MessagePhase::Arbitrating;
                    }
                }
                // Arbiter tasks always queue (ignore backpressure)
                state.normal_queue.push_front(QueuedMessage {
                    message_id: m_id.clone(),
                    probe_conn_id: 0,
                    text: String::new(),
                    messages: Vec::new(),
                    task_type: TaskType::Arbiter,
                    route_tag: None,
                    arbitration_for: Some(m_id.clone()),
                });
                drop(state);
            }
        }
    }

    // ── Arbiter ──────────────────────────────────────────────────────

    /// Try to assign an arbiter for a message. Returns true if assigned, false if no peer available.
    async fn try_assign_arbiter(self: &Arc<Self>, message_id: &str) -> bool {
        let mut state = self.state.lock().await;

        // Accept Generating (first attempt) or Arbitrating with no arbiter yet (retry from queue)
        let message = match state.messages.get(message_id) {
            Some(q) if q.phase == MessagePhase::Generating => q,
            Some(q) if q.phase == MessagePhase::Arbitrating && q.arbiter_task_id.is_none() => q,
            _ => return true, // Already handled (Done, or arbiter already assigned)
        };

        // Extract data we need while holding an immutable borrow
        let gen_conn_ids: Vec<usize> = message.gen_peers.clone();
        let m_text = message.text.clone();
        let results_snapshot: Vec<TaskResult> = message.results.clone();
        let probe = message.probe_conn_id;

        // Build exclude set: generation peers + busy peers + probe
        let mut exclude = state.busy_answerers.clone();
        for &conn_id in &gen_conn_ids {
            exclude.insert(conn_id);
        }
        exclude.insert(probe);

        let arbiters = router::select_peers(
            &state.peers,
            &exclude,
            &state.busy_answerers,
            1,
            crate::state::Capability::Answerer,
            false,
        );

        if let Some((arbiter_conn_id, arbiter_info)) = arbiters.into_iter().next() {
            let arbiter_task_id = message_id.replacen("m_", "arb_", 1);

            // Build arbiter message content
            let mut content = format!("Question: {}\n", m_text);
            for (i, r) in results_snapshot.iter().enumerate() {
                content.push_str(&format!("\nAnswer {}: {}", i + 1, r.text));
            }
            content.push_str("\n\nWhich answer is best? Respond with only the number.");

            let arbiter_system = "You are an arbiter in a distributed swarm of language models. You will receive a question and several candidate answers from other nodes. Pick the best one.\n\nRespond with ONLY the number of the best answer. Nothing else.".to_string();

            let arbiter_messages = vec![crate::protocol::ChatMessage {
                role: "user".to_string(),
                content,
            }];

            // Truncate arbiter messages to fit context window
            let truncated_arbiter_messages =
                Self::truncate_messages(&arbiter_messages, &arbiter_system, 8);

            // Now take mutable borrow to update phase
            let message = state.messages.get_mut(message_id).unwrap();
            message.arbiter_task_id = Some(arbiter_task_id.clone());
            message.arbiter_conn_id = Some(arbiter_conn_id);
            message.phase = MessagePhase::Arbitrating;

            state
                .task_to_message
                .insert(arbiter_task_id.clone(), message_id.to_string());
            state.busy_answerers.insert(arbiter_conn_id);
            drop(state);

            let line = hlog::arbiter_assigned(
                &arbiter_task_id,
                &arbiter_info.peer_id,
                &arbiter_info.model,
                &arbiter_info.model_size,
            );
            self.emit(&line).await;

            let task = HubMsg::Task {
                task_id: arbiter_task_id,
                message_id: Some(message_id.to_string()),
                task_type: "arbiter".to_string(),
                system: arbiter_system,
                messages: truncated_arbiter_messages,
                max_tokens: 8,
                text: None,
            };
            self.send_to(arbiter_conn_id, &task).await;
            true
        } else {
            false
        }
    }

    // ── Timeout ─────────────────────────────────────────────────────

    async fn handle_timeout(self: &Arc<Self>, message_id: &str) {
        let mut state = self.state.lock().await;
        let message = match state.messages.get_mut(message_id) {
            Some(q) if q.phase != MessagePhase::Done => q,
            _ => return,
        };

        match message.phase {
            MessagePhase::Generating => {
                if message.results.is_empty() {
                    let probe = message.probe_conn_id;
                    let m_id = message_id.to_string();
                    message.phase = MessagePhase::Done;
                    drop(state);

                    let line = format!(
                        "{} [error] all peers timed out for question {}",
                        hlog::ts(),
                        m_id
                    );
                    self.emit(&line).await;

                    let err = HubMsg::Error {
                        text: "All peers timed out".to_string(),
                    };
                    self.send_to(probe, &err).await;
                } else {
                    // Got some results — try to arbitrate with what we have
                    let m_id = message_id.to_string();
                    drop(state);

                    if !self.try_assign_arbiter(&m_id).await {
                        // Timed out and no arbiter — deliver first result
                        let mut state = self.state.lock().await;
                        let message = match state.messages.get_mut(&m_id) {
                            Some(q) if q.phase != MessagePhase::Done => q,
                            _ => return,
                        };
                        let first = message.results[0].clone();
                        let elapsed_ms = message.start.elapsed().as_millis() as u64;
                        let probe = message.probe_conn_id;
                        let peers_used = message.results.len() as u32;
                        message.phase = MessagePhase::Done;

                        // Remove any queued arbiter entry for this message (m4 fix)
                        state
                            .normal_queue
                            .retain(|q| q.arbitration_for.as_ref() != Some(&m_id));
                        drop(state);

                        let line = format!(
                            "{} [error] timed out for {}, no arbiter, returning first response",
                            hlog::ts(),
                            m_id
                        );
                        self.emit(&line).await;

                        let total_secs = elapsed_ms as f64 / 1000.0;
                        let line =
                            hlog::response_delivered(&m_id, first.tokens, total_secs, peers_used);
                        self.emit(&line).await;

                        let response = HubMsg::Response {
                            message_id: m_id,
                            text: first.text,
                            peer_id: first.peer_id,
                            total_tokens: first.tokens,
                            total_ms: elapsed_ms,
                            peers_used,
                        };
                        self.send_to(probe, &response).await;
                    }
                }
            }
            MessagePhase::Arbitrating => {
                // Arbiter timed out — deliver first result
                let first = message.results[0].clone();
                let elapsed_ms = message.start.elapsed().as_millis() as u64;
                let probe = message.probe_conn_id;
                let m_id = message_id.to_string();
                let peers_used = message.results.len() as u32;
                message.phase = MessagePhase::Done;

                // Remove any queued arbiter entry for this message (m4 fix)
                state
                    .normal_queue
                    .retain(|q| q.arbitration_for.as_ref() != Some(&m_id));
                drop(state);

                let line = format!(
                    "{} [error] arbiter timed out for {}, returning first response",
                    hlog::ts(),
                    m_id
                );
                self.emit(&line).await;

                let total_secs = elapsed_ms as f64 / 1000.0;
                let line = hlog::response_delivered(&m_id, first.tokens, total_secs, peers_used);
                self.emit(&line).await;

                let response = HubMsg::Response {
                    message_id: m_id,
                    text: first.text,
                    peer_id: first.peer_id,
                    total_tokens: first.tokens,
                    total_ms: elapsed_ms,
                    peers_used,
                };
                self.send_to(probe, &response).await;
            }
            MessagePhase::Classifying => {
                // Classify timeout handling added in Task #4
            }
            MessagePhase::Done => {}
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

    pub fn start_sweeper(self: &Arc<Self>) {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                hub.state.lock().await.sweep_done();
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
        let m_id = format!("time_{}", tag);

        let readable = now.format("%A, %B %-d, %Y, %H:%M UTC").to_string();
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: format!("Current time: {readable}"),
        }];

        if self
            .route_message(&m_id, 0, "", &messages, TaskType::Time, None)
            .await
        {
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
        state.low_queue.push_back(QueuedMessage {
            message_id: m_id.clone(),
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
        let total_conns = self.senders.lock().unwrap().len();
        let non_peers = total_conns.saturating_sub(state.peers.len());
        let probes = state.probes.len();
        let observers = non_peers.saturating_sub(probes);
        let (peer_line, client_line) = hlog::swarm_stats(
            &state.peers,
            probes,
            observers,
            state.messages_today,
            state.total_flops,
        );
        drop(state);
        self.emit(&peer_line).await;
        if let Some(line) = client_line {
            self.emit(&line).await;
        }
    }

    // ── Archivist ─────────────────────────────────────────────────────

    async fn queue_archivist(
        self: &Arc<Self>,
        message_id: &str,
        question_text: &str,
        response_text: &str,
    ) {
        let arch_id = message_id.replacen("m_", "arch_", 1);
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: format!("Question: {}\nAnswer: {}", question_text, response_text),
        }];

        let mut state = self.state.lock().await;
        if state.low_queue_full() {
            return; // silently drop
        }
        state.low_queue.push_back(QueuedMessage {
            message_id: arch_id,
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

    /// Print to stdout and broadcast to all connected clients. Never waits on a client:
    /// a client whose queue is full or closed is dropped.
    async fn emit(&self, line: &str) {
        println!("{line}");

        let msg = HubMsg::Log {
            line: line.to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();

        let mut senders = self.senders.lock().unwrap();
        let dead: Vec<usize> = senders
            .iter()
            .filter(|(_, conn)| conn.tx.try_send(Message::Text(json.clone())).is_err())
            .map(|(&conn_id, _)| conn_id)
            .collect();
        for id in dead {
            if let Some(conn) = senders.remove(&id) {
                conn.writer.abort();
            }
        }
    }

    /// Queues a message for one connection. Returns false if the connection is gone or
    /// too far behind (and is dropped).
    async fn send_to(&self, conn_id: usize, msg: &HubMsg) -> bool {
        let json = serde_json::to_string(msg).unwrap();
        let mut senders = self.senders.lock().unwrap();
        let Some(conn) = senders.get(&conn_id) else {
            return false;
        };
        if conn.tx.try_send(Message::Text(json)).is_ok() {
            return true;
        }
        if let Some(conn) = senders.remove(&conn_id) {
            conn.writer.abort();
        }
        false
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Connects one page to a hub and returns the hub, the page's stream and its connection id.
    async fn hub_with_page() -> (
        Arc<Hub>,
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        usize,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hub = Hub::new();
        let server_hub = Arc::clone(&hub);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            server_hub.handle_connection(ws).await;
        });
        let (page, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .unwrap();
        let conn_id = loop {
            if let Some(id) = hub.senders.lock().unwrap().keys().next().copied() {
                break id;
            }
            tokio::task::yield_now().await;
        };
        (hub, page, conn_id)
    }

    async fn pending(hub: &Arc<Hub>, m_id: &str, probe_conn_id: usize, task_type: TaskType) {
        hub.state.lock().await.messages.insert(
            m_id.to_string(),
            PendingMessage {
                message_id: m_id.to_string(),
                probe_conn_id,
                text: String::new(),
                messages: Vec::new(),
                task_id: m_id.to_string(),
                gen_peers: Vec::new(),
                results: Vec::new(),
                expected_results: 1,
                arbiter_task_id: None,
                arbiter_conn_id: None,
                start: Instant::now(),
                phase: MessagePhase::Generating,
                task_type,
                route_tag: None,
                classify_task_id: None,
                classify_conn_id: None,
            },
        );
    }

    async fn next_error(
        page: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> String {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(5), page.next())
                .await
                .expect("no message")
                .unwrap()
                .unwrap();
            let Message::Text(text) = msg else { continue };
            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
            if json["type"] == "error" {
                return json["text"].as_str().unwrap().to_string();
            }
        }
    }

    #[tokio::test]
    async fn timeout_without_results_tells_the_asker_for_every_task_type() {
        for (i, task_type) in [TaskType::Answer, TaskType::Time].into_iter().enumerate() {
            let (hub, mut page, conn_id) = hub_with_page().await;
            let m_id = format!("m_t{i}");
            pending(&hub, &m_id, conn_id, task_type).await;
            hub.handle_timeout(&m_id).await;
            assert_eq!(next_error(&mut page).await, "All peers timed out");
            let state = hub.state.lock().await;
            assert_eq!(state.messages[&m_id].phase, MessagePhase::Done);
        }
    }

    #[tokio::test]
    async fn timeout_of_a_task_nobody_waits_on_just_ends() {
        let (hub, _page, _conn_id) = hub_with_page().await;
        for (m_id, task_type) in [("time_1", TaskType::Time), ("arch_1", TaskType::Archivist)] {
            pending(&hub, m_id, 0, task_type).await;
            hub.handle_timeout(m_id).await;
            let state = hub.state.lock().await;
            assert_eq!(state.messages[m_id].phase, MessagePhase::Done);
        }
    }
    #[tokio::test]
    async fn sweep_drops_old_finished_messages_and_their_task_ids() {
        let (hub, _page, conn_id) = hub_with_page().await;
        let old = Instant::now()
            .checked_sub(crate::state::DONE_KEEP + Duration::from_secs(1))
            .unwrap();
        for (m_id, phase, start) in [
            ("m_old_done", MessagePhase::Done, old),
            ("m_new_done", MessagePhase::Done, Instant::now()),
            ("m_old_live", MessagePhase::Generating, old),
        ] {
            pending(&hub, m_id, conn_id, TaskType::Answer).await;
            let mut state = hub.state.lock().await;
            let m = state.messages.get_mut(m_id).unwrap();
            m.phase = phase;
            m.start = start;
            state
                .task_to_message
                .insert(format!("task_{m_id}"), m_id.to_string());
        }
        hub.state.lock().await.sweep_done();
        let state = hub.state.lock().await;
        assert!(!state.messages.contains_key("m_old_done"));
        assert!(state.messages.contains_key("m_new_done"));
        assert!(state.messages.contains_key("m_old_live"));
        assert!(!state.task_to_message.contains_key("task_m_old_done"));
        assert!(state.task_to_message.contains_key("task_m_new_done"));
        assert!(state.task_to_message.contains_key("task_m_old_live"));
    }

    #[tokio::test]
    async fn new_questions_are_refused_when_the_hub_is_full() {
        let (hub, mut page, conn_id) = hub_with_page().await;
        {
            let mut state = hub.state.lock().await;
            for i in 0..MAX_LIVE_MESSAGES {
                state.messages.insert(
                    format!("m_{i}"),
                    PendingMessage {
                        message_id: String::new(),
                        probe_conn_id: 0,
                        text: String::new(),
                        messages: Vec::new(),
                        task_id: String::new(),
                        gen_peers: Vec::new(),
                        results: Vec::new(),
                        expected_results: 0,
                        arbiter_task_id: None,
                        arbiter_conn_id: None,
                        start: Instant::now(),
                        phase: MessagePhase::Done,
                        task_type: TaskType::Answer,
                        route_tag: None,
                        classify_task_id: None,
                        classify_conn_id: None,
                    },
                );
            }
        }
        hub.process_incoming_message(
            conn_id,
            "hi".to_string(),
            vec![ChatMessage {
                role: "user".to_string(),
                content: "hi".to_string(),
            }],
        )
        .await;
        assert_eq!(
            next_error(&mut page).await,
            "The hub is full, try again later"
        );
        assert_eq!(hub.state.lock().await.messages.len(), MAX_LIVE_MESSAGES);
    }

    #[tokio::test]
    async fn message_ids_carry_sixteen_random_hex_digits() {
        let (hub, _page, conn_id) = hub_with_page().await;
        hub.process_incoming_message(conn_id, "hi".to_string(), Vec::new())
            .await;
        let state = hub.state.lock().await;
        let id = &state.normal_queue[0].message_id;
        let hex = id.strip_prefix("m_").unwrap();
        assert_eq!(hex.len(), 16);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }
    /// A hub that accepts any number of connections.
    async fn hub_serving() -> (Arc<Hub>, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hub = Hub::new();
        let server_hub = Arc::clone(&hub);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let hub = Arc::clone(&server_hub);
                tokio::spawn(async move {
                    let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                    hub.handle_connection(ws).await;
                });
            }
        });
        (hub, addr)
    }

    async fn wait_for_connections(hub: &Arc<Hub>, n: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while hub.senders.lock().unwrap().len() != n {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("connection count");
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_does_not_hold_up_the_others() {
        let (hub, addr) = hub_serving().await;
        let url = format!("ws://{addr}/ws");
        let (_stuck, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        wait_for_connections(&hub, 1).await;
        let stuck_id = *hub.senders.lock().unwrap().keys().next().unwrap();
        let (mut healthy, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        wait_for_connections(&hub, 2).await;
        tokio::spawn(async move { while healthy.next().await.is_some() {} });

        let line = "x".repeat(64 * 1024);
        tokio::time::timeout(Duration::from_secs(20), async {
            for _ in 0..1000 {
                hub.emit(&line).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("emit waited on a client that does not read");

        let senders = hub.senders.lock().unwrap();
        assert_eq!(senders.len(), 1);
        assert!(!senders.contains_key(&stuck_id));
    }
    type Ws = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn connect(addr: std::net::SocketAddr) -> Ws {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
            .await
            .unwrap();
        ws
    }

    async fn send_json(ws: &mut Ws, value: serde_json::Value) {
        ws.send(Message::Text(value.to_string())).await.unwrap();
    }

    /// Reads until a message of type `ty` arrives. None after `ms` milliseconds.
    async fn next_of_type(ws: &mut Ws, ty: &str, ms: u64) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_millis(ms), async {
            loop {
                let Message::Text(text) = ws.next().await?.ok()? else {
                    continue;
                };
                let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                if json["type"] == ty {
                    return Some(json);
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    async fn wait_state(hub: &Arc<Hub>, what: &str, cond: impl Fn(&HubState) -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !cond(&*hub.state.lock().await) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    /// A fake drone. It answers its own benchmark and records every other task; the test
    /// decides what it sends back.
    struct Drone {
        tasks: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
        out: mpsc::UnboundedSender<serde_json::Value>,
    }

    impl Drone {
        async fn join(hub: &Arc<Hub>, addr: std::net::SocketAddr, peer_id: &str) -> Drone {
            let mut ws = connect(addr).await;
            send_json(
                &mut ws,
                serde_json::json!({"type":"register","peer_id":peer_id,"model":"m","model_size":"1B","tok_s":10.0,"capabilities":["answerer"]}),
            )
            .await;
            let tasks = Arc::new(std::sync::Mutex::new(Vec::new()));
            let (out, mut outbox) = mpsc::unbounded_channel::<serde_json::Value>();
            let seen = Arc::clone(&tasks);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        incoming = ws.next() => {
                            let Some(Ok(Message::Text(text))) = incoming else { continue };
                            let json: serde_json::Value = serde_json::from_str(&text).unwrap();
                            if json["type"] != "task" {
                                continue;
                            }
                            if json["task_type"] == "benchmark" {
                                let reply = serde_json::json!({"type":"result","task_id":json["task_id"],"text":"3","tok_s":10.0,"tokens":1,"inference_ms":1});
                                ws.send(Message::Text(reply.to_string())).await.unwrap();
                            } else {
                                seen.lock().unwrap().push(json);
                            }
                        }
                        Some(msg) = outbox.recv() => {
                            ws.send(Message::Text(msg.to_string())).await.unwrap();
                        }
                    }
                }
            });
            let id = peer_id.to_string();
            wait_state(hub, "drone ready", |s| {
                s.peers
                    .iter()
                    .any(|(c, p)| p.peer_id == id && !s.busy_answerers.contains(c))
            })
            .await;
            Drone { tasks, out }
        }

        /// The first task of this type the drone has been given.
        async fn task(&self, task_type: &str) -> serde_json::Value {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(t) = self
                        .tasks
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|t| t["task_type"] == task_type)
                    {
                        return t.clone();
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no {task_type} task arrived"))
        }

        fn answer(&self, task: &serde_json::Value, text: &str) {
            self.out
                .send(serde_json::json!({"type":"result","task_id":task["task_id"],"text":text,"tok_s":10.0,"tokens":5,"inference_ms":5}))
                .unwrap();
        }
    }

    fn ask(text: &str) -> serde_json::Value {
        serde_json::json!({"type":"message","text":text})
    }

    #[tokio::test]
    async fn a_result_counts_only_from_the_connection_the_task_went_to() {
        let (hub, addr) = hub_serving().await;
        let d1 = Drone::join(&hub, addr, "d1").await;
        let mut asker = connect(addr).await;
        let mut attacker = connect(addr).await;
        send_json(&mut asker, ask("hi")).await;
        let task = d1.task("answer").await;

        send_json(
            &mut attacker,
            serde_json::json!({"type":"result","task_id":task["task_id"],"text":"forged","tok_s":1.0,"tokens":1,"inference_ms":1}),
        )
        .await;
        assert!(next_of_type(&mut asker, "response", 300).await.is_none());

        d1.answer(&task, "real");
        let response = next_of_type(&mut asker, "response", 3000).await.unwrap();
        assert_eq!(response["text"], "real");
    }

    #[tokio::test]
    async fn a_second_result_from_the_same_drone_is_ignored() {
        let (hub, addr) = hub_serving().await;
        let drones = [
            Drone::join(&hub, addr, "d1").await,
            Drone::join(&hub, addr, "d2").await,
            Drone::join(&hub, addr, "d3").await,
        ];
        let mut asker = connect(addr).await;
        send_json(&mut asker, ask("hi")).await;
        let task = drones[0].task("answer").await;
        for _ in 0..3 {
            drones[0].answer(&task, "again");
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let state = hub.state.lock().await;
        let message = state
            .messages
            .values()
            .find(|m| m.task_type == TaskType::Answer)
            .unwrap();
        assert_eq!(message.results.len(), 1);
        assert_eq!(message.phase, MessagePhase::Generating);
    }
}
