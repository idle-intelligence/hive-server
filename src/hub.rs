use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::log as hlog;
use crate::protocol::{ClientMsg, HubMsg};
use crate::state::{HubState, PeerInfo};

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

    async fn handle_message(&self, conn_id: usize, msg: ClientMsg) {
        match msg {
            ClientMsg::Register { peer_id, tier, model, tok_s } => {
                let line = hlog::peer_joined(&peer_id, &model, &tier, tok_s);
                self.emit(&line).await;

                let info = PeerInfo {
                    peer_id: peer_id.clone(),
                    tier: tier.clone(),
                    model: model.clone(),
                    tok_s,
                    load: 0.0,
                };
                self.state.lock().await.add_peer(conn_id, info);

                let count = self.state.lock().await.peer_count();
                let status = hlog::swarm_status(count);
                self.emit(&status).await;

                // Send test task
                let task_id = format!("task_{}", &uuid::Uuid::new_v4().to_string()[..8]);
                let task = HubMsg::Task {
                    task_id: task_id.clone(),
                    system: "You are helpful.".to_string(),
                    prompt: "What is 2+2?".to_string(),
                    max_tokens: 256,
                };

                let task_line = hlog::task_assigned(&task_id, &peer_id, &model, &tier);
                self.emit(&task_line).await;

                self.send_to(conn_id, &task).await;
            }

            ClientMsg::Result { task_id, text, tok_s, tokens, inference_ms } => {
                let state = self.state.lock().await;
                let peer_id = state.peers.get(&conn_id)
                    .map(|p| p.peer_id.clone())
                    .unwrap_or_else(|| format!("conn_{conn_id}"));
                drop(state);

                let line = hlog::task_completed(&task_id, &peer_id, tokens, inference_ms, tok_s);
                self.emit(&line).await;

                // Log the actual result text
                let ts = hlog::ts();
                let result_line = format!("{ts} [task {task_id}] result: \"{text}\"");
                self.emit(&result_line).await;
            }

            ClientMsg::Heartbeat { peer_id: _, load } => {
                self.state.lock().await.update_load(conn_id, load);
                // Heartbeats are silent — no log line to avoid noise
            }

            ClientMsg::Question { text } => {
                let ts = hlog::ts();
                let q_id = format!("q_{}", &uuid::Uuid::new_v4().to_string()[..8]);
                let line = format!("{ts} [question {q_id}] received: \"{text}\"");
                self.emit(&line).await;
                // Full question routing is post-milestone-1
            }
        }
    }

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
