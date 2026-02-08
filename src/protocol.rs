use serde::{Deserialize, Serialize};

/// Messages sent from clients to the hub.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMsg {
    Register {
        peer_id: String,
        tier: String,
        model: String,
        tok_s: f64,
    },
    Result {
        task_id: String,
        text: String,
        tok_s: f64,
        tokens: u32,
        inference_ms: u64,
    },
    Heartbeat {
        peer_id: String,
        load: f64,
    },
    Question {
        text: String,
    },
}

/// Messages sent from the hub to clients.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubMsg {
    Task {
        task_id: String,
        system: String,
        prompt: String,
        max_tokens: u32,
    },
    Log {
        line: String,
    },
}
