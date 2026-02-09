use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

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
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        messages: Option<Vec<ChatMessage>>,
    },
}

/// Messages sent from the hub to clients.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubMsg {
    Task {
        task_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        question_id: Option<String>,
        system: String,
        messages: Vec<ChatMessage>,
        max_tokens: u32,
    },
    Log {
        line: String,
    },
    Answer {
        question_id: String,
        text: String,
        peer_id: String,
        total_tokens: u32,
        total_ms: u64,
        peers_used: u32,
    },
    Error {
        text: String,
    },
}
