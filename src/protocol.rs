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
        model: String,
        model_size: String,
        #[serde(default)]
        tok_s: Option<f64>,
        #[serde(default)]
        capabilities: Option<Vec<String>>,
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
    Message {
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        messages: Option<Vec<ChatMessage>>,
        // Accepted from old clients, ignored since drones-are-drones refactor
        #[serde(default)]
        #[allow(dead_code)]
        route: Option<String>,
        // Accepted from old clients, ignored since drones-are-drones refactor
        #[serde(default)]
        #[allow(dead_code)]
        classifier_ms: Option<u64>,
        // Accepted from old clients, ignored since drones-are-drones refactor
        #[serde(default)]
        #[allow(dead_code)]
        classifier_score: Option<f64>,
    },
}

/// Messages sent from the hub to clients.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubMsg {
    Task {
        task_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
        task_type: String,
        system: String,
        messages: Vec<ChatMessage>,
        max_tokens: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
    Log {
        line: String,
    },
    Response {
        message_id: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_register_with_defaults() {
        let m: ClientMsg = serde_json::from_str(
            r#"{"type":"register","peer_id":"p1","model":"m","model_size":"1B"}"#,
        )
        .unwrap();
        match m {
            ClientMsg::Register {
                peer_id,
                tok_s,
                capabilities,
                ..
            } => {
                assert_eq!(peer_id, "p1");
                assert!(tok_s.is_none() && capabilities.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn parses_message_ignoring_old_fields() {
        let m: ClientMsg = serde_json::from_str(
            r#"{"type":"message","text":"hi","route":"swarm","classifier_ms":3,"classifier_score":0.9}"#,
        )
        .unwrap();
        assert!(matches!(m, ClientMsg::Message { text: Some(t), .. } if t == "hi"));
    }

    #[test]
    fn rejects_unknown_type() {
        assert!(serde_json::from_str::<ClientMsg>(r#"{"type":"nope"}"#).is_err());
    }

    #[test]
    fn log_serializes_with_type_tag() {
        let s = serde_json::to_string(&HubMsg::Log { line: "x".into() }).unwrap();
        assert_eq!(s, r#"{"type":"log","line":"x"}"#);
    }
}
