use chrono::Utc;

pub fn ts() -> String {
    Utc::now().format("%H:%M:%S").to_string()
}

pub fn peer_joined(peer_id: &str, model: &str, tier: &str, tok_s: f64) -> String {
    format!("{} [peer] {} joined ({}, {}, {:.1} tok/s)", ts(), peer_id, model, tier, tok_s)
}

pub fn peer_left(peer_id: &str, reason: &str) -> String {
    format!("{} [peer] {} left ({})", ts(), peer_id, reason)
}

pub fn task_assigned(task_id: &str, peer_id: &str, model: &str, tier: &str) -> String {
    format!("{} [task {}] assigned to {} ({}, {})", ts(), task_id, peer_id, model, tier)
}

pub fn task_completed(task_id: &str, peer_id: &str, tokens: u32, inference_ms: u64, tok_s: f64) -> String {
    let secs = inference_ms as f64 / 1000.0;
    format!(
        "{} [task {}] {} responded ({} tok, {:.1}s, {:.1} tok/s)",
        ts(), task_id, peer_id, tokens, secs, tok_s
    )
}

pub fn swarm_status(peer_count: usize) -> String {
    format!("{} [swarm] {} peer(s) connected", ts(), peer_count)
}

pub fn heartbeat_received(peer_id: &str, load: f64) -> String {
    format!("{} [peer] {} heartbeat (load: {:.2})", ts(), peer_id, load)
}
