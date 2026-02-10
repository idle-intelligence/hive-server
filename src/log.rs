use std::collections::HashMap;

use chrono::Utc;

use crate::state::PeerInfo;

pub fn ts() -> String {
    Utc::now().format("%H:%M:%S").to_string()
}

pub fn peer_joined(peer_id: &str, model: &str, model_size: &str, tok_s: f64) -> String {
    format!("{} [peer] {} joined ({}, {}, {:.1} tok/s)", ts(), peer_id, model, model_size, tok_s)
}

pub fn peer_left(peer_id: &str, reason: &str) -> String {
    format!("{} [peer] {} left ({})", ts(), peer_id, reason)
}

pub fn task_assigned(task_id: &str, peers: &[(String, String)]) -> String {
    let list = peers.iter()
        .map(|(id, size)| format!("{} ({})", id, size))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{} [task {}] assigned to [{}]", ts(), task_id, list)
}

pub fn task_completed(task_id: &str, peer_id: &str, tokens: u32, inference_ms: u64, tok_s: f64) -> String {
    let secs = inference_ms as f64 / 1000.0;
    format!(
        "{} [task {}] {} responded ({} tok, {:.1}s, {:.1} tok/s)",
        ts(), task_id, peer_id, tokens, secs, tok_s
    )
}

pub fn question_received(q_id: &str, text: &str, route: Option<&str>, tagger_tokens: Option<u32>, tagger_ms: Option<u64>) -> String {
    let base = format!("{} [question {}] received: \"{}\"", ts(), q_id, text);
    match (route, tagger_tokens, tagger_ms) {
        (Some(r), Some(tok), Some(ms)) => format!("{base} (route:{r}, tagger: {tok} tok, {ms}ms)"),
        (Some(r), _, _) => format!("{base} (route:{r})"),
        _ => base,
    }
}

pub fn arbiter_assigned(arbiter_id: &str, peer_id: &str, model: &str, model_size: &str) -> String {
    format!("{} [arbiter {}] assigned to {} ({}, {})", ts(), arbiter_id, peer_id, model, model_size)
}

pub fn arbiter_completed(arbiter_id: &str, picked_peer: &str, candidates: usize, secs: f64) -> String {
    format!(
        "{} [arbiter {}] picked {} ({} candidates, {:.1}s)",
        ts(), arbiter_id, picked_peer, candidates, secs
    )
}

pub fn answer_delivered(q_id: &str, total_tokens: u32, total_secs: f64, peers_used: u32) -> String {
    format!(
        "{} [answer {}] delivered ({} total tok, {:.1}s, {} peers)",
        ts(), q_id, total_tokens, total_secs, peers_used
    )
}

pub fn time_result(peer_id: &str, result_text: &str, tokens: u32, flops: f64) -> String {
    let text = result_text.trim();
    format!(
        "{} [time] \"{}\" ({}, {} tok, {:.2e} FLOPS)",
        ts(), text, peer_id, tokens, flops
    )
}

pub fn time_skipped() -> String {
    format!("{} [time] skipped — no peers available", ts())
}

pub fn queue_full(q_id: &str) -> String {
    format!("{} [queue] {} rejected — queue full", ts(), q_id)
}

pub fn archivist_result(task_id: &str, text: &str) -> String {
    format!("{} [archivist {}] {}", ts(), task_id, text.trim())
}

pub fn archivist_failed(task_id: &str, reason: &str) -> String {
    format!("{} [archivist {}] failed ({})", ts(), task_id, reason)
}

pub fn swarm_status(peer_count: usize) -> String {
    format!("{} [swarm] {} Peer connected", ts(), peer_count)
}

pub fn swarm_stats(peers: &HashMap<usize, PeerInfo>, probes: usize, observers: usize, questions_today: u64, total_flops: f64) -> (String, Option<String>) {
    let total = peers.len();
    let mut total_tok_s = 0.0;

    for peer in peers.values() {
        total_tok_s += peer.tok_s;
    }

    let peer_line = format!(
        "{} [swarm] {} Peer | {:.1} tok/s | {} Question | {:.2e} total FLOPS",
        ts(), total, total_tok_s, questions_today, total_flops
    );

    let clients = probes + observers;
    let client_line = if clients > 0 {
        let mut parts = Vec::new();
        if probes > 0 { parts.push(format!("{probes} Probe")); }
        if observers > 0 { parts.push(format!("{observers} Observer")); }
        Some(format!("{} [swarm] {} Client : {}", ts(), clients, parts.join(", ")))
    } else {
        None
    };

    (peer_line, client_line)
}
