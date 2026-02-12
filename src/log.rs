use std::collections::HashMap;

use chrono::Utc;

use crate::state::PeerInfo;

pub fn ts() -> String {
    Utc::now().format("%H:%M:%S").to_string()
}

pub fn peer_joined_benchmarking(peer_id: &str, model: &str, model_size: &str) -> String {
    format!("{} [peer] {} joined ({}, {}, benchmarking...)", ts(), peer_id, model, model_size)
}

pub fn peer_ready(peer_id: &str, tok_s: f64) -> String {
    format!("{} [peer] {} ready ({:.1} tok/s)", ts(), peer_id, tok_s)
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

pub fn message_received(m_id: &str, text: &str, route: Option<&str>, classifier_score: Option<f64>, classifier_ms: Option<u64>) -> String {
    let base = format!("{} [message {}] received: \"{}\"", ts(), m_id, text);
    match (route, classifier_score, classifier_ms) {
        (Some(r), Some(score), Some(ms)) => format!("{base} (route:{r}, classifier: {score:.2} score, {ms}ms)"),
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

pub fn response_delivered(m_id: &str, total_tokens: u32, total_secs: f64, peers_used: u32) -> String {
    format!(
        "{} [response {}] delivered ({} total tok, {:.1}s, {} peers)",
        ts(), m_id, total_tokens, total_secs, peers_used
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

pub fn queue_full(m_id: &str) -> String {
    format!("{} [queue] {} rejected — queue full", ts(), m_id)
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

pub fn swarm_stats(peers: &HashMap<usize, PeerInfo>, probes: usize, observers: usize, messages_today: u64, total_flops: f64) -> (String, Option<String>) {
    let total = peers.len();
    let mut total_tok_s = 0.0;
    let mut size_counts: std::collections::BTreeMap<&str, u32> = std::collections::BTreeMap::new();

    for peer in peers.values() {
        total_tok_s += peer.tok_s;
        *size_counts.entry(&peer.model_size).or_insert(0) += 1;
    }

    let sizes = size_counts.iter()
        .map(|(size, count)| format!("{count} {size}"))
        .collect::<Vec<_>>()
        .join(", ");

    let peer_line = format!(
        "{} [swarm] {} Peer: {} | {:.1} tok/s | {} Message | {:.2e} total FLOPS",
        ts(), total, sizes, total_tok_s, messages_today, total_flops
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
