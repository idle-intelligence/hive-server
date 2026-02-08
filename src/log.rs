use std::collections::HashMap;

use chrono::Utc;

use crate::state::PeerInfo;

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

pub fn question_received(q_id: &str, text: &str) -> String {
    format!("{} [question {}] received: \"{}\"", ts(), q_id, text)
}

pub fn judge_assigned(judge_id: &str, peer_id: &str, model: &str, tier: &str) -> String {
    format!("{} [judge {}] assigned to {} ({}, {})", ts(), judge_id, peer_id, model, tier)
}

pub fn judge_completed(judge_id: &str, picked_peer: &str, candidates: usize, secs: f64) -> String {
    format!(
        "{} [judge {}] picked {} ({} candidates, {:.1}s)",
        ts(), judge_id, picked_peer, candidates, secs
    )
}

pub fn answer_delivered(q_id: &str, total_tokens: u32, total_secs: f64, peers_used: u32) -> String {
    format!(
        "{} [answer {}] delivered ({} total tok, {:.1}s, {} peers)",
        ts(), q_id, total_tokens, total_secs, peers_used
    )
}

pub fn error_no_peers(q_id: &str) -> String {
    format!("{} [error] no peers available for question {}", ts(), q_id)
}

pub fn swarm_status(peer_count: usize) -> String {
    format!("{} [swarm] {} peer(s) connected", ts(), peer_count)
}

pub fn swarm_stats(peers: &HashMap<usize, PeerInfo>, questions_today: u64) -> String {
    let mut heavy = 0u32;
    let mut worker = 0u32;
    let mut lite = 0u32;
    let mut total_tok_s = 0.0;

    for peer in peers.values() {
        match peer.tier.as_str() {
            "heavy" => heavy += 1,
            "worker" => worker += 1,
            _ => lite += 1,
        }
        total_tok_s += peer.tok_s;
    }

    let total = peers.len();
    let mut tiers = Vec::new();
    if heavy > 0 { tiers.push(format!("{heavy} heavy")); }
    if worker > 0 { tiers.push(format!("{worker} worker")); }
    if lite > 0 { tiers.push(format!("{lite} lite")); }
    let tier_str = if tiers.is_empty() {
        String::new()
    } else {
        format!(" ({})", tiers.join(", "))
    };

    format!(
        "{} [swarm] {} peers{} | {:.1} tok/s | {} questions today",
        ts(), total, tier_str, total_tok_s, questions_today
    )
}
