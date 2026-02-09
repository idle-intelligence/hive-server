use std::collections::HashMap;

use chrono::Utc;

use crate::state::PeerInfo;

pub fn ts() -> String {
    Utc::now().format("%H:%M:%S").to_string()
}

pub fn peer_joined(peer_id: &str, model: &str, tier: &str, tok_s: f64) -> String {
    if tier == "carrier" {
        format!("{} [peer] {} joined ({}, {}, {:.1} tok/s) — Carrier has arrived.", ts(), peer_id, model, tier, tok_s)
    } else {
        format!("{} [peer] {} joined ({}, {}, {:.1} tok/s)", ts(), peer_id, model, tier, tok_s)
    }
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

pub fn arbiter_assigned(arbiter_id: &str, peer_id: &str, model: &str, tier: &str) -> String {
    format!("{} [arbiter {}] assigned to {} ({}, {})", ts(), arbiter_id, peer_id, model, tier)
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

pub fn task_queued(q_id: &str, queue_len: usize) -> String {
    format!("{} [queue] {} queued ({} in queue)", ts(), q_id, queue_len)
}

pub fn queue_dispatch(q_id: &str, queue_len: usize) -> String {
    format!("{} [queue] {} dispatched ({} remaining)", ts(), q_id, queue_len)
}

pub fn arbiter_queued(q_id: &str, queue_len: usize) -> String {
    format!("{} [queue] {} awaiting arbiter ({} in queue)", ts(), q_id, queue_len)
}

pub fn queue_full(q_id: &str) -> String {
    format!("{} [queue] {} rejected — queue full", ts(), q_id)
}

pub fn swarm_status(peer_count: usize) -> String {
    format!("{} [swarm] {} Peer connected", ts(), peer_count)
}

pub fn swarm_stats(peers: &HashMap<usize, PeerInfo>, probes: usize, observers: usize, questions_today: u64, total_flops: f64) -> String {
    let mut n_carrier = 0u32;
    let mut n_drone = 0u32;
    let mut n_probe_tier = 0u32;
    let mut total_tok_s = 0.0;

    for peer in peers.values() {
        match peer.tier.as_str() {
            "carrier" => n_carrier += 1,
            "drone" => n_drone += 1,
            _ => n_probe_tier += 1,
        }
        total_tok_s += peer.tok_s;
    }

    let total = peers.len();
    let mut tiers = Vec::new();
    if n_carrier > 0 { tiers.push(format!("{n_carrier} Carrier")); }
    if n_drone > 0 { tiers.push(format!("{n_drone} Drone")); }
    if n_probe_tier > 0 { tiers.push(format!("{n_probe_tier} Probe")); }
    let tier_str = if tiers.is_empty() {
        String::new()
    } else {
        format!(" ({})", tiers.join(", "))
    };

    format!(
        "{} [swarm] {} Peer{}, {} Probe, {} Observer | {:.1} tok/s | {} Question | {:.2e} total FLOPS",
        ts(), total, tier_str, probes, observers, total_tok_s, questions_today, total_flops
    )
}
