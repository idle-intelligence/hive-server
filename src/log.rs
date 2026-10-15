use std::collections::HashMap;

use chrono::Utc;

use crate::state::PeerInfo;

pub fn ts() -> String {
    Utc::now().format("%H:%M:%S").to_string()
}

pub fn peer_joined_benchmarking(peer_id: &str, model: &str, model_size: &str) -> String {
    format!(
        "{} [peer] {} joined ({}, {}, benchmarking...)",
        ts(),
        peer_id,
        model,
        model_size
    )
}

pub fn peer_joined_tagger(peer_id: &str, model: &str, model_size: &str) -> String {
    format!(
        "{} [peer] {} joined ({}, {}, tagger-only)",
        ts(),
        peer_id,
        model,
        model_size
    )
}

pub fn peer_ready(peer_id: &str, tok_s: f64) -> String {
    format!("{} [peer] {} ready ({:.1} tok/s)", ts(), peer_id, tok_s)
}

pub fn peer_ready_tagger(peer_id: &str) -> String {
    format!("{} [peer] {} ready", ts(), peer_id)
}

pub fn peer_left(peer_id: &str, reason: &str) -> String {
    format!("{} [peer] {} left ({})", ts(), peer_id, reason)
}

pub fn task_assigned(task_id: &str, peers: &[(String, String)]) -> String {
    let list = peers
        .iter()
        .map(|(id, size)| format!("{} ({})", id, size))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{} [task {}] assigned to [{}]", ts(), task_id, list)
}

pub fn task_completed(
    task_id: &str,
    peer_id: &str,
    tokens: u32,
    inference_ms: u64,
    tok_s: f64,
) -> String {
    let secs = inference_ms as f64 / 1000.0;
    format!(
        "{} [task {}] {} responded ({} tok, {:.1}s, {:.1} tok/s)",
        ts(),
        task_id,
        peer_id,
        tokens,
        secs,
        tok_s
    )
}

pub fn message_received(m_id: &str, text: &str) -> String {
    format!(
        "{} [message {}] received ({} chars)",
        ts(),
        m_id,
        text.chars().count()
    )
}

pub fn arbiter_assigned(arbiter_id: &str, peer_id: &str, model: &str, model_size: &str) -> String {
    format!(
        "{} [arbiter {}] assigned to {} ({}, {})",
        ts(),
        arbiter_id,
        peer_id,
        model,
        model_size
    )
}

pub fn arbiter_completed(
    arbiter_id: &str,
    picked_peer: &str,
    candidates: usize,
    secs: f64,
) -> String {
    format!(
        "{} [arbiter {}] picked {} ({} candidates, {:.1}s)",
        ts(),
        arbiter_id,
        picked_peer,
        candidates,
        secs
    )
}

pub fn response_delivered(
    m_id: &str,
    total_tokens: u32,
    total_secs: f64,
    peers_used: u32,
) -> String {
    format!(
        "{} [response {}] delivered ({} total tok, {:.1}s, {} peers)",
        ts(),
        m_id,
        total_tokens,
        total_secs,
        peers_used
    )
}

pub fn time_result(peer_id: &str, tokens: u32, flops: f64) -> String {
    format!(
        "{} [time] time answered ({}, {} tok, {:.2e} FLOPS)",
        ts(),
        peer_id,
        tokens,
        flops
    )
}

pub fn time_skipped() -> String {
    format!("{} [time] skipped — no peers available", ts())
}

pub fn queue_full(m_id: &str) -> String {
    format!("{} [queue] {} rejected — queue full", ts(), m_id)
}

pub fn archivist_result(task_id: &str, text: &str) -> String {
    format!(
        "{} [archivist {}] filed ({} chars)",
        ts(),
        task_id,
        text.trim().chars().count()
    )
}

pub fn archivist_failed(task_id: &str, reason: &str) -> String {
    format!("{} [archivist {}] failed ({})", ts(), task_id, reason)
}

pub fn classify_assigned(task_id: &str, peer_id: &str) -> String {
    format!("{} [classify {}] assigned to {}", ts(), task_id, peer_id)
}

pub fn classify_completed(
    task_id: &str,
    peer_id: &str,
    route: &str,
    score: f64,
    ms: u64,
    routed: bool,
) -> String {
    if routed {
        format!(
            "{} [classify {}] {} → \"{}\" ({:.2}, {}ms)",
            ts(),
            task_id,
            peer_id,
            route,
            score,
            ms
        )
    } else if score < 0.6 {
        format!(
            "{} [classify {}] {} → \"{}\" ({:.2}, {}ms) below threshold, other",
            ts(),
            task_id,
            peer_id,
            route,
            score,
            ms
        )
    } else {
        // Score >= 0.6 but label is "other" (unrouted category)
        format!(
            "{} [classify {}] {} → \"{}\" ({:.2}, {}ms) unrouted",
            ts(),
            task_id,
            peer_id,
            route,
            score,
            ms
        )
    }
}

pub fn classify_timeout(task_id: &str) -> String {
    format!(
        "{} [classify {}] timed out, proceeding unrouted",
        ts(),
        task_id
    )
}

pub fn classify_no_tagger() -> String {
    format!(
        "{} [classify] no tagger available, proceeding unrouted",
        ts()
    )
}

pub fn swarm_status(peer_count: usize) -> String {
    format!("{} [swarm] {} Peer connected", ts(), peer_count)
}

pub fn swarm_stats(
    peers: &HashMap<usize, PeerInfo>,
    probes: usize,
    observers: usize,
    messages_today: u64,
    total_flops: f64,
) -> (String, Option<String>) {
    use crate::state::Capability;

    let total = peers.len();
    let mut answerer_count = 0;
    let mut tagger_only_count = 0;
    let mut total_tok_s = 0.0;
    let mut answerer_size_counts: std::collections::BTreeMap<&str, u32> =
        std::collections::BTreeMap::new();

    for peer in peers.values() {
        let has_answerer = peer.capabilities.contains(&Capability::Answerer);
        let has_tagger = peer.capabilities.contains(&Capability::Tagger);

        if has_answerer {
            answerer_count += 1;
            total_tok_s += peer.tok_s;
            *answerer_size_counts.entry(&peer.model_size).or_insert(0) += 1;
        } else if has_tagger {
            tagger_only_count += 1;
        }
    }

    let peer_line = if tagger_only_count > 0 {
        let sizes = answerer_size_counts
            .iter()
            .map(|(size, count)| format!("{count} {size}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{} [swarm] {} Peer: {} answerer ({}, {:.1} tok/s) + {} tagger | {} Message | {:.2e} total FLOPS",
            ts(), total, answerer_count, sizes, total_tok_s, tagger_only_count, messages_today, total_flops
        )
    } else {
        let sizes = answerer_size_counts
            .iter()
            .map(|(size, count)| format!("{count} {size}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{} [swarm] {} Peer: {} | {:.1} tok/s | {} Message | {:.2e} total FLOPS",
            ts(),
            total,
            sizes,
            total_tok_s,
            messages_today,
            total_flops
        )
    };

    let clients = probes + observers;
    let client_line = if clients > 0 {
        let mut parts = Vec::new();
        if probes > 0 {
            parts.push(format!("{probes} Probe"));
        }
        if observers > 0 {
            parts.push(format!("{observers} Observer"));
        }
        Some(format!(
            "{} [swarm] {} Client : {}",
            ts(),
            clients,
            parts.join(", ")
        ))
    } else {
        None
    };

    (peer_line, client_line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_received_hides_the_question() {
        let line = message_received("m_1", "my secret question");
        assert!(!line.contains("secret"));
        assert!(line.contains("m_1") && line.contains("18 chars"));
    }

    #[test]
    fn archivist_result_hides_model_text() {
        let line = archivist_result("m_1", "  STORY:music a summary  ");
        assert!(!line.contains("STORY") && !line.contains("summary"));
        assert!(line.contains("m_1") && line.contains("filed"));
    }

    #[test]
    fn time_result_says_time_answered() {
        let line = time_result("p1", 7, 1e9);
        assert!(line.contains("time answered") && line.contains("p1"));
    }
}
