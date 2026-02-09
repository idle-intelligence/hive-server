use std::collections::{HashMap, HashSet};

use crate::state::PeerInfo;

/// Select up to `n` available peers, preferring higher tier and lower load.
/// Excludes peers whose conn_id is in `exclude`.
pub fn select_peers(
    peers: &HashMap<usize, PeerInfo>,
    exclude: &HashSet<usize>,
    n: usize,
) -> Vec<(usize, PeerInfo)> {
    let mut candidates: Vec<(usize, &PeerInfo)> = peers
        .iter()
        .filter(|(id, _)| !exclude.contains(id))
        .map(|(&id, info)| (id, info))
        .collect();

    // Sort: carrier first, then drone, then probe; break ties by lower load
    candidates.sort_by(|a, b| {
        let tier_rank = |t: &str| match t {
            "carrier" => 0u8,
            "drone" => 1,
            _ => 2,
        };
        tier_rank(&a.1.tier)
            .cmp(&tier_rank(&b.1.tier))
            .then(a.1.load.partial_cmp(&b.1.load).unwrap_or(std::cmp::Ordering::Equal))
    });

    candidates
        .into_iter()
        .take(n)
        .map(|(id, info)| (id, info.clone()))
        .collect()
}
