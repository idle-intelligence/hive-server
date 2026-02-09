use std::collections::{HashMap, HashSet};

use crate::state::PeerInfo;

/// Select up to `n` available peers.
/// `prefer_tier`: if Some, peers of that tier sort first. If None, random selection.
/// Within each group, peers are shuffled randomly.
pub fn select_peers(
    peers: &HashMap<usize, PeerInfo>,
    exclude: &HashSet<usize>,
    n: usize,
    prefer_tier: Option<&str>,
) -> Vec<(usize, PeerInfo)> {
    let mut candidates: Vec<(usize, &PeerInfo)> = peers
        .iter()
        .filter(|(id, _)| !exclude.contains(id))
        .map(|(&id, info)| (id, info))
        .collect();

    if candidates.is_empty() {
        return Vec::new();
    }

    // Shuffle for randomness within groups
    fastrand::shuffle(&mut candidates);

    if let Some(preferred) = prefer_tier {
        // Stable sort: preferred tier first, then others
        candidates.sort_by_key(|(_, info)| if info.tier == preferred { 0u8 } else { 1 });
    }

    candidates
        .into_iter()
        .take(n)
        .map(|(id, info)| (id, info.clone()))
        .collect()
}
