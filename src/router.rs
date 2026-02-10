use std::collections::{HashMap, HashSet};

use crate::state::PeerInfo;

/// Select up to `n` available peers, randomly shuffled.
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

    if candidates.is_empty() {
        return Vec::new();
    }

    fastrand::shuffle(&mut candidates);

    candidates
        .into_iter()
        .take(n)
        .map(|(id, info)| (id, info.clone()))
        .collect()
}
