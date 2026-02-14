use std::collections::{HashMap, HashSet};

use crate::state::{Capability, PeerInfo};

/// Select up to `n` available peers, randomly shuffled.
/// Filters by required capability and busyness.
/// If prefer_exclusive is true, prefers peers with ONLY the required capability.
pub fn select_peers(
    peers: &HashMap<usize, PeerInfo>,
    exclude: &HashSet<usize>,
    busy_for_capability: &HashSet<usize>,
    n: usize,
    required: Capability,
    prefer_exclusive: bool,
) -> Vec<(usize, PeerInfo)> {
    // Filter by capability, exclude, and busyness
    let mut candidates: Vec<(usize, &PeerInfo)> = peers
        .iter()
        .filter(|(id, info)| {
            info.capabilities.contains(&required)
                && !exclude.contains(id)
                && !busy_for_capability.contains(id)
        })
        .map(|(&id, info)| (id, info))
        .collect();

    if candidates.is_empty() {
        return Vec::new();
    }

    // If prefer_exclusive, separate into exclusive vs dual-capability groups
    if prefer_exclusive {
        let (mut exclusive, mut dual): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, info)| info.capabilities.len() == 1);

        // Shuffle within each group
        fastrand::shuffle(&mut exclusive);
        fastrand::shuffle(&mut dual);

        // Concatenate: exclusive first, then dual
        exclusive.extend(dual);
        candidates = exclusive;
    } else {
        // Just shuffle everything
        fastrand::shuffle(&mut candidates);
    }

    // Take first N
    candidates
        .into_iter()
        .take(n)
        .map(|(id, info)| (id, info.clone()))
        .collect()
}
