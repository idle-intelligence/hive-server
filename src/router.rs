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

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, caps: &[Capability]) -> PeerInfo {
        PeerInfo {
            peer_id: id.to_string(),
            model: "m".to_string(),
            model_size: "1B".to_string(),
            tok_s: 10.0,
            load: 0.0,
            capabilities: caps.iter().copied().collect(),
        }
    }

    fn peers() -> HashMap<usize, PeerInfo> {
        let mut m = HashMap::new();
        m.insert(1, peer("a", &[Capability::Answerer]));
        m.insert(2, peer("b", &[Capability::Answerer, Capability::Tagger]));
        m.insert(3, peer("t", &[Capability::Tagger]));
        m
    }

    #[test]
    fn filters_by_capability() {
        let got = select_peers(
            &peers(),
            &HashSet::new(),
            &HashSet::new(),
            10,
            Capability::Tagger,
            false,
        );
        let mut ids: Vec<_> = got.iter().map(|(id, _)| *id).collect();
        ids.sort();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn respects_exclude_busy_and_limit() {
        let exclude: HashSet<usize> = [1].into();
        let busy: HashSet<usize> = [2].into();
        let got = select_peers(&peers(), &exclude, &busy, 10, Capability::Answerer, false);
        assert!(got.is_empty());
        let got = select_peers(
            &peers(),
            &HashSet::new(),
            &HashSet::new(),
            1,
            Capability::Answerer,
            false,
        );
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn prefers_exclusive_peers() {
        for _ in 0..20 {
            let got = select_peers(
                &peers(),
                &HashSet::new(),
                &HashSet::new(),
                1,
                Capability::Answerer,
                true,
            );
            assert_eq!(got[0].0, 1);
        }
    }
}
