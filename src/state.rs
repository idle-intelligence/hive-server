use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: String,
    pub tier: String,
    pub model: String,
    pub tok_s: f64,
    pub load: f64,
}

#[derive(Debug, Default)]
pub struct HubState {
    pub peers: HashMap<usize, PeerInfo>,
}

impl HubState {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    pub fn add_peer(&mut self, conn_id: usize, info: PeerInfo) {
        self.peers.insert(conn_id, info);
    }

    pub fn remove_peer(&mut self, conn_id: usize) -> Option<PeerInfo> {
        self.peers.remove(&conn_id)
    }

    pub fn update_load(&mut self, conn_id: usize, load: f64) {
        if let Some(peer) = self.peers.get_mut(&conn_id) {
            peer.load = load;
        }
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }
}
