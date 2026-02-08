use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: String,
    pub tier: String,
    pub model: String,
    pub tok_s: f64,
    pub load: f64,
}

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub task_id: String,
    pub peer_id: String,
    pub conn_id: usize,
    pub text: String,
    pub tokens: u32,
    pub inference_ms: u64,
    pub tok_s: f64,
}

#[derive(Debug, PartialEq)]
pub enum QuestionPhase {
    Generating,
    Judging,
    Done,
}

#[derive(Debug)]
pub struct PendingQuestion {
    pub question_id: String,
    pub questioner_conn_id: usize,
    pub text: String,
    /// task_id → conn_id for generation tasks
    pub task_peers: HashMap<String, usize>,
    pub results: Vec<TaskResult>,
    pub expected_results: usize,
    pub judge_task_id: Option<String>,
    pub judge_conn_id: Option<usize>,
    pub start: Instant,
    pub phase: QuestionPhase,
}

#[derive(Debug)]
pub struct HubState {
    pub peers: HashMap<usize, PeerInfo>,
    pub questions: HashMap<String, PendingQuestion>,
    pub task_to_question: HashMap<String, String>,
    pub busy_peers: HashSet<usize>,
    pub questions_today: u64,
}

impl HubState {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            questions: HashMap::new(),
            task_to_question: HashMap::new(),
            busy_peers: HashSet::new(),
            questions_today: 0,
        }
    }

    pub fn add_peer(&mut self, conn_id: usize, info: PeerInfo) {
        self.peers.insert(conn_id, info);
    }

    pub fn remove_peer(&mut self, conn_id: usize) -> Option<PeerInfo> {
        self.busy_peers.remove(&conn_id);
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
