use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskType {
    Answer,
    Arbiter,
    Time,
    Archivist,
}

impl TaskType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskType::Answer => "answer",
            TaskType::Arbiter => "arbiter",
            TaskType::Time => "time",
            TaskType::Archivist => "archivist",
        }
    }
}

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
    Arbitrating,
    Done,
}

#[derive(Debug)]
pub struct PendingQuestion {
    pub question_id: String,
    pub probe_conn_id: usize,
    pub text: String,
    pub messages: Vec<crate::protocol::ChatMessage>,
    pub task_id: String,
    pub gen_peers: Vec<usize>,
    pub results: Vec<TaskResult>,
    pub expected_results: usize,
    pub arbiter_task_id: Option<String>,
    pub arbiter_conn_id: Option<usize>,
    pub start: Instant,
    pub phase: QuestionPhase,
    pub task_type: TaskType,
}

#[derive(Debug)]
pub struct QueuedQuestion {
    pub question_id: String,
    pub probe_conn_id: usize,
    pub text: String,
    pub messages: Vec<crate::protocol::ChatMessage>,
    pub task_type: TaskType,
    /// If Some, this queue entry is waiting for an arbiter, not generation.
    pub arbitration_for: Option<String>,
}

const MAX_NORMAL_QUEUE: usize = 20;
const MAX_LOW_QUEUE: usize = 10;

#[derive(Debug)]
pub struct HubState {
    pub peers: HashMap<usize, PeerInfo>,
    pub questions: HashMap<String, PendingQuestion>,
    pub task_to_question: HashMap<String, String>,
    pub busy_peers: HashSet<usize>,
    pub probes: HashSet<usize>,
    pub normal_queue: VecDeque<QueuedQuestion>,
    pub low_queue: VecDeque<QueuedQuestion>,
    pub questions_today: u64,
    pub total_flops: f64,
}

impl HubState {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            questions: HashMap::new(),
            task_to_question: HashMap::new(),
            busy_peers: HashSet::new(),
            probes: HashSet::new(),
            normal_queue: VecDeque::new(),
            low_queue: VecDeque::new(),
            questions_today: 0,
            total_flops: 0.0,
        }
    }

    pub fn add_peer(&mut self, conn_id: usize, info: PeerInfo) {
        self.peers.insert(conn_id, info);
    }

    pub fn remove_peer(&mut self, conn_id: usize) -> Option<PeerInfo> {
        self.busy_peers.remove(&conn_id);
        self.probes.remove(&conn_id);
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

    pub fn queue_len(&self) -> usize {
        self.normal_queue.len() + self.low_queue.len()
    }

    pub fn normal_queue_full(&self) -> bool {
        self.normal_queue.len() >= MAX_NORMAL_QUEUE
    }

    pub fn low_queue_full(&self) -> bool {
        self.low_queue.len() >= MAX_LOW_QUEUE
    }
}
