use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum Capability {
    Tagger,
    Answerer,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TaskType {
    Answer,
    Arbiter,
    Time,
    Archivist,
    Benchmark,
    Classify,
}

impl TaskType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskType::Answer => "answer",
            TaskType::Arbiter => "arbiter",
            TaskType::Time => "time",
            TaskType::Archivist => "archivist",
            TaskType::Benchmark => "benchmark",
            TaskType::Classify => "classify",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: String,
    pub model: String,
    pub model_size: String,
    pub tok_s: f64,
    pub load: f64, // TODO: V2 weighted routing (currently updated via heartbeat but not used)
    pub capabilities: HashSet<Capability>,
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
pub enum MessagePhase {
    Classifying,
    Generating,
    Arbitrating,
    Done,
}

#[derive(Debug)]
pub struct PendingMessage {
    pub message_id: String,
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
    pub phase: MessagePhase,
    pub task_type: TaskType,
    pub route_tag: Option<String>,
    pub classify_task_id: Option<String>,
    pub classify_conn_id: Option<usize>,
}

#[derive(Debug)]
pub struct QueuedMessage {
    pub message_id: String,
    pub probe_conn_id: usize,
    pub text: String,
    pub messages: Vec<crate::protocol::ChatMessage>,
    pub task_type: TaskType,
    pub route_tag: Option<String>,
    /// If Some, this queue entry is waiting for an arbiter, not generation.
    pub arbitration_for: Option<String>,
}

const MAX_NORMAL_QUEUE: usize = 20;
const MAX_LOW_QUEUE: usize = 10;

#[derive(Debug)]
pub struct HubState {
    pub peers: HashMap<usize, PeerInfo>,
    pub messages: HashMap<String, PendingMessage>,
    pub task_to_message: HashMap<String, String>,
    pub busy_answerers: HashSet<usize>,
    pub busy_taggers: HashSet<usize>,
    pub probes: HashSet<usize>,
    pub normal_queue: VecDeque<QueuedMessage>,
    pub low_queue: VecDeque<QueuedMessage>,
    pub messages_today: u64,
    pub total_flops: f64,
}

impl HubState {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            messages: HashMap::new(),
            task_to_message: HashMap::new(),
            busy_answerers: HashSet::new(),
            busy_taggers: HashSet::new(),
            probes: HashSet::new(),
            normal_queue: VecDeque::new(),
            low_queue: VecDeque::new(),
            messages_today: 0,
            total_flops: 0.0,
        }
    }

    pub fn add_peer(&mut self, conn_id: usize, info: PeerInfo) {
        self.peers.insert(conn_id, info);
    }

    pub fn remove_peer(&mut self, conn_id: usize) -> Option<PeerInfo> {
        self.busy_answerers.remove(&conn_id);
        self.busy_taggers.remove(&conn_id);
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

    pub fn normal_queue_full(&self) -> bool {
        self.normal_queue.len() >= MAX_NORMAL_QUEUE
    }

    pub fn low_queue_full(&self) -> bool {
        self.low_queue.len() >= MAX_LOW_QUEUE
    }
}
