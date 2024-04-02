#![allow(dead_code)]

use crate::task::Task;
use std::collections::VecDeque;
use std::sync::Arc;

pub enum WorkerStatus {
    Idle,
    Busy,
    Stopped,
}

// This should be information we get from the executor at registration time.
// Exact details are TBD.
struct ComputeRegion {
    node_id: u64,
    region_id: u64,
}

// Static information about workers, as well as, some state about what they are doing right now.
pub struct Worker {
    id: u32,
    status: WorkerStatus,
}

pub struct Dispatcher {
    // Pointer to task queue that is owned by the scheduler.
    task_queue: Arc<VecDeque<Task>>,

    // Internal worker state.
    workers: Vec<Worker>,
    // Need some datastructure to select next worker to assign a task to.
}

impl Dispatcher {
    pub fn new(task_queue: Arc<VecDeque<Task>>) -> Self {
        Self {
            task_queue,
            workers: Vec::new(),
        }
    }

    pub fn add_worker(&mut self, worker: Worker) {
        self.workers.push(worker);
    }

    pub fn remove_worker(&mut self, worker_id: u32) {
        self.workers.retain(|worker| worker.id != worker_id);
    }
}
