#![allow(dead_code)]

pub enum WorkerStatus {
    Idle,
    Busy,
    Stopped,
}

struct ComputeRegion {
    node_id: u64,
    region_id: u64,
}

pub struct Worker {
    id: u32,
    status: WorkerStatus,
}

pub struct Dispatcher {
    // Internal worker state.
    workers: Vec<i32>,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    pub fn add_worker(&mut self, worker: Worker) {
        self.workers.push(worker.id as i32);
    }

    pub fn remove_worker(&mut self, worker_id: u32) {
        self.workers.retain(|&x| x != worker_id as i32);
    }

    pub fn get_workers(&self) -> Vec<i32> {
        self.workers.clone()
    }
}
