#![allow(dead_code)]

use crate::scheduler::Task;
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::RwLock;
use std::{mem, sync::Arc};

pub enum StageStatus {
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}

pub struct QueryStage {
    plan: Arc<dyn ExecutionPlan>,
    status: StageStatus,
    outputs: Vec<u64>,
    inputs: Vec<u64>,
}

pub struct QueryGraph {
    pub query_id: u64,
    plan: Arc<dyn ExecutionPlan>, // Potentially can be thrown away at this point.

    stages: Vec<QueryStage>,
    frontier: Vec<Task>,
    frontier_lock: tokio::sync::RwLock<()>,
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            query_id,
            plan,
            stages: Vec::new(),
            frontier: Vec::new(),
            frontier_lock: RwLock::new(()),
        }
    }

    // Atomically clear frontier vector and return old frontier.
    pub fn get_frontier(&mut self) -> Vec<Task> {
        let _ = self.frontier_lock.blocking_write();
        let mut old_frontier = Vec::new();
        mem::swap(&mut self.frontier, &mut old_frontier);
        self.frontier = Vec::new();
        old_frontier
    }
}

enum TaskStatus {
    Waiting,
    Ready,
    Running(i32), // ID of executor running this task
    Finished,
    Failed,
}
