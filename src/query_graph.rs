#![allow(dead_code)]

use crate::scheduler::Task;
use datafusion::physical_plan::ExecutionPlan;
use tokio::sync::RwLock;
use std::{mem, sync::Arc};
use std::sync::atomic::{AtomicU64, Ordering};

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
    tid_counter: AtomicU64,
    stages: Vec<QueryStage>, // Can be a vec since all stages in a query are enumerated from 0.
    plan: Arc<dyn ExecutionPlan>, // Potentially can be thrown away at this point.
    frontier: Vec<Task>,
    frontier_lock: tokio::sync::RwLock<()>,
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            query_id,
            plan,
            tid_counter: AtomicU64::new(0),
            stages: Vec::new(),
            frontier: Vec::new(),
            frontier_lock: RwLock::new(()),
        }
    }

    fn next_task_id(&mut self) -> u64 {
        self.tid_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Atomically clear frontier vector and return old frontier.
    pub fn get_frontier(&mut self) -> Vec<Task> {
        let _ = self.frontier_lock.blocking_write();
        let mut old_frontier = Vec::new();
        mem::swap(&mut self.frontier, &mut old_frontier);
        self.frontier = Vec::new();
        old_frontier
    }

    pub fn update_stage_status(&mut self, stage_id: u64, status: StageStatus) -> Result<(), &'static str> {
        if let Some(stage) = self.stages.get_mut(stage_id as usize) {
            match (&stage.status, status) {
                // TODO: handle input/output stuff
                (StageStatus::NotStarted, StageStatus::Running(_)) => Ok(()),
                (StageStatus::Running(_a), StageStatus::Finished(_b)) => Ok(()),
                _ => Err("Mismatched stage statuses.")
            }
        } else {
            Err("Task not found.")
        }
    }
}

enum TaskStatus {
    Waiting,
    Ready,
    Running(i32), // ID of executor running this task
    Finished,
    Failed,
}
