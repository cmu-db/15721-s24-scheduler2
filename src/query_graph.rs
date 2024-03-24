#![allow(dead_code)]

use crate::scheduler::{Task, TaskStatus};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{mem, sync::Arc};
use tokio::sync::RwLock;

pub enum StageStatus {
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}

pub struct QueryStage {
    pub plan: Arc<dyn ExecutionPlan>,
    status: StageStatus,
    outputs: HashSet<u64>,
    inputs: HashSet<u64>,
}

pub struct QueryGraph {
    pub query_id: u64,
    tid_counter: AtomicU64, // TODO: add mutex to stages and make elements pointers to avoid copying
    pub stages: Vec<QueryStage>, // Can be a vec since all stages in a query are enumerated from 0.
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

    pub fn update_stage_status(
        &mut self,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        if self.stages.get(stage_id as usize).is_some() {
            match (&self.stages.get(stage_id as usize).unwrap().status, &status) {
                // TODO: handle input/output stuff
                (StageStatus::NotStarted, StageStatus::Running(_)) => {
                    self.stages.get_mut(stage_id as usize).unwrap().status = status;
                    Ok(())
                }
                (StageStatus::Running(_a), StageStatus::Finished(_b)) => {
                    let outputs = {
                        let stage = self.stages.get_mut(stage_id as usize).unwrap();
                        stage.status = status;
                        stage.outputs.clone()
                    };
                    // stage.status = status;
                    // Remove this stage from each output stage's input stage
                    for output_stage_id in &outputs {
                        if let Some(output_stage) = self.stages.get_mut(*output_stage_id as usize) {
                            output_stage.inputs.remove(&stage_id);

                            // Add output stage to frontier if its input size is zero
                            if output_stage.inputs.len() == 0 {
                                output_stage.status = StageStatus::Running(0); // TODO: "ready stage status?"
                                let new_output_task = Task {
                                    id: *output_stage_id, // same as stage ID for now
                                    query_id: self.query_id,
                                    stage_id: *output_stage_id,
                                    status: TaskStatus::Ready,
                                };
                                let _ = self.frontier_lock.blocking_write();
                                self.frontier.push(new_output_task);
                            }
                        } else {
                            return Err("Output stage not found.");
                        }
                    }
                    Ok(())
                }
                _ => Err("Mismatched stage statuses."),
            }
        } else {
            Err("Task not found.")
        }
    }
}
