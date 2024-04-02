#![allow(dead_code)]

use crate::task::{Task, TaskStatus};
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashSet;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{mem, sync::Arc};
use tokio::sync::RwLock;
use crate::api::composable_database::TaskId;
use crate::query_graph::StageStatus::NotStarted;

#[derive(Debug, Default)]
pub enum StageStatus {
    #[default]
    NotStarted,
    Running(u64),
    Finished(u64), // More detailed datatype to describe location(s) of ALL output data.
}
#[derive(Debug)]
pub struct QueryStage {
    pub plan: Arc<dyn ExecutionPlan>,
    status: StageStatus,
    outputs: HashSet<u64>,
    inputs: HashSet<u64>,
}
#[derive(Debug)]
pub struct QueryGraph {
    pub query_id: u64,
    pub done: bool,
    tid_counter: AtomicU64, // TODO: add mutex to stages and make elements pointers to avoid copying
    pub stages: Vec<QueryStage>, // Can be a vec since all stages in a query are enumerated from 0.
    plan: Arc<dyn ExecutionPlan>, // Potentially can be thrown away at this point.
    frontier: tokio::sync::RwLock<Vec<Task>>,
}

impl QueryGraph {
    pub fn new(query_id: u64, plan: Arc<dyn ExecutionPlan>) -> Self {

        Self {
            query_id,
            done: false,
            plan: plan.clone(),
            tid_counter: AtomicU64::new(0),
            stages: vec![QueryStage{plan: plan.clone(), status: NotStarted, outputs: HashSet::new(), inputs: HashSet::new()}],
            frontier: RwLock::new(Vec::new()),
        }
    }

    pub fn num_stages(&self) -> u64 {
        self.stages.len() as u64
    }

    fn next_task_id(&mut self) -> u64 {
        self.tid_counter.fetch_add(1, Ordering::SeqCst)
    }

    // Atomically clear frontier vector and return old frontier.
    pub async fn get_frontier(&self) -> Vec<Task> {
        let f = self.frontier.write();
        let mut old_frontier = Vec::new();
        mem::swap(f.await.deref_mut(), &mut old_frontier);
        old_frontier
    }

    pub async fn update_stage_status(
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

                    if outputs.is_empty() {
                        self.done = true;
                    }
                    // stage.status = status;
                    // Remove this stage from each output stage's input stage
                    for output_stage_id in &outputs {
                        if let Some(output_stage) = self.stages.get_mut(*output_stage_id as usize) {
                            output_stage.inputs.remove(&stage_id);

                            // Add output stage to frontier if its input size is zero
                            if output_stage.inputs.len() == 0 {
                                output_stage.status = StageStatus::Running(0); // TODO: "ready stage status?"
                                let new_output_task = Task {
                                    task_id: TaskId{
                                        query_id: self.query_id,
                                        task_id: *output_stage_id,
                                        stage_id: *output_stage_id
                                    },
                                    status: TaskStatus::Ready,
                                };
                                self.frontier.write().await.push(new_output_task);
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
