#![allow(dead_code)]

use std::sync::Arc;

use crate::query_graph::{QueryGraph, QueryStage, StageStatus};
use crate::query_table::QueryTable;
use crate::task_queue::TaskQueue;
use crate::SchedulerError;
use datafusion::physical_plan::ExecutionPlan;

pub enum TaskStatus {
    Waiting,
    Ready,
    Running(u32), // ID of executor running this task
    Finished,
    Failed,
    Aborted,
}

pub struct Task {
    pub(crate) id: u64,
    pub(crate) query_id: u64,
    pub(crate) stage_id: u64,
    pub(crate) status: TaskStatus,
}

pub struct Scheduler {
    // Internal state of queries.
    query_table: QueryTable,

    // Task queue
    task_queue: TaskQueue,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            query_table: QueryTable::new(),
            task_queue: TaskQueue::new(),
        }
    }

    pub fn schedule_plan(&mut self, query_id: u64, plan: Arc<dyn ExecutionPlan>) {
        // Build a query graph from the plan.
        let query = QueryGraph::new(query_id, plan);
        let frontier = self.query_table.add_query(query);

        // Add the query to the task queue.
        self.task_queue.add_tasks(frontier);
    }

    pub fn update_stage_status(&mut self, query_id: u64, stage_id: u64, status: StageStatus) {
        // Update the status of the stage in the query graph.
        self.query_table
            .update_stage_status(query_id, stage_id, status);
    }

    pub fn store_result(&self, _result: Vec<u8>) {
        // Use either optimizer API to push result, or store in query table
    }

    pub fn update_task_state(&mut self, query_id: u64, task_id: u64) {
        // Update the status of the stage in the query graph.
        self.query_table
            .update_stage_status(query_id, task_id, StageStatus::Finished(0));

        // If new tasks are available, add them to the queue
        let frontier = self.query_table.get_frontier(query_id);
        self.task_queue.add_tasks(frontier);
    }

    pub fn next_task(&mut self) -> Result<(Task, Vec<u8>), SchedulerError> {
        let task = self.task_queue.next_task();
        let stage = self
            .query_table
            .get_plan_bytes(task.query_id, task.stage_id)?;
        Ok((task, stage))
    }
}
