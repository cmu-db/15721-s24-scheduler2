use crate::parser::serialize_physical_plan;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::scheduler::Task;
use futures::executor;
use std::cell::RefCell;
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};
use crate::SchedulerError;
use std::sync::Arc;

pub struct QueryTable {
    // Maps query IDs to query graphs
    table: tokio::sync::RwLock<HashMap<u64, RefCell<QueryGraph>>>,
}

impl QueryTable {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_frontier(&self, query_id: u64) -> Vec<Task> {
        executor::block_on(async {
            let mut t = self.table.write().await;
            let frontier = t.get(&query_id).unwrap().borrow_mut().get_frontier();
            frontier
        })
    }

    #[must_use]
    pub fn add_query(&mut self, mut graph: QueryGraph) -> Vec<Task> {
        executor::block_on(async {
            let mut t = self.table.write().await;
            let frontier = graph.get_frontier();
            (*t).insert(graph.query_id, RefCell::new(graph));
            frontier
        })
    }

    #[must_use]
    pub async fn update_stage_status(
        &mut self,
        query_id: u64,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        let t = self.table.read().await;
        if let Some(graph) = t.get(&query_id) {
            graph.borrow_mut().update_stage_status(stage_id, status)?;
            Ok(())
        } else {
            Err("Graph not found.")
        }
    }

    pub fn get_plan_bytes(&mut self, query_id: u64, stage_id: u64) 
    -> Result<Vec<u8>, SchedulerError> {
        let t = self.table.blocking_read();
        if let Some(graph) = t.get(&query_id) {
            let plan = Arc::clone(&graph.borrow().stages[stage_id as usize].plan);
            executor::block_on(async{
                match serialize_physical_plan(plan).await {
                    Ok(p) => Ok(p),
                    Err(e) => Err(SchedulerError::DfError(e))
                }})
        } else {
            Err(SchedulerError::Error("Graph not found.".to_string()))
        }
    }

    pub async fn cancel_query(&mut self, query_id: u64) -> bool {
        true
    }
}
