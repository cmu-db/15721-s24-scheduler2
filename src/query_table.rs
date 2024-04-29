use crate::composable_database::QueryStatus;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::task::Task;
use crate::SchedulerError;
use dashmap::DashMap;
use datafusion_proto::bytes::physical_plan_to_bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct QueryTable {
    // Maps query IDs to query graphs
    pub table: DashMap<u64, RwLock<QueryGraph>>, // TODO: make private
    qid_counter: AtomicU64,
}

impl QueryTable {
    pub fn new() -> Self {
        Self {
            table: DashMap::new(),
            qid_counter: AtomicU64::new(0),
        }
    }

    fn next_query_id(&self) -> u64 {
        self.qid_counter.fetch_add(1, Ordering::SeqCst)
    }


    pub async fn get_frontier(&self, query_id: u64) -> Vec<Task> {
        let frontier = self.table.get(&query_id).unwrap().read().await.get_frontier();
        frontier
    }

    pub async fn add_query(&self, graph: QueryGraph) -> Vec<Task> {
        let frontier = graph.get_frontier();
        let query_id = self.next_query_id();
        self.table.insert(query_id, RwLock::new(graph));
        frontier
    }

    pub async fn get_query_status(&self, query_id: u64) -> Option<QueryStatus> {
        if let Some(query) = self.table.get(&query_id) {
            let status = query.read().await.get_query_status();
            Some(status)
        } else {
            None
        }
    }

    pub async fn update_stage_status(
        &self,
        query_id: u64,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        if let Some(graph) = self.table.get(&query_id) {
            graph
                .write()
                .await
                .update_stage_status(stage_id, status)?;
            Ok(())
        } else {
            Err("Graph not found.")
        }
    }

    pub async fn delete_query(&self, query_id: u64) {
        todo!()
    }

    pub async fn get_plan_bytes(
        &self,
        query_id: u64,
        stage_id: u64,
    ) -> Result<Vec<u8>, SchedulerError> {
        if let Some(graph) = self.table.get(&query_id) {
            let plan = Arc::clone(&graph.read().await.stages[stage_id as usize].plan);
            Ok(physical_plan_to_bytes(plan)
                .expect("Failed to serialize physical plan")
                .to_vec())
        } else {
            Err(SchedulerError::Error("Graph not found.".to_string()))
        }
    }
}
