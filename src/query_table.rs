use crate::server::composable_database::QueryStatus;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::task::Task;
use crate::SchedulerError;
use dashmap::DashMap;
use datafusion_proto::bytes::physical_plan_to_bytes;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct QueryTable {
    // Maps query IDs to query graphs
    pub table: DashMap<u64, RwLock<QueryGraph>>,
}

impl QueryTable {
    pub fn new() -> Self {
        Self {
            table: DashMap::new(),
        }
    }

    pub async fn get_frontier(&self, query_id: u64) -> Vec<Task> {
        let frontier = self.table.get(&query_id).unwrap().read().await.get_frontier();
        frontier
    }

    pub async fn add_query(&self, graph: QueryGraph) -> Vec<Task> {
        println!("scheduler: adding query graph: {:#?}", graph);
        let frontier = graph.get_frontier();
        self.table.insert(graph.query_id, RwLock::new(graph));
        frontier
    }

    pub async fn get_query_status(&self, query_id: u64) -> QueryStatus {
        todo!()
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

    pub async fn remove_query(&self, query_id: u64) {
        todo!()
    }
}
