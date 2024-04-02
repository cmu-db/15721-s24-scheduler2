use crate::parser::serialize_physical_plan;
use crate::query_graph::{QueryGraph, StageStatus};
use crate::task::Task;
use crate::SchedulerError;
use futures::executor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct QueryTable {
    // Maps query IDs to query graphs
    table: RwLock<HashMap<u64, RwLock<QueryGraph>>>,
}

impl QueryTable {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_frontier(&self, query_id: u64) -> Vec<Task> {
        let x = self
            .table
            .write()
            .await
            .get(&query_id)
            .unwrap()
            .read()
            .await
            .get_frontier();
        x
    }

    #[must_use]
    pub async fn add_query(&self, graph: QueryGraph) -> Vec<Task> {
        let mut t = self.table.write().await;
        let frontier = graph.get_frontier();
        (*t).insert(graph.query_id, RwLock::new(graph));
        frontier
    }

    #[must_use]
    pub async fn update_stage_status(
        &self,
        query_id: u64,
        stage_id: u64,
        status: StageStatus,
    ) -> Result<(), &'static str> {
        let t = self.table.read().await;
        if let Some(graph) = t.get(&query_id) {
            graph
                .write()
                .await
                .update_stage_status(stage_id, status)
                .await?;
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
        let t = self.table.read().await;
        if let Some(graph) = t.get(&query_id) {
            let plan = Arc::clone(&graph.read().await.stages[stage_id as usize].plan);
            match serialize_physical_plan(plan).await {
                Ok(p) => Ok(p),
                Err(e) => Err(SchedulerError::DfError(e)),
            }
        } else {
            Err(SchedulerError::Error("Graph not found.".to_string()))
        }
    }

    pub async fn cancel_query(&mut self, query_id: u64) -> bool {
        true
    }
}
