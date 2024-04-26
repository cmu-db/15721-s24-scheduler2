use crate::query_graph::QueryGraph;
use crate::server::composable_database::QueryStatus;
use crate::SchedulerError;
use datafusion_proto::bytes::physical_plan_to_bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct QueryTable {
    // Maps query IDs to query graphs
    pub table: RwLock<HashMap<u64, Arc<RwLock<QueryGraph>>>>,
}

impl QueryTable {
    pub async fn new() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_query(&self, graph: QueryGraph) -> Arc<RwLock<QueryGraph>> {
        println!("scheduler: adding query graph: {:#?}", graph);
        let qid = graph.query_id;
        let graph_lock = Arc::new(RwLock::new(graph));
        let mut t = self.table.write().await;
        (*t).insert(qid, Arc::clone(&graph_lock));
        graph_lock
    }

    pub async fn get_query_status(&self, query_id: u64) -> QueryStatus {
        let t = self.table.read().await;
        todo!()
    }

    pub async fn get_plan_bytes(
        &self,
        query_id: u64,
        stage_id: u64,
    ) -> Result<Vec<u8>, SchedulerError> {
        let t = self.table.read().await;
        if let Some(graph) = t.get(&query_id) {
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
