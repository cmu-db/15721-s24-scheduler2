use crate::query_graph::QueryGraph;
use tokio::sync::RwLock;
use std::collections::HashMap;
use futures::executor;
use crate::scheduler::Task;

pub struct QueryTable {
  // Maps query IDs to query graphs
  table: tokio::sync::RwLock<HashMap<u64, QueryGraph>>,
}

impl QueryTable {
  pub fn new() -> Self {
      Self {
        table: RwLock::new(HashMap::new()),
      }
  }
  
  #[must_use]
  pub fn add_query(&mut self, mut graph: QueryGraph) -> Vec<Task> {
    executor::block_on(async {
      let mut t = self.table.write().await;
      let frontier = graph.get_frontier();
      (*t).insert(graph.query_id, graph);
      frontier
    })
  }

  pub async fn cancel_query(&mut self, query_id: u64) -> bool {

    true
  }
}