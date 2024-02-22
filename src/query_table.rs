use crate::query_graph::{QueryGraph, StageStatus};
use tokio::sync::{RwLock, Mutex};
// use std::cell::RefCell;
use std::collections::HashMap;
use futures::executor;
use crate::scheduler::Task;
use std::borrow::BorrowMut;

pub struct QueryTable {
  // Maps query IDs to query graphs
  table: tokio::sync::RwLock<HashMap<u64, Mutex<QueryGraph>>>,
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
      (*t).insert(graph.query_id, Mutex::new(graph));
      frontier
    })
  }

  #[must_use]
  pub async fn update_stage_status(&mut self, query_id: u64, stage_id: u64, status: StageStatus) 
  -> Result<(), &'static str> {
    let t = self.table.read().await;
    if let Some(mut graph) = t.get(&query_id) {
      graph.borrow_mut().lock().await.update_stage_status(stage_id, status)?;
      Ok(())
    } else {
      Err("Graph not found.")
    }
  }

  pub async fn cancel_query(&mut self, query_id: u64) -> bool {

    true
  }
}