use datafusion::arrow::array::RecordBatch;
use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;

// Definition of the key used in the map
#[derive(Debug, Clone, Copy)]
pub struct TaskKey {
    pub stage_id: u64,
    pub query_id: u64,
}

impl PartialEq for TaskKey {
    fn eq(&self, other: &Self) -> bool {
        self.stage_id == other.stage_id && self.query_id == other.query_id
    }
}

impl Eq for TaskKey {}

impl Hash for TaskKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stage_id.hash(state);
        self.query_id.hash(state);
    }
}

pub static INTERMEDIATE_RESULTS: Lazy<Arc<Mutex<HashMap<TaskKey, Vec<RecordBatch>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

pub async fn get_results(task_id: &TaskKey) -> Option<Vec<RecordBatch>> {
    let lock = INTERMEDIATE_RESULTS.lock().await;
    lock.get(task_id).cloned()
}

pub async fn insert_results(task_id: TaskKey, results: Vec<RecordBatch>) {
    let mut lock = INTERMEDIATE_RESULTS.lock().await;
    lock.insert(task_id, results);
}

pub async fn append_results(task: &TaskKey, new_results: Vec<RecordBatch>) {
    let mut lock = INTERMEDIATE_RESULTS.lock().await;
    if let Some(results) = lock.get_mut(task) {
        results.extend(new_results);
    } else {
        lock.insert(*task, new_results);
    }
}

pub async fn remove_results(task: &TaskKey) -> Option<Vec<RecordBatch>> {
    let mut lock = INTERMEDIATE_RESULTS.lock().await;
    lock.remove(task)
}

async fn rewrite_node(
    plan: Arc<dyn ExecutionPlan>,
    query_id: u64,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let children = plan.children();
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;

    for child in children.into_iter() {
        let new_child = Box::pin(rewrite_node(child, query_id));
        let new_child = new_child.await?;
        if !Arc::ptr_eq(&new_child, &new_children.last().unwrap_or(&new_child)) {
            changed = true;
        }
        new_children.push(new_child);
    }

    if changed {
        eprintln!("Executor: query rewritten");
        with_new_children_if_necessary(plan, new_children)
    } else {
        Ok(plan)
    }
}

pub async fn rewrite_query(
    plan: Arc<dyn ExecutionPlan>,
    query_id: u64,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let result = Box::pin(rewrite_node(plan, query_id));
    result.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    // Helper function to create a dummy RecordBatch
    fn create_dummy_record_batch() -> Vec<RecordBatch> {
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
        vec![batch]
    }

    #[tokio::test]
    async fn test_insert_and_get_results() {
        let task_key = TaskKey {
            stage_id: 1,
            query_id: 1,
        };
        let results = create_dummy_record_batch();

        insert_results(task_key, results.clone()).await;
        let fetched_results = get_results(&task_key).await;

        assert!(fetched_results.is_some());
        assert_eq!(fetched_results.unwrap(), results);
    }

    #[tokio::test]
    async fn test_append_results() {
        let task_key = TaskKey {
            stage_id: 2,
            query_id: 2,
        };
        let initial_results = create_dummy_record_batch();
        let additional_results = create_dummy_record_batch();

        insert_results(task_key, initial_results.clone()).await;
        append_results(&task_key, additional_results.clone()).await;

        let fetched_results = get_results(&task_key).await.unwrap();
        assert_eq!(fetched_results.len(), 2);
    }

    #[tokio::test]
    async fn test_remove_results() {
        let task_key = TaskKey {
            stage_id: 3,
            query_id: 3,
        };
        let results = create_dummy_record_batch();

        insert_results(task_key, results.clone()).await;
        let removed_results = remove_results(&task_key).await;

        assert!(removed_results.is_some());
        assert_eq!(removed_results.unwrap(), results);

        let fetched_results_after_removal = get_results(&task_key).await;
        assert!(fetched_results_after_removal.is_none());
    }
}
