use std::collections::HashMap;
use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use tokio::sync::Mutex as TokioMutex;
use once_cell::sync::Lazy;
use crate::composable_database::TaskId;

static INTERMEDIATE_RESULTS: Lazy<Arc<TokioMutex<HashMap<TaskId, Vec<RecordBatch>>>>> = Lazy::new(|| {
    Arc::new(TokioMutex::new(HashMap::new()))
});
