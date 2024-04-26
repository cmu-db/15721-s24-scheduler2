// ======================================================================================
// Module: mock_executor.rs
//
// Description:
//     This module implements a mock executor using DataFusion. To integrate a custom executor, simply
//     replace the implementations of `new` and `execute` with your own logic.
//
// Example:
//     let executor = MockExecutor::new("path/to/catalog").await;
//     let execution_plan = /* previously generated plan */;
//     let results = executor.execute(Arc::new(execution_plan)).await.expect("Execution failed");
//
// ======================================================================================

use crate::project_config::load_catalog;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use std::sync::Arc;

pub struct MockExecutor {
    ctx: SessionContext,
}

impl MockExecutor {
    pub async fn new(catalog_path: &str) -> Self {
        let ctx = load_catalog(catalog_path).await;
        Self {
            ctx: (*ctx).clone(),
        }
    }
    pub async fn execute(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();
        let mut results = Vec::new();
        let mut stream = plan.execute(0, task_ctx)?;
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }
        assert!(!results.is_empty());
        Ok(results)
    }
}
