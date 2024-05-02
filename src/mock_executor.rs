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

use crate::mock_catalog::load_catalog;
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
    ) -> datafusion::common::Result<Vec<Vec<RecordBatch>>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();

        let mut results = vec![];
        let num_partitions = plan.properties().output_partitioning().partition_count();
        for i in 0..num_partitions {
            let mut partition_results = Vec::new();
            let mut stream = plan.execute(i, task_ctx.clone())?;
            while let Some(batch) = stream.next().await {
                partition_results.push(batch?);
            }
            results.push(partition_results);
        }
        Ok(results)
    }
}
