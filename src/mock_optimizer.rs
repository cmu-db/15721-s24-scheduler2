// ======================================================================================
// Module: mock_optimizer.rs
//
// Description:
//     This module implements a mock optimizer that converts a DataFusion LogicalPlan into an
//     ExecutionPlan.
//
//     This optimizer serves as a placeholder. To integrate a custom optimizer (optd), simply
//     replace the implementations of `new` and `optimize` with your own logic.
//
// Example:
//     let optimizer = Optimizer::new("path/to/catalog").await;
//     let execution_plan = optimizer.optimize(&logical_plan).await;
//
// ======================================================================================

use crate::mock_catalog::load_catalog;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub struct Optimizer {
    ctx: SessionContext,
}

impl Optimizer {
    pub async fn new(catalog_path: &str) -> Self {
        let ctx = load_catalog(catalog_path).await;
        Self {
            ctx: (*ctx).clone(),
        }
    }
    pub async fn optimize(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.ctx.state().create_physical_plan(logical_plan).await
    }
}
