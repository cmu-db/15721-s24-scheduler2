use crate::project_config::load_catalog;
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
