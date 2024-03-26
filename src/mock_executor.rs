use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use std::sync::Arc;

struct DatafusionExecutor {
    ctx: Arc<SessionContext>,
}

impl DatafusionExecutor {
    pub fn new() -> Self {
        Self {
            ctx: Arc::new(SessionContext::new()),
        }
    }

    // pub async fn register_csv(
    //     &self,
    //     table_name: &str,
    //     file_path: &str,
    //     options: CsvReadOptions<'_>,
    // ) -> Result<()> {
    //     self.ctx.register_csv(table_name, file_path, options).await
    // }

    // Function to execute a query from a SQL string
    pub async fn execute_query(&self, query: &str) -> Result<DataFrame> {
        self.ctx.sql(query).await
    }

    // Function to execute a query from an ExecutionPlan
    // pub async fn execute_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<DataFrame> {
    //     let execution = self.ctx.collect(plan).await?;
    //     Ok(DataFrame::new(self.ctx.state.clone(), &execution))
    // }
}
