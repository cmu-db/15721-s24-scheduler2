use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
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

    pub async fn register_csv(
        &self,
        table_name: &str,
        file_path: &str,
        options: CsvReadOptions<'_>,
    ) -> Result<()> {
        self.ctx.register_csv(table_name, file_path, options).await
    }

    pub async fn execute_query(&self, query: &str) -> Result<DataFrame> {
        self.ctx.sql(query).await
    }
}

// #[tokio::main]
// async fn main() -> Result<()> {
//     let executor = DatafusionExecutor::new();
//     executor.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
//
//     let query = "SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100";
//     let df = executor.execute_query(query).await?;
//
//     df.show().await?;
//     Ok(())
// }
