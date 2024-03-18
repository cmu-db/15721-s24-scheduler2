use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use std::sync::Arc;
use datafusion::execution::context::TaskContext;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::prelude::CsvReadOptions;
use futures::stream::StreamExt;


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

    // Function to execute a query from a SQL string
    pub async fn execute_query(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await;
        return df.collect();
    }

    // Function to execute a query from an ExecutionPlan
    pub async fn execute_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();        
        let mut batches = Vec::new();

        match plan.execute(1, task_ctx).await {
            Ok(mut stream) => {
                // Iterate over the stream
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(record_batch) => {
                           batches.push(record_batch);
                        },
                        Err(e) => {
                            eprintln!("Error processing batch: {}", e);
                            return Err(e);
                        }
                    }
                }
            },
            Err(e) => eprintln!("Failed to execute plan: {}", e),
        }
        Ok(batches)
    }
}
