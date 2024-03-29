use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::CsvReadOptions;
use futures::stream::StreamExt;
use std::sync::Arc;

pub struct DatafusionExecutor {
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
        let df = match self.ctx.sql(query).await {
            Ok(dataframe) => dataframe,
            Err(e) => {
                eprintln!("Error executing query: {}", e);
                return Err(e);
            }
        };

        match df.collect().await {
            Ok(result) => Ok(result),
            Err(e) => {
                eprintln!("Error collecting results: {}", e);
                Err(e)
            }
        }
    }

    // Function to execute a query from an ExecutionPlan
    pub async fn execute_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = self.ctx.task_ctx();
        let mut batches = Vec::new();

        match plan.execute(0, task_ctx) {
            Ok(mut stream) => {
                // Iterate over the stream
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(record_batch) => {
                            batches.push(record_batch);
                        }
                        Err(e) => {
                            eprintln!("Error processing batch: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Failed to execute plan: {}", e),
        }
        Ok(batches)
    }

    pub fn get_session_context(&self) -> Arc<SessionContext> {
        self.ctx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    // Helper function to create a DatafusionExecutor instance
    async fn create_executor() -> DatafusionExecutor {
        let executor = DatafusionExecutor::new();
        let table_name = "mock_executor_test_table";
        let file_path = "./test_files/mock_executor_test_table.csv"; // Ensure this file exists in the test environment
        let options = CsvReadOptions::new();
        let result = executor.register_csv(table_name, file_path, options).await;
        assert!(result.is_ok());
        executor
    }

    // Test registering a CSV file
    #[tokio::test]
    async fn test_register_csv() {
        let _ = create_executor();
    }

    // Test running a SQL query
    #[tokio::test]
    async fn test_execute_sql_query() {
        let executor = create_executor();

        let query = "SELECT * FROM mock_executor_test_table";
        let result = executor.await.execute_query(query).await;

        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }

    // Test executing a plan
    #[tokio::test]
    async fn test_execute_plan() {
        let executor = create_executor().await;

        let query = "SELECT * FROM mock_executor_test_table";

        let plan_result = executor.get_session_context().sql(&query).await;
        let plan = match plan_result {
            Ok(plan) => plan,
            Err(e) => {
                panic!("Failed to create plan: {:?}", e);
            }
        };

        let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
            Ok(plan) => plan,
            Err(e) => {
                panic!("Failed to create physical plan: {:?}", e);
            }
        };

        let result = executor.execute_plan(plan).await;
        assert!(result.is_ok());
        let batches = result.unwrap();
        assert!(!batches.is_empty()); // Ensure that we get some results
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 2);
    }
}
