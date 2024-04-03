use crate::project_config::load_catalog;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::StreamExt;
use std::sync::Arc;

// Reference executor for verifying correctness
pub struct ReferenceExecutor {
    ctx: Arc<SessionContext>,
}

impl ReferenceExecutor {
    pub async fn new(catalog_path: &str) -> Self {
        Self {
            ctx: load_catalog(catalog_path).await,
        }
    }

    // Function to execute a query from a SQL string
    pub async fn execute_sql_query(&self, query: &str) -> Result<Vec<RecordBatch>> {
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
    pub async fn execute_physical_plan(
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
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     // Test running a SQL query
//     #[tokio::test]
//     async fn test_execute_sql_query() {
//         let executor = ReferenceExecutor::new("./test_files/").await;
//
//         let query = "SELECT * FROM mock_executor_test_table";
//         let result = executor.execute_query(query).await;
//
//         assert!(result.is_ok());
//         let batches = result.unwrap();
//         assert!(!batches.is_empty());
//         assert_eq!(batches[0].num_columns(), 2);
//         assert_eq!(batches[0].num_rows(), 2);
//     }
//
//     // Test executing a plan
//     #[tokio::test]
//     async fn test_execute_plan() {
//         let executor = ReferenceExecutor::new("./test_files/").await;
//
//         let query = "SELECT * FROM mock_executor_test_table";
//
//         let plan_result = executor.get_session_context().sql(&query).await;
//         let plan = match plan_result {
//             Ok(plan) => plan,
//             Err(e) => {
//                 panic!("Failed to create plan: {:?}", e);
//             }
//         };
//
//         let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
//             Ok(plan) => plan,
//             Err(e) => {
//                 panic!("Failed to create physical plan: {:?}", e);
//             }
//         };
//
//         let result = executor.execute_plan(plan).await;
//         assert!(result.is_ok());
//         let batches = result.unwrap();
//         assert!(!batches.is_empty()); // Ensure that we get some results
//         assert_eq!(batches[0].num_columns(), 2);
//         assert_eq!(batches[0].num_rows(), 2);
//     }
// }
