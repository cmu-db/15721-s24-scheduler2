use crate::executor::Executor;
use crate::mock_frontend::MockFrontend;
use crate::project_config::Config;
use crate::project_config::{load_catalog, read_config};
use crate::server::composable_database::scheduler_api_server::SchedulerApiServer;
use crate::server::composable_database::TaskId;
use crate::server::SchedulerService;
use datafusion::arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, Expr};
use datafusion::prelude::SessionContext;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::time::Instant;
use tonic::transport::Server;

/**
This gRPC facilitates communication between executors and the scheduler:
rpc NotifyTaskState(NotifyTaskStateArgs) returns (NotifyTaskStateRet);

Executor to scheduler message:
message NotifyTaskStateArgs {
    TaskID task = 1; // Task identifier
    bool success = 2; // Indicates if the task was executed successfully
    bytes result = 3; // Result data as bytes
}
- Notifies the scheduler of the task's execution xwstatus using the associated TaskID and transmits the result data.

Scheduler to executor message:
message NotifyTaskStateRet {
    bool has_new_task = 1; // Indicates the presence of a new task
    TaskID task = 2; // New task identifier
    bytes physical_plan = 3; // Task execution plan
}
- Enables the scheduler to assign new tasks to the executor.

Establishing connection:
During integration tests, executors utilize NotifyTaskStateArgs to establish initial communication with the scheduler. The initial message contains the HANDSHAKE_TASK_ID. Upon receipt, the scheduler begins assigning tasks using NotifyTaskStateRet messages.
 */

lazy_static! {
    static ref HANDSHAKE_QUERY_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_TASK_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_STAGE_ID: u64 = -1i64 as u64;
    static ref HANDSHAKE_TASK: TaskId = TaskId {
        query_id: *HANDSHAKE_QUERY_ID,
        stage_id: *HANDSHAKE_STAGE_ID,
        task_id: *HANDSHAKE_TASK_ID,
    };
}

pub struct IntegrationTest {
    catalog_path: String,
    config_path: String,
    ctx: Arc<SessionContext>,
    config: Config,
}

/**
This integration test uses hardcoded addresses for executors and the scheduler,
specified in a config file located in the project root directory. In production
systems, these addresses would typically be retrieved from a catalog. This section'
is responsible for parsing the config file.*/

impl IntegrationTest {
    // Given the paths to the catalog (containing all db files) and a path to the config file,
    // create a new instance of IntegrationTester
    pub async fn new(catalog_path: String, config_path: String) -> Self {
        let ctx = load_catalog(&catalog_path).await;
        let config = read_config(&config_path);
        Self {
            ctx,
            config,
            catalog_path,
            config_path,
        }
    }

    pub async fn run_server(&self) {
        let scheduler_addr = format!(
            "{}:{}",
            self.config.scheduler.id_addr, self.config.scheduler.port
        );
        let catalog_path = self.catalog_path.clone();
        tokio::spawn(async move {
            // Starts the scheduler gRPC service
            let addr = scheduler_addr.parse().expect("Invalid address");
            println!("Scheduler listening on {}", addr);

            let scheduler_service = SchedulerService::new(&catalog_path).await;
            Server::builder()
                .add_service(SchedulerApiServer::new(scheduler_service))
                .serve(addr)
                .await
                .expect("unable to start scheduler gRPC server");
        });
    }
    pub async fn run_client(&self) {
        let start = Instant::now(); // Start timing
        let scheduler_addr = format!(
            "{}:{}",
            self.config.scheduler.id_addr, self.config.scheduler.port
        );
        // Start executor clients
        for executor in &self.config.executors {
            // Clone the scheduler_addr for each executor client
            let mut mock_executor = Executor::new("./test_files/", executor.id).await;
            let scheduler_addr_copy = scheduler_addr.clone();
            tokio::spawn(async move {
                mock_executor.connect(&scheduler_addr_copy).await;
            });
        }
        let end = Instant::now();
        let duration = end.duration_since(start);
        println!("Time elapsed: {:?}", duration);
    }

    pub async fn run_frontend(&self) -> MockFrontend {
        let scheduler_addr = format!(
            "{}:{}",
            self.config.scheduler.id_addr, self.config.scheduler.port
        );
        let catalog_path = self.catalog_path.clone();

        let frontend =
            tokio::spawn(async move { MockFrontend::new(&catalog_path, &scheduler_addr).await })
                .await
                .expect("Failed to join the async task");

        frontend
    }

    async fn sort_batch_by_all_columns(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DataFusionError> {
        let df = self.ctx.read_batch(batch)?;

        // Get the list of column names from the schema
        let column_names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        // Create a vector of sort expressions based on the column names
        let sort_exprs: Vec<Expr> = column_names
            .iter()
            .map(|name| col(name).sort(true, true))
            .collect();

        // Sort the DataFrame by the generated expressions
        let sorted_df = df.sort(sort_exprs)?.collect().await?;
        assert_eq!(1, sorted_df.len());

        Ok(sorted_df[0].clone())
    }

    // Compares if two result sets are equal
    // Two record batches are equal if they have the same set of elements, and the ordering does
    // not matter
    async fn is_batch_equal(
        &self,
        res1: RecordBatch,
        res2: RecordBatch,
    ) -> Result<bool, DataFusionError> {
        if res1.schema() != res2.schema() {
            return Ok(false);
        }

        if res1.num_rows() != res2.num_rows() || res1.num_columns() != res2.num_columns() {
            return Ok(false);
        }

        // sort each row by a random column to see if they are equal
        let sorted_batch1 = self.sort_batch_by_all_columns(res1).await?;
        let sorted_batch2 = self.sort_batch_by_all_columns(res2).await?;

        Ok(sorted_batch1 == sorted_batch2)
    }
    pub async fn results_eq(
        &self,
        res1: &Vec<RecordBatch>,
        res2: &Vec<RecordBatch>,
    ) -> Result<bool, DataFusionError> {
        if res1.len() != res2.len() {
            return Ok(false);
        }

        for (batch1, batch2) in res1.iter().zip(res2.iter()) {
            let are_equal = self.is_batch_equal(batch1.clone(), batch2.clone()).await?;
            if !are_equal {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::Executor;
    use crate::integration_test::IntegrationTest;
    use crate::parser::ExecutionPlanParser;
    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    async fn initialize_integration_test() -> IntegrationTest {
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");
        let config_path = concat!(env!("CARGO_MANIFEST_DIR"), "/executors.toml");
        IntegrationTest::new(catalog_path.to_string(), config_path.to_string()).await
    }

    #[tokio::test]
    async fn test_results_eq_basic() {
        let t = initialize_integration_test().await;

        // Handcraft a record batch
        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();

        // Construct another equivalent record batch
        let id_array_2 = Int32Array::from(vec![1, 5, 3, 2, 4]);
        let schema_2 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch_2 = RecordBatch::try_new(Arc::new(schema_2), vec![Arc::new(id_array_2)]).unwrap();

        // Construct a third record batch that does not equal
        let id_array_3 = Int32Array::from(vec![1, 6, 3, 2, 4]);
        let schema_3 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let batch_3 = RecordBatch::try_new(Arc::new(schema_3), vec![Arc::new(id_array_3)]).unwrap();

        assert_eq!(
            true,
            t.results_eq(&vec![batch.clone()], &vec![batch_2.clone()])
                .await
                .unwrap()
        );
        assert_eq!(
            false,
            t.results_eq(&vec![batch_2.clone()], &vec![batch_3.clone()])
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_results_eq() {
        let t = initialize_integration_test().await;

        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");

        let parser = ExecutionPlanParser::new(catalog_path).await;

        // paths to two sql queries
        let sql_1_vec = parser
            .read_sql_from_file(concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/1.sql"))
            .await
            .expect("fail to read query 1");
        assert_eq!(1, sql_1_vec.len());

        let sql_2_vec = parser
            .read_sql_from_file(concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/2.sql"))
            .await
            .expect("fail to read query 2");
        assert_eq!(1, sql_2_vec.len());

        let executor1 = Executor::new(catalog_path, 0).await;
        let executor2 = Executor::new(catalog_path, 1).await;

        // Executors 1 and 2 execute TPC Q1
        let res1 = executor1
            .execute_sql(sql_1_vec.get(0).unwrap().as_str())
            .await
            .expect("failed to execute query 1");
        let res2 = executor2
            .execute_sql(sql_1_vec.get(0).unwrap().as_str())
            .await
            .expect("failed to execute query 1");

        // Executors 1 and 2 execute TPC Q2
        let res3 = executor1
            .execute_sql(sql_2_vec.get(0).unwrap().as_str())
            .await
            .expect("failed to execute query 2");
        let res4 = executor2
            .execute_sql(sql_2_vec.get(0).unwrap().as_str())
            .await
            .expect("failed to execute query 2");

        // different executors, same query: results should be equal
        assert!(t.results_eq(&res1, &res2).await.unwrap());
        assert!(t.results_eq(&res3, &res4).await.unwrap());

        // different query: results should not be equal
        assert!(!t.results_eq(&res1, &res3).await.unwrap());
        assert!(!t.results_eq(&res2, &res4).await.unwrap());
    }

    // #[tokio::test]
    // async fn test_is_result_correct_sql() {
    //     let executor = DatafusionExecutor::new("./test_files/");
    //
    //     let query = "SELECT * FROM mock_executor_test_table";
    //     let res_with_sql = executor
    //         .execute_query(query)
    //         .await
    //         .expect("fail to execute sql");
    //
    //     let plan_result = executor.get_session_context().sql(&query).await;
    //     let plan = match plan_result {
    //         Ok(plan) => plan,
    //         Err(e) => {
    //             panic!("Failed to create plan: {:?}", e);
    //         }
    //     };
    //
    //     let plan: Arc<dyn ExecutionPlan> = match plan.create_physical_plan().await {
    //         Ok(plan) => plan,
    //         Err(e) => {
    //             panic!("Failed to create physical plan: {:?}", e);
    //         }
    //     };
    //
    //     let result = executor.execute_plan(plan).await;
    //     assert!(result.is_ok());
    //     let batches = result.unwrap();
    //
    //     assert!(!batches.is_empty());
    //
    //     assert_eq!(true, is_result_correct(res_with_sql, batches));
    // }

    // #[tokio::test]
    // async fn test_handshake() {
    //     let config = read_config();
    //     let scheduler_addr = format!("{}:{}", config.scheduler.id_addr, config.scheduler.port);
    //
    //     let scheduler_addr_for_server = scheduler_addr.clone();
    //     tokio::spawn(async move {
    //         start_scheduler_server(&scheduler_addr_for_server).await;
    //     });
    //
    //     // Start executor clients
    //     for executor in config.executors {
    //         // Clone the scheduler_addr for each executor client
    //         let scheduler_addr_for_client = scheduler_addr.clone();
    //         tokio::spawn(async move {
    //             start_executor_client(executor, &scheduler_addr_for_client).await;
    //         });
    //     }
    // }

    // #[tokio::test]
    // async fn test_generate_refsol() {
    //     let res = generate_refsol("./test_files/test_select.sql").await;
    //     assert_eq!(false, res.is_empty());
    //     eprintln!("Number of arguments is {}", res.len());
    // }
}
