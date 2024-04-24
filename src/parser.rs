use crate::project_config::load_catalog;
use datafusion::arrow::array::{RecordBatch, RecordBatchReader};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::reader::{FileReader, StreamReader};
use datafusion::arrow::ipc::writer::{FileWriter, IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use futures::TryFutureExt;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use bytes::Bytes;
use crc32fast::Hasher;

#[derive(Default)]
pub struct ExecutionPlanParser {
    pub ctx: Arc<SessionContext>,
}

impl fmt::Debug for ExecutionPlanParser {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Parser {{ ctx: Arc<SessionContext> }}")
    }
}

impl ExecutionPlanParser {
    pub async fn new(catalog_path: &str) -> Self {
        Self {
            ctx: load_catalog(catalog_path).await,
        }
    }

    pub fn deserialize_physical_plan(&self, bytes: Vec<u8>) -> Result<Arc<dyn ExecutionPlan>> {
        physical_plan_from_bytes(bytes.as_slice(), &self.ctx)
    }

    pub fn serialize_physical_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
        match physical_plan_to_bytes(plan) {
            Ok(plan_bytes) => Ok(Vec::from(plan_bytes)),
            Err(e) => Err(e),
        }
    }

    pub async fn read_sql_from_file(&self, path: &str) -> Result<Vec<String>> {
        let mut file = File::open(path).await?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).await?;

        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, &contents).expect("fail to parse sql");

        let mut statements = Vec::new();
        for stmt in ast {
            statements.push(stmt.to_string());
        }

        Ok(statements)
    }

    pub async fn get_execution_plan_from_file(
        &self,
        path: &str,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let sql_statements = self.read_sql_from_file(path).await?;
        let mut plans = Vec::new();
        for stmt in sql_statements {
            let plan = self.sql_to_physical_plan(&stmt).await?;
            plans.push(plan);
        }
        Ok(plans)
    }

    // Convert a sql string to a physical plan
    pub async fn sql_to_physical_plan(&self, query: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_result = self.ctx.sql(&query).await;
        let plan = match plan_result {
            Ok(plan) => plan,
            Err(e) => {
                eprintln!("sql_to_physical_plan: invalid SQL statement: {}", e);
                return Err(e);
            }
        };
        plan.create_physical_plan().await
    }

    pub fn serialize_record_batch(batch: RecordBatch) -> Result<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::new();
        let schema = batch.schema();
        {
            let mut writer = FileWriter::try_new(&mut buffer, &schema)?;
            writer.write(&batch)?;
            writer.finish()?;
        }

        Ok(buffer)
    }

    pub fn deserialize_record_batch(bytes: Vec<u8>) -> Result<RecordBatch> {
        let cursor = Cursor::new(bytes);

        let mut reader = FileReader::try_new(cursor, None)?;

        let first_batch = reader
            .next()
            .expect("deserialize_record_batch: empty batch")
            .expect("deserialize_record_batch: error reading first batch");

        // Ensure no more batches are present
        match reader.next() {
            Some(_) => {
                panic!("deserialize_record_batch: more than one batch");
            }
            None => Ok(first_batch),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::ExecutionPlanParser;
    use std::fmt::Debug;
    use bytes::Bytes;
    use crc32fast::Hasher;
    use datafusion::physical_plan::displayable;
    use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
    use difference::{Changeset, Difference};
    use tokio::runtime::Builder;

    const CATALOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");

    fn custom_runtime() -> tokio::runtime::Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("my-custom-name")
            .thread_stack_size(5 * 1024 * 1024)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_serialize_deserialize_physical_plan() {
        let runtime = custom_runtime();

        runtime.block_on(async {
            // Define the base path for the catalog and test SQL files
            let tests_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql");

            // Create an ExecutionPlanParser instance
            let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

            let paths = std::fs::read_dir(tests_path).unwrap();

            for path in paths {
                let path = path.unwrap().path();
                eprintln!("path is {:?}", path);
                // Check if the file is a .sql file
                if path.extension().and_then(std::ffi::OsStr::to_str) == Some("sql") {
                    let test_file = path.to_str().unwrap();
                    eprintln!("Testing file {}", test_file);
                    let res = parser.get_execution_plan_from_file(test_file).await;
                    assert!(res.is_ok());

                    let plans = res.unwrap();
                    for plan in plans {
                        let serialization_result = parser.serialize_physical_plan(plan.clone());
                        assert!(serialization_result.is_ok());

                        let original_plan =
                            parser.deserialize_physical_plan(serialization_result.unwrap());
                        assert!(original_plan.is_ok());

                        let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
                        let original_plan_formatted = format!("{}", displayable(original_plan.expect("").as_ref()).indent(false));
                        assert_eq!(plan_formatted, original_plan_formatted);
                    }
                }
            }
        });
    }

    fn display_diff(text1: &str, text2: &str) {
        let changeset = Changeset::new(text1, text2, "\n");

        for diff in changeset.diffs {
            match diff {
                Difference::Same(ref x) => println!(" {}", x),
                Difference::Add(ref x) => println!("+{}", x),
                Difference::Rem(ref x) => println!("-{}", x),
            }
        }
    }


    #[tokio::test]
    async fn read_sql_from_file() {
        let parser = ExecutionPlanParser::new(CATALOG_PATH).await;
        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/test_select.sql");
        let sql_vec = parser
            .read_sql_from_file(test_file)
            .await
            .expect("fail to read test select statement");
        assert_eq!(1, sql_vec.len());
        const CORRECT_SQL: &str = r"SELECT * FROM mock_executor_test_table";
        assert_eq!(
            CORRECT_SQL,
            sql_vec.get(0).expect("fail to get test select statement")
        );
    }

    #[tokio::test]
    async fn read_sql_from_file_multiple_statement() {
        let parser = ExecutionPlanParser::new(CATALOG_PATH).await;
        let test_file = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test_sql",
            "/test_select_multiple.sql"
        );
        let sql_vec = parser
            .read_sql_from_file(test_file)
            .await
            .expect("fail to read test select statement");
        assert_eq!(2, sql_vec.len());
        const CORRECT_SQL_1: &str = r"SELECT * FROM mock_executor_test_table";
        assert_eq!(
            CORRECT_SQL_1,
            sql_vec.get(0).expect("fail to get test select statement")
        );
        const CORRECT_SQL_2: &str = r"SELECT * FROM customer LIMIT 2";
        assert_eq!(
            CORRECT_SQL_2,
            sql_vec.get(1).expect("fail to get test select statement")
        );
    }

    fn compute_crc32_checksum(data: &Bytes) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&data);
        hasher.finalize()
    }

    #[tokio::test]
    async fn test_serialize_record_batch() {
        // TODO: write tests for serializing record batches
    }

    #[tokio::test]
    async fn test_deserialize_record_batch() {}
}
