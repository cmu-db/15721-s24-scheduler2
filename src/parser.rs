//! # Execution Plan Parser
//!
//! This module provides the core functionalities for parsing, serializing, and deserializing execution plans
//! based on DataFusion and Apache Arrow. It is designed to manage the lifecycle of ExecutionPlans
//! through serialization for distributed execution.
//!
//! ## Features
//!
//! - **Plan Serialization and Deserialization**: Facilitates the serialization of physical execution plans to bytes
//!   and deserialization from bytes, enabling distributed processing and storage.
//! - **Execution Result Serialization and Deserialization: Serializes/deserializes RecordBatches
//! (output format for executors) for sending results across components
//!
//! ## Example
//!
//! Here's how you might use the `ExecutionPlanParser` to process a SQL file and serialize the resulting execution plan:
//!
//! ```rust
//! let parser = ExecutionPlanParser::new("path/to/catalog").await;
//! let sql_statements = parser.read_sql_from_file("path/to/sqlfile.sql").await.unwrap();
//! let plans = parser.get_execution_plan_from_file("path/to/another_sqlfile.sql").await.unwrap();
//! let serialized_plan = parser.serialize_physical_plan(plans[0].clone()).unwrap();
//! let deserialized_plan = parser.deserialize_physical_plan(serialized_plan).unwrap();
//! ```

use crate::mock_catalog::load_catalog;
use datafusion::{
    arrow::{
        array::RecordBatch,
        ipc::{reader::FileReader, writer::FileWriter},
    },
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use sqlparser::{dialect::GenericDialect, parser::Parser};
use std::{fmt, io::Cursor, sync::Arc};
use tokio::{fs::File, io::AsyncReadExt};

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

    pub fn serialize_record_batches(batches: Vec<RecordBatch>) -> Result<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::new();

        // Assuming all batches share the same schema for simplicity.
        if batches.is_empty() {
            DataFusionError::Internal("serialize_record_batches: empty batches".to_string());
        }
        let schema = batches[0].schema();

        {
            let mut writer = FileWriter::try_new(&mut buffer, &schema)?;

            for batch in batches {
                assert_eq!(batch.schema(), schema);
                writer.write(&batch)?;
            }

            writer.finish()?;
        }

        Ok(buffer)
    }

    /// Deserializes a `Vec<u8>` into a vector of `RecordBatch`.
    pub fn deserialize_record_batches(bytes: Vec<u8>) -> Result<Vec<RecordBatch>> {
        let cursor = Cursor::new(bytes);
        let mut reader = FileReader::try_new(cursor, None)?;
        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            let batch = batch.expect("deserialize_record_batches: error reading batch");
            batches.push(batch);
        }

        if batches.is_empty() {
            panic!("deserialize_record_batches: no batches are found");
        } else {
            Ok(batches)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mock_executor::MockExecutor;
    use crate::parser::ExecutionPlanParser;
    use datafusion::physical_plan::displayable;
    use std::fmt::Debug;
    use tokio::runtime::Builder;

    const CATALOG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");

    #[tokio::test]
    async fn test_serialize_deserialize_physical_plan() {
        // Define the base path for the catalog and test SQL files
        let tests_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql");

        // Create an ExecutionPlanParser instance
        let parser = ExecutionPlanParser::new(CATALOG_PATH).await;

        let paths = std::fs::read_dir(tests_path).unwrap();

        for path in paths {
            let path = path.unwrap().path();
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
                    let original_plan_formatted = format!(
                        "{}",
                        displayable(original_plan.expect("").as_ref()).indent(false)
                    );
                    assert_eq!(plan_formatted, original_plan_formatted);
                }
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
        const CORRECT_SQL: &str = r"SELECT * FROM customer";
        assert_eq!(
            CORRECT_SQL,
            sql_vec.get(0).expect("fail to get test select statement")
        );
    }

    #[tokio::test]
    async fn read_sql_from_file_multiple_statement() {
        let parser = ExecutionPlanParser::new(CATALOG_PATH).await;
        println!("{:?}", parser);
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
        const CORRECT_SQL_1: &str = r"SELECT * FROM customer";
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

    #[tokio::test]
    async fn test_serialize_deserialize_record_batch() {
        let parser = ExecutionPlanParser::new(CATALOG_PATH).await;
        let mock_executor = MockExecutor::new(CATALOG_PATH).await;

        let plans = parser
            .get_execution_plan_from_file("./test_sql/16.sql")
            .await
            .expect("bad plan");
        assert_eq!(plans.len(), 1);

        let record_batches_original = mock_executor
            .execute(plans.get(0).expect("empty plan").clone())
            .await
            .expect("error executing sql");

        // let record_batches_bytes =
        //     ExecutionPlanParser::serialize_record_batches(record_batches_original.clone())
        //         .expect("fail to serialize record batches");
        // let record_batches_roundtrip =
        //     ExecutionPlanParser::deserialize_record_batches(record_batches_bytes)
        //         .expect("fail to deserialize record batches");

        // for (i, batch) in record_batches_roundtrip.iter().enumerate() {
        //     assert_eq!(record_batches_original[i], batch.clone());
        // }
    }
}
