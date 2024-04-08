use crate::project_config::load_catalog;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
use std::sync::Arc;
use std::{fmt};
use futures::TryFutureExt;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Default)]
pub struct ExecutionPlanParser {
    ctx: Arc<SessionContext>,
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

    pub async fn deserialize_physical_plan(
        &self,
        bytes: Vec<u8>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        physical_plan_from_bytes(bytes.as_slice(), &self.ctx)
    }

    pub async fn serialize_physical_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
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
        let ast = Parser::parse_sql(&dialect, &contents)?;

        let mut statements = Vec::new();
        for stmt in ast {
            statements.push(stmt.to_string());
        }

        Ok(statements)
    }

    pub async fn get_execution_plan_from_file(&self, path: &str) ->  Result<Vec<Arc<dyn ExecutionPlan>>> {
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
        // self.ctx.sql(query).await?.create_physical_plan().await
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
}


#[cfg(test)]
mod tests {
    use crate::parser::ExecutionPlanParser;

    #[tokio::test]
    async fn test_get_execution_plans_from_files() {
        let dir_path = "./test_files";
        eprintln!("Parsing test files in directory: {}", dir_path);
        let parser = ExecutionPlanParser::new(dir_path).await;

        let entries = parser.list_all_slt_files(dir_path);
        // Check if there are any .slt files to process.
        if entries.is_empty() {
            eprintln!("No .sql files found in directory: {}", dir_path);
            return;
        }

        let mut total_execution_plans = 0; // Counter for the total number of execution plans.
        let mut files_scanned = 0; // Counter for the number of files scanned.

        for file_path in entries {
            let file_path_str = file_path
                .to_str()
                .expect("Failed to convert path to string");
            eprintln!("Processing test file: {}", file_path_str);

            match parser.get_execution_plan_from_file(file_path_str).await {
                Ok(plans) => {
                    total_execution_plans += plans.len();
                }
                Err(e) => {
                    eprintln!(
                        "Failed to get execution plans from file {}: {}",
                        file_path_str, e
                    );
                    panic!("Test failed due to error with file: {}", file_path_str);
                }
            }

            files_scanned += 1;
        }

        // Print out the total counts.
        eprintln!(
            "Total number of execution plans generated: {}",
            total_execution_plans
        );
        eprintln!("Total number of files scanned: {}", files_scanned);
    }

    #[tokio::test]
    async fn test_serialize_deserialize_physical_plan() {
        let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files");
        let parser = ExecutionPlanParser::new(catalog_path).await;

        let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files", "/expr.slt");
        let res = parser.get_execution_plan_from_file(&test_file).await;
        assert!(res.is_ok());
        let plans = res.unwrap();
        for plan in &plans {
            eprintln!("Trying to serialize plan {:?}", plan.clone());
            let serialization_result = parser.serialize_physical_plan(plan.clone()).await;
            assert!(serialization_result.is_ok());
        }
    }
}
