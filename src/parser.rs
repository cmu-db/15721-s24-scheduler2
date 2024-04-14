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

    pub fn deserialize_physical_plan(
        &self,
        bytes: Vec<u8>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
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

    // TODO: run the sql directly to see what happens
}


#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::panic;
    use datafusion::physical_plan::displayable;
    use datafusion_proto::bytes::physical_plan_to_bytes;
    use crate::parser::ExecutionPlanParser;

    use tokio::runtime::{Builder, Runtime};

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
            let catalog_path = concat!(env!("CARGO_MANIFEST_DIR"), "/test_data");
            let parser = ExecutionPlanParser::new(catalog_path).await;

            let test_file = concat!(env!("CARGO_MANIFEST_DIR"), "/test_sql", "/4.sql");
            let res = parser.get_execution_plan_from_file(&test_file).await;

            assert!(res.is_ok());
            let plans = res.unwrap();
            for plan in plans {
                let serialization_result = parser.serialize_physical_plan(plan.clone());
                assert!(serialization_result.is_ok());

                let original_plan = parser.deserialize_physical_plan(serialization_result.unwrap());
                assert!(original_plan.is_ok());

            }
        });
    }

}
