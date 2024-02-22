use datafusion::physical_plan::ExecutionPlan;
use datafusion::execution::context::SessionContext;
use std::{path::PathBuf, time::Duration};


pub struct IntegrationTestRunner {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl IntegrationTestRunner {
    pub fn new(ctx: SessionContext, relative_path: PathBuf) -> Self {
        Self { ctx, relative_path }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for IntegrationTestRunner {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        info!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
            sql
        );
        run_query(&self.ctx, sql).await
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "IntegrationTestRunner"
    }

    /// [`DataFusion`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DFOutput> {
    // let df = ctx.sql(sql.into().as_str()).await?;

    // let types = normalize::convert_schema_to_types(df.schema().fields());
    // let results: Vec<RecordBatch> = df.collect().await?;
    // let rows = normalize::convert_batches(results)?;

    // if rows.is_empty() && types.is_empty() {
    //     Ok(DBOutput::StatementComplete(0))
    // } else {
    //     Ok(DBOutput::Rows { types, rows })
    // }
    Ok(DBOutput::StatementComplete(0))
}



// Integration tests: 
// 1. convert SQL -> ExecutionPlan
// 2. set up mock executors (gRPC)
// 3. set up scheduler (gRPC), hardcode mock executors
// 3. for each executionplan: send to scheduler, collect results and compare

#[cfg(test)]
mod tests {
    
    // Returns a vector of execution plans inside a .slt file
    async fn get_execution_plan_from_file(file_name: &str) -> Result<Vec<ExecutionPlan>, Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let mut tester = sqllogictest::Runner::new(IntegrationTestRunner);
        let sql_statments = sqllogictest::parse_file(file_name)
            .expect("Failed to parse file");
        
        let mut plans = Vec::new(); 
        for sql_statement in sql_statements {
            let plan = ctx.sql(&sql_statement).await?;
            let plan = plan.create_physical_plan().await?; 
            plans.push(plan);
        }
        Ok(plans)
    }


    #[test]
    fn test_get_execution_plan() {
        get_execution_plan_from_file("./test_files/aggregate.slt")
    }

}