use async_trait::async_trait;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::parser::ParserError;
use sqllogictest::{Record, TestError};
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use thiserror::Error;

use log::info;
use sqllogictest::{ColumnType, DBOutput};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DFColumnType {
    Boolean,
    DateTime,
    Integer,
    Float,
    Text,
    Timestamp,
    Another,
}

impl ColumnType for DFColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Boolean),
            'D' => Some(Self::DateTime),
            'I' => Some(Self::Integer),
            'P' => Some(Self::Timestamp),
            'R' => Some(Self::Float),
            'T' => Some(Self::Text),
            _ => Some(Self::Another),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Boolean => 'B',
            Self::DateTime => 'D',
            Self::Integer => 'I',
            Self::Timestamp => 'P',
            Self::Float => 'R',
            Self::Text => 'T',
            Self::Another => '?',
        }
    }
}

pub(crate) type DFOutput = DBOutput<DFColumnType>;

pub type Result<T, E = DFSqlLogicTestError> = std::result::Result<T, E>;

/// DataFusion sql-logicaltest error
#[derive(Debug, Error)]
pub enum DFSqlLogicTestError {
    /// Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] TestError),
    /// Error from datafusion
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),
    /// Error returned when SQL is syntactically incorrect.
    #[error("SQL Parser error: {0}")]
    Sql(#[from] ParserError),
    /// Error from arrow-rs
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    /// Generic error
    #[error("Other Error: {0}")]
    Other(String),
}

impl From<String> for DFSqlLogicTestError {
    fn from(value: String) -> Self {
        DFSqlLogicTestError::Other(value)
    }
}

pub struct IntegrationTestRunner {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl IntegrationTestRunner {
    pub fn new(ctx: SessionContext, relative_path: &str) -> Self {
        Self {
            ctx,
            relative_path: relative_path.into(),
        }
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

async fn get_execution_plan_from_file(
    file_name: &str,
) -> Result<Vec<Arc<dyn ExecutionPlan>>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();

    sqllogictest::Runner::new(|| async {
        Ok(IntegrationTestRunner::new(SessionContext::new(), file_name))
    });

    let sql_statements: Vec<Record<DFColumnType>> =
        sqllogictest::parse_file(file_name).expect("Failed to parse file");

    let mut plans = Vec::new();
    for sql_statement in sql_statements {
        let plan = ctx.sql(&sql_statement.to_string()).await?;
        let plan: Arc<dyn ExecutionPlan> = plan.create_physical_plan().await?;
        plans.push(plan);
    }
    Ok(plans)
}

// #[cfg(test)]
// mod tests {
//
//     // Returns a vector of execution plans inside a .slt file
//
//
//
//     #[test]
//     fn test_get_execution_plan() {
//         get_execution_plan_from_file("./test_files/aggregate.slt")
//     }
//
// }
