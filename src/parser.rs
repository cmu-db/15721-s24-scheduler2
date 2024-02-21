use datafusion_proto::bytes::{physical_plan_from_bytes,physical_plan_to_bytes};
use datafusion::execution::context::SessionContext;
use prost::Message;
use std::io::{self, Cursor};
use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::error::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::Int32Array;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::PhysicalPlanner;

async fn deserialize_physical_plan(bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
    let ctx = SessionContext::new();
    physical_plan_from_bytes(bytes, &ctx)
}
