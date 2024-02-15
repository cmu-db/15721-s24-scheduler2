use prost::Message;
use serde_json;
use std::io::{self, Cursor};
use substrait::proto::Plan;

pub enum Format {
    Json,
    Binary,
}

pub fn deserialize_plan(data: &[u8], format: Format) -> Result<Plan, Box<dyn std::error::Error>> {
    match format {
        Format::Json => {
            // Deserialize JSON to Plan
            let plan = serde_json::from_slice::<Plan>(data)?;
            Ok(plan)
        }
        Format::Binary => {
            // Deserialize binary to Plan
            let mut cursor = Cursor::new(data);
            let plan = Plan::decode(&mut cursor)?;
            Ok(plan)
        }
    }
}
