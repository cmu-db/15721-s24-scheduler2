
pub struct QueryTask {
    id: u64,
    name: String,
    query: String,

    inputs: Vec<u64>,
}

pub struct QueryGraph {
    plan: Vec<u8>,
    tasks: Vec<QueryTask>,
}
