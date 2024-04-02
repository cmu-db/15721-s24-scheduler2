/// A task is identified by its query id and task id
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskId {
    #[prost(uint64, tag = "1")]
    pub query_id: u64,
    #[prost(uint64, tag = "2")]
    pub task_id: u64,
    #[prost(uint64, tag = "3")]
    pub stage_id: u64,
}
/// Task execution and status messages
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyTaskStateArgs {
    #[prost(message, optional, tag = "1")]
    pub task: ::core::option::Option<TaskId>,
    /// if the query succeeded
    #[prost(bool, tag = "2")]
    pub success: bool,
    /// bytes for the result
    #[prost(bytes = "vec", tag = "3")]
    pub result: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyTaskStateRet {
    /// true if there is a new task (details given below)
    #[prost(bool, tag = "1")]
    pub has_new_task: bool,
    #[prost(message, optional, tag = "2")]
    pub task: ::core::option::Option<TaskId>,
    #[prost(bytes = "vec", tag = "3")]
    pub physical_plan: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryInfo {
    #[prost(int32, tag = "1")]
    pub priority: i32,
    #[prost(int32, tag = "2")]
    pub cost: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScheduleQueryArgs {
    #[prost(bytes = "vec", tag = "1")]
    pub physical_plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub metadata: ::core::option::Option<QueryInfo>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScheduleQueryRet {
    #[prost(uint64, tag = "1")]
    pub query_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryJobStatusArgs {
    #[prost(uint64, tag = "1")]
    pub query_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryJobStatusRet {
    #[prost(enumeration = "QueryStatus", tag = "1")]
    pub query_status: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbortQueryArgs {
    #[prost(uint64, tag = "1")]
    pub query_id: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AbortQueryRet {
    #[prost(bool, tag = "1")]
    pub aborted: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum QueryStatus {
    Done = 0,
    InProgress = 1,
    Failed = 2,
    NotFound = 3,
}
impl QueryStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            QueryStatus::Done => "DONE",
            QueryStatus::InProgress => "IN_PROGRESS",
            QueryStatus::Failed => "FAILED",
            QueryStatus::NotFound => "NOT_FOUND",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "DONE" => Some(Self::Done),
            "IN_PROGRESS" => Some(Self::InProgress),
            "FAILED" => Some(Self::Failed),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}
