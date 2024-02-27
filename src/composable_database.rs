#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskId {
    #[prost(uint64, tag = "1")]
    pub query_id: u64,
    #[prost(uint64, tag = "2")]
    pub task: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskDone {
    #[prost(uint64, tag = "1")]
    pub result_size: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskStatusIncomplete {
    #[prost(enumeration = "task_status_incomplete::StatusType", tag = "1")]
    pub status_type: i32,
}
/// Nested message and enum types in `TaskStatusIncomplete`.
pub mod task_status_incomplete {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum StatusType {
        Failed = 0,
        Prefetch = 1,
    }
    impl StatusType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                StatusType::Failed => "FAILED",
                StatusType::Prefetch => "PREFETCH",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "FAILED" => Some(Self::Failed),
                "PREFETCH" => Some(Self::Prefetch),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyTaskStateArgs {
    #[prost(uint64, tag = "1")]
    pub task_id: u64,
    #[prost(oneof = "notify_task_state_args::State", tags = "2, 3")]
    pub state: ::core::option::Option<notify_task_state_args::State>,
}
/// Nested message and enum types in `NotifyTaskStateArgs`.
pub mod notify_task_state_args {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum State {
        #[prost(message, tag = "2")]
        Result(super::TaskDone),
        #[prost(message, tag = "3")]
        Code(super::TaskStatusIncomplete),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewTaskPlan {
    #[prost(bytes = "vec", tag = "1")]
    pub physical_plan: ::prost::alloc::vec::Vec<u8>,
    #[prost(oneof = "new_task_plan::ExchangeInstruction", tags = "2, 3")]
    pub exchange_instruction: ::core::option::Option<new_task_plan::ExchangeInstruction>,
}
/// Nested message and enum types in `NewTaskPlan`.
pub mod new_task_plan {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExchangeInstruction {
        /// Definitions for scatter, gather, discard results
        #[prost(int32, tag = "2")]
        Scatter(i32),
        #[prost(string, tag = "3")]
        Gather(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotifyTaskStateRet {
    #[prost(oneof = "notify_task_state_ret::Response", tags = "1")]
    pub response: ::core::option::Option<notify_task_state_ret::Response>,
}
/// Nested message and enum types in `NotifyTaskStateRet`.
pub mod notify_task_state_ret {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        /// Wait or other instructions
        #[prost(message, tag = "1")]
        Plan(super::NewTaskPlan),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueryInfo {
    #[prost(int32, tag = "1")]
    pub priority: i32,
    /// Additional fields here
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
