use std::usize;
use datafusion_proto::protobuf;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreviousStage {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(uint64, tag = "2")]
    pub caller: u64,
    #[prost(string, repeated, tag = "3")]
    pub actors: Vec<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContext {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(message, optional, tag = "2")]
    pub previous_stage: Option<PreviousStage>,
    #[prost(uint64, tag = "3")]
    pub current: u64,
    #[prost(uint64, tag = "4")]
    pub delegate: u64,
    #[prost(string, repeated, tag = "5")]
    pub actors: Vec<String>,
    #[prost(message, optional, tag = "6")]
    pub partitioning: Option<protobuf::Partitioning>
}
