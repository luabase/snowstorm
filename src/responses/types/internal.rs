use crate::responses::types::{chunk::Chunk, query::QueryStatus, row_type::RowType};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Option<Vec<Vec<serde_json::Value>>>,
    pub rowset_base64: Option<String>,
    pub query_id: String,
    pub total: usize,
    pub chunks: Option<Vec<Chunk>>,
    pub(crate) chunk_headers: Option<HashMap<String, serde_json::Value>>,
    pub(crate) qrmk: Option<String>,
    pub(crate) query_result_format: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalInitAsyncQueryResult {
    pub query_id: String,
    pub get_result_url: String,
    pub query_aborts_after_secs: i64,
    pub progress_desc: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalMonitoringQueriesResult {
    pub queries: Vec<InternalMonitoringQueryResult>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalMonitoringQueryResult {
    pub id: String,
    pub status: QueryStatus,
}
