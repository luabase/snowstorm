use crate::responses::types::{chunk::Chunk, row_type::RowType};
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
