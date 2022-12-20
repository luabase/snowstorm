use crate::responses::types::{chunk::Chunk, row_type::RowType};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>,
    pub query_id: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}
