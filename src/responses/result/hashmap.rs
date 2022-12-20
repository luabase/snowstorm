use crate::errors::SnowflakeError;
use crate::responses::{QueryResult, get_query_detail_url};
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{chunk::Chunk, internal::InternalResult, row_type::RowType, value::Value};
use crate::session::Session;

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}

impl QueryDeserializer for HashMapResult {

    type ReturnType = Vec<HashMap<String, Value>>;

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError> {
        res.rowset
            .iter()
            .map(|r| {
                r.iter().zip(res.rowtype.iter())
                .map(|(v, t)| {
                    match Self::deserialize_value(v, t) {
                        Ok(x) => Ok((t.name.clone(), x)),
                        Err(e) => Err(e)
                    }
                })
                .collect()
            })
            .collect()
    }

}

impl QuerySerializer for HashMapResult {}

impl QueryResult for HashMapResult {

    fn new(res: &InternalResult, rowset: &Self::ReturnType, session: &Session) -> Self {
        Self {
            rowtype: res.rowtype.clone(),
            rowset: rowset.clone(),
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone()),
            total: res.total,
            qrmk: res.qrmk.clone(),
            chunks: res.chunks.clone()
        }
    }

}
