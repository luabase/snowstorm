use crate::errors::SnowflakeError;
use crate::responses::{QueryResult, get_query_detail_url};
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{chunk::Chunk, internal::InternalResult, row_type::RowType, value::Value};
use crate::session::Session;

#[derive(Clone, Debug)]
pub struct VecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}

impl QueryDeserializer for VecResult {

    type ReturnType = Vec<Vec<Value>>;

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError> {
        res.rowset
            .iter()
            .map(|r| r.iter().zip(res.rowtype.iter()).map(|(v, t)| Self::deserialize_value(v, t)).collect())
            .collect()
    }

}

impl QuerySerializer for VecResult {}

impl QueryResult for VecResult {

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
