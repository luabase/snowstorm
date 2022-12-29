use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType, value::Value};
use crate::responses::{get_query_detail_url, QueryResult};
use crate::session::Session;

#[derive(Clone, Debug)]
pub struct VecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
}

impl QueryDeserializer for VecResult {
    type ReturnType = Vec<Value>;

    fn deserialize_rowset(
        rowset: &Vec<Vec<serde_json::Value>>,
        rowtype: &Vec<RowType>,
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        rowset
            .iter()
            .map(|r| {
                r.iter()
                    .zip(rowtype.iter())
                    .map(|(v, t)| Self::deserialize_value(v, t))
                    .collect()
            })
            .collect()
    }

    #[cfg(feature = "arrow")]
    fn deserialize_rowset64(rowset: &str) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let mut rows: Vec<Self::ReturnType> = vec![];
        let (metadata, chunk) = Self::get_arrow_stream_from_rowset64(rowset)?;
        if let Some(chunk) = chunk {
            for (idx, column) in chunk.columns().iter().enumerate() {
                let field = &metadata.schema.fields[idx];
                let col = Self::deserialize_arrow_column(column.as_ref(), field)?;
                rows.push(col);
            }
        }

        Ok(rows)
    }
}

impl QuerySerializer for VecResult {}

impl QueryResult for VecResult {
    fn new(res: &InternalResult, rowset: &Vec<Self::ReturnType>, session: &Session) -> Self {
        Self {
            rowtype: res.rowtype.clone(),
            rowset: rowset.clone(),
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone()),
            total: res.total,
        }
    }
}
