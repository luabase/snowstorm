use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType, value::Value};
use crate::responses::{get_query_detail_url, QueryResult};
use crate::session::Session;

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
}

impl QueryDeserializer for HashMapResult {
    type ReturnType = HashMap<String, Value>;

    fn deserialize_rowset(
        rowset: &[Vec<serde_json::Value>],
        rowtype: &[RowType],
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        rowset
            .iter()
            .map(|r| {
                r.iter()
                    .zip(rowtype.iter())
                    .map(|(v, t)| match Self::deserialize_value(v, t) {
                        Ok(x) => Ok((t.name.clone(), x)),
                        Err(e) => Err(e),
                    })
                    .collect()
            })
            .collect()
    }

    #[cfg(feature = "arrow")]
    fn deserialize_arrow_chunk(
        schema: &arrow2::datatypes::Schema,
        chunk: &arrow2::chunk::Chunk<Box<dyn arrow2::array::Array>>,
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let mut rows: Vec<Self::ReturnType> = vec![Self::ReturnType::new(); chunk.len()];
        for (idx, column) in chunk.columns().iter().enumerate() {
            let field = &schema.fields[idx];
            let col = Self::deserialize_arrow_column(column.as_ref(), field)?;
            for (i, c) in col.iter().enumerate() {
                rows[i].insert(field.name.clone(), c.clone());
            }
        }

        Ok(rows)
    }
}

impl QuerySerializer for HashMapResult {}

impl QueryResult for HashMapResult {
    fn new(res: &InternalResult, rowset: &[Self::ReturnType], session: &Session) -> Self {
        Self {
            rowtype: res.rowtype.clone(),
            rowset: rowset.to_vec(),
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone()),
            total: res.total,
        }
    }
}
