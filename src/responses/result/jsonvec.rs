use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType};
use crate::responses::{get_query_detail_url, QueryResult};
use crate::session::Session;

#[derive(Clone, Debug)]
pub struct JsonVecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
}

impl QueryDeserializer for JsonVecResult {
    type ReturnType = Vec<serde_json::Value>;

    fn deserialize_rowset(
        rowset: &[Vec<serde_json::Value>],
        rowtype: &[RowType],
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let mut deserialized = Vec::new();
        for row in rowset {
            let mut deserialied_row = Vec::<serde_json::Value>::with_capacity(row.len());
            for it in row.iter().zip(rowtype.iter()) {
                let (v, t) = it;
                let deserialized = Self::deserialize_value(v, t);
                match deserialized {
                    Ok(v) => {
                        let serialized =
                            Self::serialize_value(&v).map_err(|e| SnowflakeError::SerializationError(e.into()))?;
                        deserialied_row.push(serialized);
                    }
                    Err(e) => return Err(e),
                }
            }

            deserialized.push(deserialied_row);
        }

        Ok(deserialized)
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
                let serialized = Self::serialize_value(c).map_err(|e| SnowflakeError::SerializationError(e.into()))?;
                rows[i].push(serialized);
            }
        }

        Ok(rows)
    }
}

impl QuerySerializer for JsonVecResult {}

impl QueryResult for JsonVecResult {
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
