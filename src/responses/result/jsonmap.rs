use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType};
use crate::responses::{get_query_detail_url, QueryResult};
use crate::session::Session;

#[derive(Clone, Debug)]
pub struct JsonMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<serde_json::Map<String, serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
}

impl QueryDeserializer for JsonMapResult {
    type ReturnType = serde_json::Map<String, serde_json::Value>;

    fn deserialize_rowset(
        rowset: &[Vec<serde_json::Value>],
        rowtype: &[RowType],
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let mut deserialized = Vec::new();
        for row in rowset {
            let mut mapping = serde_json::Map::<String, serde_json::Value>::new();
            for it in row.iter().zip(rowtype.iter()) {
                let (v, t) = it;
                let deserialized = Self::deserialize_value(v, t);
                match deserialized {
                    Ok(v) => {
                        let serialized =
                            Self::serialize_value(&v).map_err(|e| SnowflakeError::SerializationError(e.into()))?;
                        mapping.insert(t.name.clone(), serialized);
                    }
                    Err(e) => return Err(e),
                }
            }

            deserialized.push(mapping);
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
                rows[i].insert(field.name.clone(), serialized);
            }
        }

        Ok(rows)
    }
}

impl QuerySerializer for JsonMapResult {}

impl QueryResult for JsonMapResult {
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
