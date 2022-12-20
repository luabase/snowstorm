use crate::errors::SnowflakeError;
use crate::responses::{QueryResult, get_query_detail_url};
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType};
use crate::session::Session;

pub struct JsonMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<serde_json::Map<String, serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize
}

impl QueryDeserializer for JsonMapResult {

    type ReturnType = serde_json::Map<String, serde_json::Value>;

    fn deserialize_rowset(
        rowset: &Vec<Vec<serde_json::Value>>,
        rowtype: &Vec<RowType>
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        let mut deserialized = Vec::new();
        for row in rowset {
            let mut mapping = serde_json::Map::<String, serde_json::Value>::new();
            for it in row.iter().zip(rowtype.iter()) {
                let (v, t) = it;
                let deserialized = Self::deserialize_value(v, t);
                match deserialized {
                    Ok(v) => {
                        let serialized = Self::serialize_value(&v)
                            .map_err(|e| SnowflakeError::SerializationError(e.into()))?;
                        mapping.insert(t.name.clone(), serialized);
                    },
                    Err(e) => return Err(e)
                }

            }

            deserialized.push(mapping);
        }

        Ok(deserialized)
    }
}

impl QuerySerializer for JsonMapResult {}

impl QueryResult for JsonMapResult {

    fn new(res: &InternalResult, rowset: &Vec<Self::ReturnType>, session: &Session) -> Self {
        Self {
            rowtype: res.rowtype.clone(),
            rowset: rowset.clone(),
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone()),
            total: res.total
        }
    }

}
