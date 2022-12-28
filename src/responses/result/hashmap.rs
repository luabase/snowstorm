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
        rowset: &Vec<Vec<serde_json::Value>>,
        rowtype: &Vec<RowType>,
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
    fn deserialize_rowset64(rowset: &str) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        use arrow2::io::ipc::read;

        let data = base64::decode(rowset).map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;
        let mut stream: &[u8] = &data;

        let metadata =
            read::read_stream_metadata(&mut stream).map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;
        let schema = &metadata.schema.clone();

        let mut stream = read::StreamReader::new(&mut stream, metadata, None);

        let mut rows: Vec<Self::ReturnType> = vec![];
        if let Some(x) = stream.next() {
            match x {
                Ok(read::StreamState::Some(chunk)) => {
                    rows = vec![Self::ReturnType::new(); chunk.len()];
                    for (idx, column) in chunk.columns().iter().enumerate() {
                        let field = &schema.fields[idx];
                        let col = Self::deserialize_arrow_column(column.as_ref(), field)?;
                        for (i, c) in col.iter().enumerate() {
                            rows[i].insert(field.name.clone(), c.clone());
                        }
                    }
                }
                Ok(read::StreamState::Waiting) => (),
                Err(e) => return Err(SnowflakeError::new_deserialization_error(e.into())),
            }
        };

        Ok(rows)
    }
}

impl QuerySerializer for HashMapResult {}

impl QueryResult for HashMapResult {
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
