use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType};
use crate::responses::{get_query_detail_url, QueryResult};
use crate::session::Session;

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
        rowset: &Vec<Vec<serde_json::Value>>,
        rowtype: &Vec<RowType>,
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
                    }
                    Err(e) => return Err(e),
                }
            }

            deserialized.push(mapping);
        }

        Ok(deserialized)
    }

    #[cfg(feature = "arrow")]
    fn deserialize_rowset64(
        rowset: &String,
        rowtype: &Vec<RowType>,
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        use anyhow::anyhow;
        use arrow2::io::ipc::read;
        use std::thread;
        use std::time::Duration;

        let data = base64::decode(rowset)
            .map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;
        let mut stream: &[u8] = &data;

        let metadata = read::read_stream_metadata(&mut stream)
            .map_err(|e| SnowflakeError::new_deserialization_error(e.into()))?;
        let mut stream = read::StreamReader::new(&mut stream, metadata, None);
        let mut idx = 0;
        loop {
            match stream.next() {
                Some(x) => match x {
                    Ok(read::StreamState::Some(b)) => {
                        idx += 1;
                        println!("batch: {:?}", idx)
                    }
                    Ok(read::StreamState::Waiting) => thread::sleep(Duration::from_millis(2000)),
                    Err(l) => println!("{:?} ({})", l, idx),
                },
                None => break,
            };
        }

        Err(SnowflakeError::GeneralError(anyhow!("Bla")))
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
            total: res.total,
        }
    }
}
