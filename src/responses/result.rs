use crate::errors::SnowflakeError;
use crate::responses::{deserializer::QueryDeserializer, row::RowType, types::ValueType};
use crate::session::Session;

use serde::Deserialize;
use std::collections::HashMap;


#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InternalResult {
    rowtype: Vec<RowType>,
    rowset: Vec<Vec<serde_json::Value>>,
    query_id: String
}


#[derive(Debug)]
pub struct VecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<ValueType>>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for VecResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        let rowset: Result<Vec<Vec<ValueType>>, SnowflakeError> = res.rowset
            .iter()
            .map(|r| r.iter().zip(res.rowtype.iter()).map(|(v, t)| Self::deserialize_value(v, t)).collect())
            .collect();
        match rowset {
            Ok(r) => Ok(VecResult {
                rowtype: res.rowtype,
                rowset: r,
                query_id: res.query_id.clone(),
                query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
            }),
            Err(e) => Err(e)
        }
    }

}


pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, ValueType>>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for HashMapResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        let rowset: Result<Vec<HashMap<String, ValueType>>, SnowflakeError> = res.rowset
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
            .collect();

        match rowset {
            Ok(r) => Ok(HashMapResult {
                rowtype: res.rowtype,
                rowset: r,
                query_id: res.query_id.clone(),
                query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
            }),
            Err(e) => Err(e)
        }
    }

}


pub struct JsonMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<serde_json::Map<String, serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for JsonMapResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        let mut rowset = Vec::new();
        for row in res.rowset {
            let mut mapping = serde_json::Map::<String, serde_json::Value>::new();
            for it in row.iter().zip(res.rowtype.iter()) {
                let (v, t) = it;
                let value = Self::deserialize_value(v, t);
                match value {
                    Ok(x) => {
                        let val = serde_json::to_value(&x)
                            .map_err(|e| SnowflakeError::SerializationError(e.into()))?;
                        mapping.insert(t.name.clone(), val);
                    },
                    Err(e) => return Err(e)
                }

            }

            rowset.push(mapping);
        }

        Ok(JsonMapResult {
            rowtype: res.rowtype,
            rowset,
            query_id: res.query_id.clone(),
            query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
        })
    }

}
