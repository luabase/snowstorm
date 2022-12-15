use crate::errors::SnowflakeError;
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::row::RowType;
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


#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for VecResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        Ok(VecResult {
            rowtype: res.rowtype,
            rowset: res.rowset,
            query_id: res.query_id.clone(),
            query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
        })
    }

}


pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for HashMapResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;

        let mut rowset = Vec::new();
        for row in res.rowset {
            let mut mapping = HashMap::<String, serde_json::Value>::new();
            for it in res.rowtype.iter().zip(row.iter()) {
                let (ai, bi) = it;
                mapping.insert(ai.name.clone(), bi.clone());
            }

            rowset.push(mapping);
        }

        Ok(HashMapResult {
            rowtype: res.rowtype,
            rowset,
            query_id: res.query_id.clone(),
            query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
        })
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
            for it in res.rowtype.iter().zip(row.iter()) {
                let (ai, bi) = it;
                mapping.insert(ai.name.clone(), bi.clone());
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
