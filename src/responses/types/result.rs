use crate::errors::SnowflakeError;
use crate::responses::deserializer::{QueryDeserializer, get_query_detail_url};
use crate::responses::types::{row::RowType, value::Value};
use crate::session::Session;

use serde::Deserialize;
use std::collections::HashMap;


#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chunk {
    pub row_count: u64,
    pub url: String
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InternalResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>,
    pub query_id: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}


#[derive(Debug)]
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

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError> {
        res.rowset
            .iter()
            .map(|r| r.iter().zip(res.rowtype.iter()).map(|(v, t)| Self::deserialize_value(v, t)).collect())
            .collect()
    }

}


pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}

impl QueryDeserializer for HashMapResult {

    type ReturnType = Vec<HashMap<String, Value>>;

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

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError> {
        res.rowset
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
            .collect()
    }

}


pub struct JsonMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<serde_json::Map<String, serde_json::Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize,
    pub qrmk: Option<String>,
    pub chunks: Option<Vec<Chunk>>
}

impl QueryDeserializer for JsonMapResult {

    type ReturnType = Vec<serde_json::Map<String, serde_json::Value>>;

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

    fn deserialize_rowset(res: &InternalResult) -> Result<Self::ReturnType, SnowflakeError> {
        let mut deserialized = Vec::new();
        for row in &res.rowset {
            let mut mapping = serde_json::Map::<String, serde_json::Value>::new();
            for it in row.iter().zip(res.rowtype.iter()) {
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
