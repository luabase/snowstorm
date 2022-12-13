use crate::errors::SnowflakeError;
use crate::responses::row::RowType;

use serde::Deserialize;
use std::collections::HashMap;


pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value) -> Result<Self, SnowflakeError>;

}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VecResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>
}

impl QueryDeserializer for VecResult {

    fn deserialize(json: serde_json::Value) -> Result<Self, SnowflakeError> {
        let res: VecResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        Ok(res)
    }

}

pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, serde_json::Value>>
}

impl QueryDeserializer for  HashMapResult {

    fn deserialize(json: serde_json::Value) -> Result<Self, SnowflakeError> {
        let res: VecResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;

        let mut rowset = Vec::<HashMap<String, serde_json::Value>>::new();
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
            rowset
        })
    }

}
