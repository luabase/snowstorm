use crate::errors::SnowflakeError;
use crate::responses::row::RowType;
use crate::session::Session;

use serde::Deserialize;
use std::collections::HashMap;


pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError>;

    fn get_query_detail_url(session: &Session, query_id: &String) -> String {
        let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
            .into_iter()
            .filter_map(|x| x)
            .collect();
        let path = components.join("/");
        format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
    }

}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InternalErrorResult {
    #[serde(rename="type")]
    error_type: Option<String>,
    error_code: String,
    internal_error: bool,
    line: Option<i32>,
    pos: Option<i32>,
    query_id: String
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResult {
    #[serde(rename="type")]
    pub error_type: Option<String>,
    pub error_code: String,
    pub internal_error: bool,
    pub line: Option<i32>,
    pub pos: Option<i32>,
    pub query_id: String,
    pub query_detail_url: String
}

impl QueryDeserializer for ErrorResult {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalErrorResult = serde_json::from_value(json)
            .map_err(|e| SnowflakeError::DeserializationError(e.into()))?;
        Ok(ErrorResult {
            error_type: res.error_type,
            error_code: res.error_code,
            internal_error: res.internal_error,
            line: res.line,
            pos: res.pos,
            query_id: res.query_id.clone(),
            query_detail_url: Self::get_query_detail_url(session, &res.query_id.clone())
        })
    }

}


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
