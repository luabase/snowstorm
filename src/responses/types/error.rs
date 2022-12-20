use crate::errors::SnowflakeError;
use crate::responses::get_query_detail_url;
use crate::session::Session;
use serde::Deserialize;

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

impl ErrorResult {

    pub(crate) fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalErrorResult = serde_json::from_value(json.clone())
            .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), json.to_string()))?;
        Ok(Self {
            error_type: res.error_type,
            error_code: res.error_code,
            internal_error: res.internal_error,
            line: res.line,
            pos: res.pos,
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone())
        })
    }

}
