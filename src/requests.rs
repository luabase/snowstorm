use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DataRequest<S> {
    pub data: S,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct LoginRequest<'a> {
    pub account_name: &'a str,
    pub login_name: &'a str,
    pub password: &'a str,
    pub client_app_id: &'a str,
    pub client_app_version: &'a str,
    pub session_parameters: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest<'a> {
    pub async_exec: bool,
    pub parameters: Option<serde_json::Value>,
    pub query_submission_time: i64,
    pub sequence_id: u32,
    pub sql_text: &'a str,
}
