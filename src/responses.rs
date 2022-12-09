use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DataResponse<S> {
    pub data: S,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    pub token: String
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<Vec<serde_json::Value>>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    pub nullable: bool,
    #[serde(rename="type")]
    pub data_type: String,
    pub name: String
}
