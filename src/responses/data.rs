use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DataResponse<S> {
    pub data: S,
    pub message: Option<String>,
    pub success: bool,
}
