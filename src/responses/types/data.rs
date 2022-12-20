use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct DataResponse<S> {
    pub data: S,
    pub message: Option<String>,
    pub success: bool,
}
