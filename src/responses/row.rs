use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    pub nullable: bool,
    #[serde(rename="type")]
    pub data_type: String,
    pub name: String
}
