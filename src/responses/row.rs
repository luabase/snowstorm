use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    #[serde(rename="type")]
    pub data_type: String,
    pub ext_type_name: Option<String>,
    pub name: String,
    pub nullable: bool,
    pub precision: Option<u32>,
    pub scale: Option<i32>,
    pub byte_length: Option<usize>
}
