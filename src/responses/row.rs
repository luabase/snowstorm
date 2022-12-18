use crate::responses::types::ValueType;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
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

impl RowType {

    pub fn value_type(&self) -> ValueType {
        if self.nullable {
            ValueType::Nullable(Box::new(self.inner_value_type()))
        }
        else {
            self.inner_value_type()
        }
    }

    fn inner_value_type(&self) -> ValueType {
        match self.data_type.as_str() {
            "boolean" => ValueType::Boolean,
            "fixed" => ValueType::Number,
            "real" => ValueType::Float,
            "text" => ValueType::String,
            "binary" => ValueType::Binary,
            "date" => ValueType::NaiveDate,
            "time" => ValueType::NaiveTime,
            "timestamp_ntz" => ValueType::NaiveDateTime,
            "timestamp_ltz" => ValueType::DateTimeUTC,
            "timestamp_tz" => ValueType::DateTime,
            "variant" => ValueType::Variant,
            "object" => ValueType::HashMap,
            "array" => ValueType::Vec,
            _ => ValueType::Unsupported
        }
    }

}
