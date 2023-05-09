use crate::responses::types::value::ValueType;
use serde::Deserialize;

#[cfg(feature = "arrow")]
use arrow2;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    #[serde(rename = "type")]
    pub data_type: String,
    pub ext_type_name: Option<String>,
    pub name: String,
    pub nullable: bool,
    pub precision: Option<u32>,
    pub scale: Option<i32>,
    pub byte_length: Option<usize>,
}

impl RowType {
    #[cfg(feature = "arrow")]
    pub(crate) fn from_arrow_field(field: &arrow2::datatypes::Field) -> Self {
        Self {
            data_type: field.metadata.get("logicalType").unwrap().to_ascii_lowercase(),
            ext_type_name: None,
            name: field.name.clone(),
            nullable: field.is_nullable,
            precision: field.metadata.get("precision").map(|x| x.parse().unwrap()),
            scale: field.metadata.get("scale").map(|x| x.parse().unwrap()),
            byte_length: field.metadata.get("byteLength").map(|x| x.parse().unwrap()),
        }
    }

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
            "fixed" => {
                if self.scale == Some(0) {
                    if self.precision <= Some(18) {
                        ValueType::I64
                    }
                    else {
                        ValueType::I128
                    }
                }
                else {
                    ValueType::Float
                }
            }
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
            _ => ValueType::Unsupported,
        }
    }
}
