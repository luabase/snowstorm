use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};
use anyhow::anyhow;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    _handle_null_value(&row_type.name, row_type.nullable)
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &Box<dyn arrow2::array::Array>,
    field: &arrow2::datatypes::Field,
) -> Result<Value, SnowflakeError> {
    _handle_null_value(&field.name, field.is_nullable)
}

fn _handle_null_value(field_name: &String, is_nullable: bool) -> Result<Value, SnowflakeError> {
    if is_nullable {
        Ok(Value::Nullable(None))
    }
    else {
        let e = anyhow!("Encountered NULL value for non-nullable field {}", field_name);
        Err(SnowflakeError::DeserializationError(e, None))
    }
}
