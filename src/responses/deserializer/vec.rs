use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use serde_json;

pub(super) fn from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: Vec<serde_json::Value> = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;
    if row_type.nullable {
        let boxed = Box::new(Value::Vec(parsed));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::Vec(parsed))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::Utf8Array;

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value = match x {
                Some(x) => x,
                None => return null_from_arrow(field),
            };

            let json: Vec<serde_json::Value> = serde_json::from_str(value)
                .map_err(|e| SnowflakeError::new_deserialization_error_with_field(e.into(), field.name.clone()))?;

            if field.is_nullable {
                let boxed = Box::new(Value::Vec(json));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::Vec(json))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
