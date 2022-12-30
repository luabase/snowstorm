use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use chrono::{prelude::*, Duration};
use serde_json;

pub(super) fn from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: i64 = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(parsed);
    if row_type.nullable {
        let boxed = Box::new(Value::NaiveDate(date));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::NaiveDate(date))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use arrow2::array::PrimitiveArray;

    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<i32>>().unwrap();
    downcasted
        .iter()
        .map(|e| match e {
            Some(value) => {
                let value = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(*value as i64);

                if field.is_nullable {
                    let boxed = Box::new(Value::NaiveDate(value));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::NaiveDate(value))
                }
            }
            None => null_from_arrow(field),
        })
        .collect()
}
