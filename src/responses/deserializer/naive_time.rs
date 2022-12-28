use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use chrono::{prelude::*, Duration};
use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: f64 = serde_json::from_value(json.clone()).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;
    let nanos = (parsed * 1_000_000_000.0).round() as i64;
    let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);
    if row_type.nullable {
        let boxed = Box::new(Value::NaiveTime(time));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::NaiveTime(time))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use arrow2::array::PrimitiveArray;

    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<i64>>().unwrap();
    downcasted
        .iter()
        .map(|e| match e {
            Some(value) => {
                let nanos = *value * 1000;
                let value = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + Duration::nanoseconds(nanos);

                if field.is_nullable {
                    let boxed = Box::new(Value::NaiveTime(value));
                    Ok(Value::Nullable(Some(boxed)))
                }
                else {
                    Ok(Value::NaiveTime(value))
                }
            }
            None => null_from_arrow(field),
        })
        .collect()
}
