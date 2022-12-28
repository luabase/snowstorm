use crate::errors::SnowflakeError;
use crate::responses::types::{
    row_type::RowType,
    value::{Value, ValueType},
};

use anyhow::anyhow;
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
    let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
    let datetime = DateTime::<Utc>::from_utc(naive, Utc);

    if row_type.nullable {
        let boxed = Box::new(Value::DateTimeUTC(datetime));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::DateTimeUTC(datetime))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &Box<dyn arrow2::array::Array>,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::PrimitiveArray;

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value;
            match x {
                Some(x) => {
                    let nanos = (*x * 1_000_000_000.0).round() as i64;
                    let naive = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                    value = DateTime::<Utc>::from_utc(naive, Utc);
                }
                None => return null_from_arrow(column, field),
            }

            if field.is_nullable {
                let boxed = Box::new(Value::DateTimeUTC(value));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::DateTimeUTC(value))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
