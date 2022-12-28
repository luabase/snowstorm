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
    let date = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
    if row_type.nullable {
        let boxed = Box::new(Value::NaiveDateTime(date));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::NaiveDateTime(date))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &Box<dyn arrow2::array::Array>,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::{PrimitiveArray, StructArray};
    use arrow2::scalar::PrimitiveScalar;

    let mut err = Ok(());
    let x = column.as_any().downcast_ref::<StructArray>().unwrap();
    x.iter().for_each(|x| match x {
        Some(x) => {
            println!(
                "+++ {:?}",
                (x[0]).as_any().downcast_ref::<PrimitiveScalar<i64>>().unwrap().value()
            )
        }
        None => println!("+++ {:?}", x),
    });

    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<f64>>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value;
            match x {
                Some(x) => {
                    let nanos = (*x * 1_000_000_000.0).round() as i64;
                    value = NaiveDateTime::from_timestamp_opt(0, 0).unwrap() + Duration::nanoseconds(nanos);
                }
                None => return null_from_arrow(column, field),
            }

            if field.is_nullable {
                let boxed = Box::new(Value::NaiveDateTime(value));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::NaiveDateTime(value))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
