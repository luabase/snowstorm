use crate::errors::SnowflakeError;
use crate::responses::types::{
    row_type::RowType,
    value::{Value, ValueType},
};

use anyhow::anyhow;
use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: u8 = serde_json::from_value(json.clone()).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    let v;
    match parsed {
        0 => v = false,
        1 => v = true,
        x => {
            return Err(SnowflakeError::new_deserialization_error_with_field_and_value(
                anyhow!("Unexpected boolean value {parsed}"),
                row_type.name.clone(),
                x.to_string(),
            ))
        }
    }

    if row_type.nullable {
        let boxed = Box::new(Value::Boolean(v));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::Boolean(v))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &Box<dyn arrow2::array::Array>,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::BooleanArray;

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<BooleanArray>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value;
            match x {
                Some(x) => value = x,
                None => return null_from_arrow(column, field),
            }

            if field.is_nullable {
                let boxed = Box::new(Value::Boolean(value));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::Boolean(value))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
