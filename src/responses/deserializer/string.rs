use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    if row_type.nullable {
        let boxed = Box::new(Value::String(json.to_string()));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::String(json.to_string()))
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
            let value;
            match x {
                Some(x) => value = x,
                None => return null_from_arrow(field),
            }

            if field.is_nullable {
                let boxed = Box::new(Value::String(value.to_owned()));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::String(value.to_owned()))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
