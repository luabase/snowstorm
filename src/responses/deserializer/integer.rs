use crate::errors::SnowflakeError;
use crate::responses::types::{row_type::RowType, value::Value};

use anyhow::anyhow;
use serde_json;

pub(super) fn from_json(json: &serde_json::Value, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: i128 = serde_json::from_value(json.clone()).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;

    if row_type.nullable {
        let boxed = Box::new(Value::Integer(parsed));
        Ok(Value::Nullable(Some(boxed)))
    }
    else {
        Ok(Value::Integer(parsed))
    }
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use arrow2::datatypes::DataType;

    match field.data_type {
        DataType::Int8 => downcast_integer::<i8>(column, field),
        DataType::UInt8 => downcast_integer::<u8>(column, field),
        DataType::Int16 => downcast_integer::<i16>(column, field),
        DataType::UInt16 => downcast_integer::<u16>(column, field),
        DataType::Int32 => downcast_integer::<i32>(column, field),
        DataType::UInt32 => downcast_integer::<u32>(column, field),
        DataType::Int64 => downcast_integer::<i64>(column, field),
        DataType::UInt64 => downcast_integer::<u64>(column, field),
        _ => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid integer data type {:?}", field.data_type),
            field.name.clone(),
        )),
    }
}

fn downcast_integer<T: arrow2::types::NativeType + num::NumCast>(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::PrimitiveArray;

    match column.as_any().downcast_ref::<PrimitiveArray<T>>() {
        Some(opt) => {
            let mut err = Ok(());

            let res: Vec<Value> = opt
                .iter()
                .map(|x| {
                    let value: i128;
                    match x {
                        Some(x) => value = num::cast(*x).unwrap(),
                        None => return null_from_arrow(field),
                    }

                    if field.is_nullable {
                        let boxed = Box::new(Value::Integer(value as i128));
                        Ok(Value::Nullable(Some(boxed)))
                    }
                    else {
                        Ok(Value::Integer(value))
                    }
                })
                .scan(&mut err, until_err)
                .collect();

            match err {
                Ok(..) => Ok(res),
                Err(e) => Err(e),
            }
        }
        None => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!(
                "Could not convert primitive array of type {:?} to i128",
                field.data_type
            ),
            field.name.clone(),
        )),
    }
}
