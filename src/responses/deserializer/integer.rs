use crate::errors::{SnowflakeError, WrappedDecimalConvertError};
use crate::responses::types::value::ValueType;
use crate::responses::types::{row_type::RowType, value::Value};
use decimal_rs::Decimal;

use serde_json;

fn wrap_in_nullable(value: Value, is_nullale: bool) -> Value {
    if is_nullale {
        let boxed = Box::new(value);
        Value::Nullable(Some(boxed))
    } else {
        value
    }
}

pub(super) fn i64_from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: i64 = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;
    let value = Value::I64(parsed);

    Ok(wrap_in_nullable(value, row_type.nullable))
}

pub(super) fn i128_from_json(json: &str, row_type: &RowType) -> Result<Value, SnowflakeError> {
    let parsed: i128 = serde_json::from_str(json).map_err(|e| {
        SnowflakeError::new_deserialization_error_with_field_and_value(
            e.into(),
            row_type.name.clone(),
            json.to_string(),
        )
    })?;
    let value = Value::I128(parsed);

    Ok(wrap_in_nullable(value, row_type.nullable))
}

#[cfg(feature = "arrow")]
pub(super) fn from_arrow(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
    row_type: &RowType,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::decimal::from_arrow as decimal_from_arrow;
    use anyhow::anyhow;
    use arrow2::datatypes::DataType;
    match field.data_type {
        DataType::Int8 => downcast_integer::<i8>(column, field, row_type),
        DataType::UInt8 => downcast_integer::<u8>(column, field, row_type),
        DataType::Int16 => downcast_integer::<i16>(column, field, row_type),
        DataType::UInt16 => downcast_integer::<u16>(column, field, row_type),
        DataType::Int32 => downcast_integer::<i32>(column, field, row_type),
        DataType::UInt32 => downcast_integer::<u32>(column, field, row_type),
        DataType::Int64 => downcast_integer::<i64>(column, field, row_type),
        DataType::UInt64 => downcast_integer::<u64>(column, field, row_type),
        DataType::Decimal(_, scale) => decimal_from_arrow(&scale, column, field),
        _ => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid integer data type {:?}", field.data_type),
            field.name.clone(),
        )),
    }
}

fn upcast_i64_to_value_type(
    num: i64,
    value_type: ValueType,
    scale: i32,
    field_name: String,
) -> Result<Value, SnowflakeError> {
    use anyhow::anyhow;

    let value = match value_type {
        ValueType::Nullable(inner) => wrap_in_nullable(upcast_i64_to_value_type(num, *inner, scale, field_name)?, true),
        ValueType::Decimal => {
            let decimal = Decimal::from_parts(num.abs() as u128, scale as i16, num.is_negative()).map_err(|e| {
                SnowflakeError::new_deserialization_error(WrappedDecimalConvertError { source: e }.into())
            })?;
            Value::Decimal(decimal)
        }
        ValueType::I128 => Value::I128(num.into()),
        ValueType::I64 => Value::I64(num),
        ValueType::Float => {
            // Float types are stored as integers with a scale.
            let result = Decimal::from(num) / Decimal::from(10u64.pow(scale as u32));
            Value::Float(result.into())
        }
        t => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Invalid integer value type {:?}", t),
            field_name,
        ))?,
    };
    Ok(value)
}

#[cfg(feature = "arrow")]
fn downcast_integer<T: arrow2::types::NativeType + num::NumCast>(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
    row_type: &RowType,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use anyhow::anyhow;
    use arrow2::array::PrimitiveArray;

    match column.as_any().downcast_ref::<PrimitiveArray<T>>() {
        Some(opt) => {
            let mut err = Ok(());

            let res: Vec<Value> = opt
                .iter()
                .map(|x| {
                    let value: i64 = match x {
                        Some(x) => num::cast(*x).unwrap(),
                        None => return null_from_arrow(field),
                    };

                    // The arrow return types sometimes do not match with value_type.
                    // Ex: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY.total_elapsed_time is a Decimal(28,0) in value_type but I64 in arrow2.
                    // Here we make sure to encode the result as the expected value_type.
                    upcast_i64_to_value_type(
                        value,
                        row_type.value_type(),
                        row_type.scale.unwrap_or(0),
                        field.name.clone(),
                    )
                })
                .scan(&mut err, until_err)
                .collect();

            match err {
                Ok(..) => Ok(res),
                Err(e) => Err(e),
            }
        }
        None => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Could not convert primitive array of type {:?} to i64", field.data_type),
            field.name.clone(),
        )),
    }
}
