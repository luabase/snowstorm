use crate::errors::{SnowflakeError, WrappedDecimalConvertError};
use crate::responses::types::value::Value;
use decimal_rs::Decimal;

pub(super) fn from_arrow(
    scale: &usize,
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::PrimitiveArray;

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<PrimitiveArray<i128>>().unwrap();

    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value = match x {
                Some(x) => x,
                None => return null_from_arrow(field),
            };

            let decimal =
                Decimal::from_parts(value.unsigned_abs(), *scale as i16, value.is_negative()).map_err(|e| {
                    SnowflakeError::new_deserialization_error(WrappedDecimalConvertError { source: e }.into())
                })?;

            if field.is_nullable {
                let boxed = Box::new(Value::Decimal(decimal));
                Ok(Value::Nullable(Some(boxed)))
            } else {
                Ok(Value::Decimal(decimal))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
