use crate::errors::SnowflakeError;
use crate::responses::types::value::Value;
use rust_decimal::prelude::*;

pub(super) fn from_arrow(
    scale: &usize,
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Value>, SnowflakeError> {
    use crate::responses::deserializer::null::from_arrow as null_from_arrow;
    use crate::utils::until_err;
    use arrow2::array::Utf8Array;

    println!("+++ COLUMN {scale} {column:?}");

    let mut err = Ok(());
    let downcasted = column.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
    let res: Vec<Value> = downcasted
        .iter()
        .map(|x| {
            let value = match x {
                Some(x) => x,
                None => return null_from_arrow(field),
            };

            println!("+++ VALUE {value:?}");

            if field.is_nullable {
                let boxed = Box::new(Value::Float(0.0));
                Ok(Value::Nullable(Some(boxed)))
            }
            else {
                Ok(Value::Float(0.0))
            }
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
