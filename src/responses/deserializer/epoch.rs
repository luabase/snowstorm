use crate::errors::SnowflakeError;
use crate::responses::types::row_type::RowType;
use anyhow::anyhow;

pub(super) fn get_json_time_scale(row_type: &RowType) -> Result<f64, SnowflakeError> {
    match row_type.scale {
        Some(scale) => {
            assert!(scale <= 9);
            Ok(10_f64.powf(f64::from(9 - scale)))
        }
        None => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Missing required scale for field {}", row_type.name),
            row_type.name.clone(),
        )),
    }
}

pub(super) fn get_arrow_time_scale(field: &arrow2::datatypes::Field) -> Result<i64, SnowflakeError> {
    match field.metadata.get("scale") {
        Some(s) => {
            let scale = s.parse::<u32>().map_err(|e| {
                SnowflakeError::new_deserialization_error_with_field_and_value(e.into(), field.name.clone(), s.clone())
            })?;
            assert!(scale <= 9);
            Ok(10_i64.pow(9 - scale))
        }
        None => Err(SnowflakeError::new_deserialization_error_with_field(
            anyhow!("Missing required scale for field {}", field.name),
            field.name.clone(),
        )),
    }
}

#[cfg(feature = "arrow")]
pub(super) fn arrow_int64_to_epoch(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Option<i64>>, SnowflakeError> {
    use crate::utils::until_err;
    use arrow2::array::PrimitiveArray;

    let downcasted = match column.as_any().downcast_ref::<PrimitiveArray<i64>>() {
        Some(x) => x,
        None => {
            return Err(SnowflakeError::new_deserialization_error_with_field(
                anyhow!("Could not convert to primitive array of i64"),
                field.name.clone(),
            ))
        }
    };

    let mut err = Ok(());
    let res = downcasted
        .iter()
        .map(|e| match e {
            Some(value) => Ok(Some(*value)),
            None => Ok(None),
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}

#[cfg(feature = "arrow")]
pub(super) fn arrow_struct_to_epoch(
    column: &dyn arrow2::array::Array,
    field: &arrow2::datatypes::Field,
) -> Result<Vec<Option<i64>>, SnowflakeError> {
    use crate::utils::until_err;
    use arrow2::array::StructArray;
    use arrow2::scalar::PrimitiveScalar;

    let mut err = Ok(());
    let _downcasted = column.as_any().downcast_ref::<StructArray>().unwrap();
    let res: Vec<Option<i64>> = _downcasted
        .iter()
        .map(|e| match e {
            Some(value) => {
                let downcasted = (value[0]).as_any().downcast_ref::<PrimitiveScalar<i64>>();
                match downcasted {
                    Some(d) => Ok(*d.value()),
                    None => Err(SnowflakeError::new_deserialization_error_with_field(
                        anyhow!("Could not deserialize {:?} as i64", field.data_type),
                        field.name.clone(),
                    )),
                }
            }
            None => Ok(None),
        })
        .scan(&mut err, until_err)
        .collect();

    match err {
        Ok(..) => Ok(res),
        Err(e) => Err(e),
    }
}
