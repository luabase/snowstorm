use crate::responses::types::value::Value;
use std::collections::HashMap;

pub trait QuerySerializer {
    fn serialize_value(val: &Value) -> Result<serde_json::Value, serde_json::Error> {
        match val {
            Value::Binary(v) => serde_json::to_value(&v),
            Value::Boolean(v) => serde_json::to_value(&v),
            Value::Integer(x) => {
                if cfg!(integer128) {
                    serde_json::to_value(&x)
                }
                else {
                    serde_json::to_value(&x.to_string())
                }
            }
            Value::Float(v) => serde_json::to_value(&v),
            Value::String(v) => serde_json::to_value(&v),
            Value::NaiveDate(v) => serde_json::to_value(&v),
            Value::NaiveTime(v) => serde_json::to_value(&v),
            Value::NaiveDateTime(v) => serde_json::to_value(&v),
            Value::DateTimeUTC(v) => serde_json::to_value(&v),
            Value::DateTime(v) => serde_json::to_value(&v),
            Value::HashMap(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Vec(v) => serde_json::to_value(&v),
            Value::Geography(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Geometry(v) => serde_json::to_value(Self::to_json_map(v)),
            Value::Variant(v) => serde_json::to_value(&v),
            Value::Unsupported(v) => serde_json::to_value(&v),
            Value::Nullable(v) => match v {
                Some(x) => Self::serialize_value(x),
                None => serde_json::to_value::<Option<String>>(None),
            },
        }
    }

    fn to_json_map(m: &HashMap<String, serde_json::Value>) -> String {
        let value = serde_json::to_value(m);
        match value {
            Ok(v) => serde_json::to_string(&v).unwrap_or("{}".to_owned()),
            Err(e) => e.to_string(),
        }
    }
}
