use crate::errors::SnowflakeError;
use crate::responses::{QueryResult, get_query_detail_url};
use crate::responses::deserializer::QueryDeserializer;
use crate::responses::serializer::QuerySerializer;
use crate::responses::types::{internal::InternalResult, row_type::RowType, value::Value};
use crate::session::Session;

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct HashMapResult {
    pub rowtype: Vec<RowType>,
    pub rowset: Vec<HashMap<String, Value>>,
    pub query_id: String,
    pub query_detail_url: String,
    pub total: usize
}

impl QueryDeserializer for HashMapResult {

    type ReturnType = HashMap<String, Value>;

    fn deserialize_rowset<'a>(
        rowset: &Vec<Vec<serde_json::Value>>,
        rowtype: &Vec<RowType>
    ) -> Result<Vec<Self::ReturnType>, SnowflakeError> {
        rowset
            .iter()
            .map(|r| {
                r.iter().zip(rowtype.iter())
                .map(|(v, t)| {
                    match Self::deserialize_value(v, t) {
                        Ok(x) => Ok((t.name.clone(), x)),
                        Err(e) => Err(e)
                    }
                })
                .collect()
            })
            .collect()
    }

}

impl QuerySerializer for HashMapResult {}

impl<'a> QueryResult for HashMapResult {

    fn new(res: &InternalResult, rowset: &Vec<Self::ReturnType>, session: &Session) -> Self {
        Self {
            rowtype: res.rowtype.clone(),
            rowset: rowset.clone(),
            query_id: res.query_id.clone(),
            query_detail_url: get_query_detail_url(session, &res.query_id.clone()),
            total: res.total
        }
    }

}
