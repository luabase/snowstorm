pub mod deserializer;
pub mod result;
pub mod serializer;
pub mod types;

use crate::errors::SnowflakeError;
use crate::responses::types::internal::InternalResult;
use crate::session::Session;

pub trait QueryResult: deserializer::QueryDeserializer + serializer::QuerySerializer + Sized {

    fn new(res: &InternalResult, rowset: &Self::ReturnType, session: &Session) -> Self;

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError> {
        let res: InternalResult = serde_json::from_value(json.clone())
            .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), json.to_string()))?;
        let rowset = Self::deserialize_rowset(&res);
        match rowset {
            Ok(r) => Ok(Self::new(&res, &r, session)),
            Err(e) => Err(e)
        }
    }

    // fn load_chunk(&self, chunk: Chunk) -> Result<Self::ReturnType, SnowflakeError> {
    //     Self::ReturnType::new()
    // }

}

pub(crate) fn get_query_detail_url(session: &Session, query_id: &String) -> String {
    let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
        .into_iter()
        .filter_map(|x| x)
        .collect();
    let path = components.join("/");
    format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
}
