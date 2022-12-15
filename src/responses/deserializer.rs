use crate::errors::SnowflakeError;
use crate::session::Session;

pub trait QueryDeserializer: Sized {

    fn deserialize(json: serde_json::Value, session: &Session) -> Result<Self, SnowflakeError>;

    fn get_query_detail_url(session: &Session, query_id: &String) -> String {
        let components: Vec<String> = [session.region.clone(), Some(session.account.clone())]
            .into_iter()
            .filter_map(|x| x)
            .collect();
        let path = components.join("/");
        format!("https://app.snowflake.com/{path}/#/compute/history/queries/{query_id}/detail")
    }

}
