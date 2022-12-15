use crate::errors::SnowflakeError;
use crate::responses::{data::DataResponse, deserializer::QueryDeserializer, error::ErrorResult};
use crate::requests::QueryRequest;

use anyhow::anyhow;
use chrono::prelude::*;
use serde_json::json;
use std::str;

#[derive(Debug)]
pub struct Session {
    pub(crate) client: reqwest::Client,
    pub(crate) host: String,
    pub(crate) account: String,
    pub(crate) region: Option<String>,
    pub(crate) sequence_counter: u32
}

impl Session {

    pub fn new(client: reqwest::Client, host: &str, account: &str, region: Option<&str>) -> Self {
        Session {
            client,
            host: host.to_owned(),
            account: account.to_owned(),
            region: region.map(str::to_string),
            sequence_counter: 1
        }
    }

    pub async fn execute<T: QueryDeserializer>(&self, query: &str) -> Result<T, SnowflakeError> {
        let now = Utc::now();
        let req = QueryRequest {
            async_exec: false,
            parameters: Some(json!({"TIMESTAMP": "UTC"})),
            query_submission_time: now.timestamp_millis(),
            sequence_id: self.sequence_counter,
            sql_text: query
        };

        let json = self.client
            .post(&self.get_queries_url("query-request"))
            .json(&req)
            .build()
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let body = self.client
            .execute(json).await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let text = body
            .text().await
            .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))?;

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| {
                log::error!("Failed to execute query {query} due to deserialization error.");
                SnowflakeError::DeserializationError(e.into())
            })?;

        if !res.success {
            let err = ErrorResult::deserialize(res.data, self)?;
            if let Some(message) = res.message {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query} with reason: {message}"),
                    Some(err)
                ));
            }
            else {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query}, but no reason was given by Snowflake API"),
                    Some(err)
                ));
            }
        }

        let data = T::deserialize(res.data, self)
            .map_err(|e| {
                log::error!(
                    "Failed to execute query {query} due to data deserialization error."
                );
                e
            })?;

        Ok(data)
    }

    fn get_queries_url(&self, command: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        let guid = uuid::Uuid::new_v4();
        let url = format!("{}/queries/v1/{command}?requestId={uuid}&request_guid={guid}", self.host);
        log::debug!("Using query url {url}");
        url
    }

}
