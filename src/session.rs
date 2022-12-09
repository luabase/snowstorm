use crate::errors::SnowflakeError;
use crate::responses::{DataResponse, QueryResponse};
use crate::requests::QueryRequest;

use anyhow::anyhow;
use std::str;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Session {
    client: reqwest::Client,
    host: String,
    sequence_counter: u32
}

impl Session {

    pub fn new(client: reqwest::Client, host: &str) -> Self {
        Session {
            client,
            host: host.to_owned(),
            sequence_counter: 1
        }
    }

    pub async fn execute(&self, query: &str) -> Result<QueryResponse, SnowflakeError> {
        let timestamp = (OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000) as i64;
        let req = QueryRequest {
            async_exec: false,
            query_submission_time: timestamp,
            sequence_id: self.sequence_counter,
            sql_text: query
        };

        let json = self.client
            .post(&self.get_queries_url("query-request"))
            .json(&req)
            .build()
            .map_err(|e| SnowflakeError::ExecutionError(e.into()))?;

        let body = self.client
            .execute(json).await
            .map_err(|e| SnowflakeError::ExecutionError(e.into()))?;

        let text = body
            .text().await
            .map_err(|e| SnowflakeError::ExecutionError(e.into()))?;

        log::debug!("Response: {text}");

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| {
                log::error!("Failed to execute query {query} due to deserialization error. API response was: {text}");
                SnowflakeError::DeserializationError(e.into())
            })?;

        if !res.success {
            if let Some(message) = res.message {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query} with reason: {message}")
                ));
            }
            else {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query}, but no reason was given by Snowflake API")
                ));
            }
        }

        let data: QueryResponse = serde_json::from_value(res.data)
            .map_err(|e| {
                log::error!(
                    "Failed to execute query {query} due to data deserialization error. API response was: {text}"
                );
                SnowflakeError::DeserializationError(e.into())
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
