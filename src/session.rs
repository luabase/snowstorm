use crate::errors::SnowflakeError;
use crate::requests::QueryRequest;
use crate::responses::types::{
    data::DataResponse,
    error::ErrorResult,
    internal::{InternalInitAsyncQueryResult, InternalMonitoringQueriesResult, InternalResult},
    query::QueryStatus,
};
use crate::responses::{get_query_detail_url, make_chunk_downloader, QueryResult};

use anyhow::anyhow;
use chrono::prelude::*;
use futures::StreamExt;
use reqwest::header::ACCEPT;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::sync::atomic::AtomicI32;
use std::time::Duration;
use std::{
    str,
    sync::atomic::{AtomicU32, Ordering},
};
use tokio::time::Instant;

const MAX_NO_DATA_RETRY: i32 = 24;

#[derive(Debug)]
pub struct Session {
    pub(crate) client: reqwest::Client,
    pub(crate) host: String,
    pub(crate) account: String,
    pub(crate) region: Option<String>,
    pub(crate) proxy: Option<String>,
    pub(crate) sequence_counter: AtomicU32,
    pub(crate) max_parallel_downloads: Option<usize>,
    pub(crate) timeout: Option<Duration>,
}

impl Session {
    pub fn new(
        client: reqwest::Client,
        host: &str,
        account: &str,
        region: Option<&str>,
        proxy: &Option<String>,
        max_parallel_downloads: Option<usize>,
        timeout: Option<Duration>,
    ) -> Self {
        Session {
            client,
            host: host.to_owned(),
            account: account.to_owned(),
            region: region.map(str::to_string),
            proxy: proxy.clone(),
            sequence_counter: AtomicU32::new(1),
            max_parallel_downloads,
            timeout,
        }
    }

    pub async fn execute_async<T: QueryResult + Send + Sync>(&self, query: &str) -> Result<T, SnowflakeError> {
        let start_ts = Instant::now();
        let init_res: InternalInitAsyncQueryResult = self.execute_query_request(query, true, start_ts).await?;

        self.await_async_query(query, &init_res, start_ts).await?;
        let query_id = &init_res.query_id;
        self.execute_impl(&format!("select * from table(result_scan('{query_id}'))"), start_ts)
            .await
    }

    pub async fn execute<T: QueryResult + Send + Sync>(&self, query: &str) -> Result<T, SnowflakeError> {
        self.execute_impl(query, Instant::now()).await
    }

    async fn execute_impl<T: QueryResult + Send + Sync>(
        &self,
        query: &str,
        start_ts: Instant,
    ) -> Result<T, SnowflakeError> {
        let internal: InternalResult = self.execute_query_request(query, false, start_ts).await?;

        self.sequence_counter.fetch_add(1, Ordering::Relaxed);

        let mut rowset;
        if let Some(r) = &internal.rowset_base64 {
            rowset = T::deserialize_rowset64(r)?;
        }
        else if let Some(r) = &internal.rowset {
            rowset = T::deserialize_rowset(r, &internal.rowtype)?;
        }
        else {
            return Err(SnowflakeError::new_deserialization_error_with_value(
                anyhow!("Missing rowsetBase64 or rowset for Arrow format"),
                format!("{:?}", internal),
            ));
        }

        let timeout = self.get_remaining_timeout(start_ts);
        if let Some(chunks) = internal.chunks.clone() {
            let downloader = make_chunk_downloader(self, &internal, timeout)?;
            let mut buffered_chunks_futures = tokio_stream::iter(chunks)
                .map(|chunk| {
                    let task_query_result_format = internal.query_result_format.clone();
                    let task_downloader = downloader.clone();
                    let task_row_type = internal.rowtype.clone();
                    tokio::spawn(async move {
                        log::debug!("Downloading chunk at url: {}", chunk.url);
                        match task_query_result_format.as_str() {
                            "arrow" => chunk.load_arrow::<T>(&task_downloader).await,
                            "json" => chunk.load_json::<T>(&task_downloader, &task_row_type).await,
                            x => Err(SnowflakeError::ChunkLoadingError(anyhow!(
                                "Unsupported query result format {x}"
                            ))),
                        }
                    })
                })
                .buffered(self.max_parallel_downloads.unwrap_or(1));

            while let Some(joined_chunk) = buffered_chunks_futures.next().await {
                let chunk = joined_chunk.map_err(|e| SnowflakeError::ExecutionError(e.into(), None))??;
                rowset.extend(&mut chunk.into_iter());

                // This timeout is passed to the reqwest client, but because it's buffered the timeout may extend past the timeout.
                if let Some(Duration::ZERO) = self.get_remaining_timeout(start_ts) {
                    return Err(SnowflakeError::ExecutionError(
                        anyhow!("Request timed out after {:#?}", self.timeout.unwrap()),
                        None,
                    ));
                }
            }
        }
        Ok(T::new(&internal, &rowset, self))
    }

    async fn await_async_query(
        &self,
        query: &str,
        init_async_query_res: &InternalInitAsyncQueryResult,
        start_ts: Instant,
    ) -> Result<(), SnowflakeError> {
        let query_id = &init_async_query_res.query_id;
        log::debug!("Awaiting async snowflake query '{query_id}'");

        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(500))
            .with_max_interval(Duration::from_secs(5))
            .with_max_elapsed_time(self.get_remaining_timeout(start_ts))
            .build();

        let no_data_counter = AtomicI32::new(0);
        let start_time = Instant::now();

        let request_op = || async {
            let json = self
                .client
                .get(&self.get_monitoring_queries_url(query_id))
                // Monitoring queries uses ACCEPT - JSON. Reqwest client wont' override this.
                .header(ACCEPT, "application/json")
                .build()
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            let body = self
                .client
                .execute(json)
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            let status = body.status();

            let text = body
                .text()
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            self.parse_response_status_with_retry(&status, &text, Some(query_id.clone()))?;

            let res: DataResponse<serde_json::Value> = serde_json::from_str(&text)
                .map_err(|e| {
                    log::error!("Failed to execute monitoring query {query_id} due to deserialization error.");
                    SnowflakeError::new_deserialization_error_with_value(e.into(), text)
                })
                .map_err(backoff::Error::Permanent)?;

            let monitoring_result: InternalMonitoringQueriesResult = serde_json::from_value(res.data.clone())
                .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), res.data.to_string()))
                .map_err(backoff::Error::Permanent)?;

            let query_result = monitoring_result.queries.get(0);
            let query_status = query_result.map_or(QueryStatus::NoData, |res| res.status.clone());

            if !query_status.is_still_running() {
                if query_status == QueryStatus::Success {
                    log::debug!("Async snowflake query '{query_id}' finished.");
                    return Ok(());
                }
                else {
                    log::debug!("Async snowflake query failed with query result {:?}", query_result);
                    let error_result = query_result.map(|qr| {
                        qr.error_result
                            .to_error_result(query_id, &get_query_detail_url(self, query_id))
                    });
                    let message = query_result
                        .map(|qr| qr.error_result.error_message.clone())
                        .unwrap_or_default()
                        .unwrap_or_default();
                    return Err(backoff::Error::Permanent(SnowflakeError::ExecutionError(
                        anyhow!("Failed to execute query '{query}', with reason: {message}"),
                        error_result,
                    )));
                }
            }

            if query_status == QueryStatus::NoData {
                let counter = no_data_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if counter > MAX_NO_DATA_RETRY {
                    return Err(backoff::Error::Permanent(SnowflakeError::ExecutionError(
                        anyhow!("Cannot retrieve data on the status of this query. No information returned from server for query '{query_id}'"),
                    None)));
                }
            }

            // Transient failure until timeout.
            let elapsed = start_time.elapsed();
            return Err(SnowflakeError::ExecutionError(
                anyhow!("Timed out waiting for async snowflake query after '{:?}'", elapsed),
                None,
            )
            .into());
        };

        backoff::future::retry_notify(backoff, request_op, |e, dur| {
            log::warn!("await-async-query operation failed in {:?} with error: {}", dur, e)
        })
        .await
    }

    async fn execute_query_request<T: DeserializeOwned>(
        &self,
        query: &str,
        async_exec: bool,
        start_ts: Instant,
    ) -> Result<T, SnowflakeError> {
        let now = Utc::now();
        let req = QueryRequest {
            async_exec,
            parameters: Some(json!({ "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": Self::result_format() })),
            query_submission_time: now.timestamp_millis(),
            sequence_id: self.sequence_counter.load(Ordering::Relaxed),
            sql_text: query,
        };
        let query_url = self.get_queries_url("query-request");

        // https://github.com/snowflakedb/snowflake-connector-python/blob/f0a38d958c82bf039765faee7050c89d2ccb1d72/src/snowflake/connector/network.py#L791
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_secs(1))
            .with_max_interval(Duration::from_secs(16))
            .with_max_elapsed_time(self.get_remaining_timeout(start_ts))
            .build();

        let request_op = || async {
            let json = self
                .client
                .post(&query_url)
                .json(&req)
                .build()
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            let body = self
                .client
                .execute(json)
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            let status = body.status();

            let text = body
                .text()
                .await
                .map_err(|e| SnowflakeError::ExecutionError(e.into(), None))
                .map_err(backoff::Error::Permanent)?;

            // Handles retries.
            self.parse_response_status_with_retry(&status, &text, None)?;

            Ok(text)
        };

        let text = backoff::future::retry_notify(backoff, request_op, |e, dur| {
            log::warn!("execute-query-request operation failed in {:?} with error: {}", dur, e)
        })
        .await?;

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text).map_err(|e| {
            log::error!("Failed to execute query {query} with URL {query_url} due to deserialization error.");
            SnowflakeError::new_deserialization_error_with_value(e.into(), text)
        })?;

        if !res.success {
            let err = ErrorResult::deserialize(res.data, self)?;
            if let Some(message) = res.message {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query} with reason: {message}"),
                    Some(err),
                ));
            }
            else {
                return Err(SnowflakeError::ExecutionError(
                    anyhow!("Failed to execute query {query}, but no reason was given by Snowflake API"),
                    Some(err),
                ));
            }
        }

        let parsed: T = serde_json::from_value(res.data.clone())
            .map_err(|e| SnowflakeError::new_deserialization_error_with_value(e.into(), res.data.to_string()))?;
        Ok(parsed)
    }

    fn parse_response_status_with_retry(
        &self,
        status: &StatusCode,
        text: &String,
        query_id: Option<String>,
    ) -> Result<(), backoff::Error<SnowflakeError>> {
        if status.is_success() {
            return Ok(());
        }

        let query_detail_url = match &query_id {
            Some(id) => get_query_detail_url(self, id),
            None => "".to_owned(),
        };
        let query_id_str = query_id.unwrap_or_default();

        let err = SnowflakeError::ExecutionError(
            anyhow!("Non-successful response from Snowflake API. Status: {status}."),
            Some(ErrorResult {
                error_type: Some(text.clone()),
                error_code: status.to_string(),
                internal_error: true,
                line: None,
                pos: None,
                query_id: query_id_str,
                query_detail_url,
            }),
        );

        // Transient retry.
        if *status == StatusCode::SERVICE_UNAVAILABLE {
            return Err(err.into());
        }
        return Err(backoff::Error::Permanent(err));
    }

    fn get_queries_url(&self, command: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        let guid = uuid::Uuid::new_v4();
        let url = format!(
            "{}/queries/v1/{command}?requestId={uuid}&request_guid={guid}",
            self.host
        );
        log::debug!("Using query url {url}");
        url
    }

    fn get_monitoring_queries_url(&self, query_id: &String) -> String {
        let encoded_query_id = url_escape::encode_fragment(query_id);
        let url = format!("{}/monitoring/queries/{encoded_query_id}", self.host);
        log::debug!("Using query monitoring {url}");
        url
    }

    fn result_format() -> &'static str {
        if cfg!(feature = "arrow") {
            "ARROW"
        }
        else {
            "JSON"
        }
    }

    fn get_remaining_timeout(&self, start_ts: Instant) -> Option<Duration> {
        self.timeout
            .map(|d| d.checked_sub(start_ts.elapsed()).unwrap_or_default())
    }
}
