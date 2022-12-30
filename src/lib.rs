pub mod errors;
pub mod responses;
pub mod requests;
pub mod session;

mod utils;

use anyhow::anyhow;
use errors::SnowflakeError;
use requests::{DataRequest, LoginRequest};
use responses::result::vec::VecResult;
use responses::types::{data::DataResponse, login::LoginResponse};
use serde_json::json;
use session::Session;
use std::collections::HashMap;
use reqwest::Url;
use reqwest::header::{HeaderMap, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use utils::urldecode_some;


#[derive(Debug)]
pub struct Snowstorm {
    // Required properties
    pub account: String,
    pub password: String,
    pub user: String,

    // Optional properties
    role: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    warehouse: Option<String>,

    // Optional settings
    proxy: Option<String>
}

impl Snowstorm {

    /// Creates a minimal client instance able to connect to Snowflake.
    pub fn new(
        account: String,
        user: String,
        password: String
    ) -> Self {
        Snowstorm {
            account,
            password,
            user,
            role: None,
            database: None,
            schema: None,
            warehouse: None,
            proxy: None
        }
    }

    pub fn proxy(mut self, address: &str) -> Self {
        self.proxy = Some(address.to_owned());
        self
    }

    /// Creates a client instance using a DSN string.
    ///
    /// DSN should be in the following format:
    /// snowflake://{user}:{password}@{account}/?role={role}&database={database}&schema={schema}&warehouse={warehouse}
    pub fn try_new_with_dsn(dsn: String) -> Result<Self, SnowflakeError> {
        let url = Url::parse(&dsn)
            .map_err(|e| SnowflakeError::GeneralError(e.into()))?;

        if url.scheme() != "snowflake" {
            return Err(SnowflakeError::GeneralError(anyhow!("Invalid proto {}, expected 'snowflake'", url.scheme())));
        }

        let user = urldecode_some(Some(url.username()));
        if user.is_empty() {
            return Err(SnowflakeError::GeneralError(anyhow!("Username is required, but missing from DSN")));
        }

        let password = urldecode_some(url.password());
        if password.is_empty() {
            return Err(SnowflakeError::GeneralError(anyhow!("Password is required, but missing from DSN")));
        }

        let account = urldecode_some(url.host_str());
        if account.is_empty() {
            return Err(SnowflakeError::GeneralError(anyhow!("Account is required, but missing from DSN")));
        }

        let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
        let role = query.get("role").map(|x| x.to_owned());
        let database = query.get("database").map(|x| x.to_owned());
        let schema = query.get("schema").map(|x| x.to_owned());
        let warehouse = query.get("warehouse").map(|x| x.to_owned());

        Ok(Snowstorm {
            account,
            password,
            user,
            role,
            database,
            schema,
            warehouse,
            proxy: None
        })
    }

    /// Creates a connection to Snowflake.
    ///
    /// Assumes default role, database, schema and warehouse if specified.
    pub async fn connect(&self) -> Result<Session, SnowflakeError> {
        let headers = Snowstorm::get_headers(None)
            .map_err(SnowflakeError::GeneralError)?;

        let mut builder = reqwest::Client::builder()
            .default_headers(headers);

        if let Some(proxy) = &self.proxy {
            builder = builder.proxy(reqwest::Proxy::https(proxy).unwrap());
        }

        let client = builder
            .build()
            .map_err(|e| SnowflakeError::GeneralError(e.into()))?;

        let (account_name, region) = &self.account.split_once('.').unwrap_or((&self.account, ""));

        let req = DataRequest {
            data: LoginRequest {
                account_name,
                login_name: &self.user,
                password: &self.password,
                client_app_id: "PythonConnector",
                client_app_version: "2.9.0",
                session_parameters: Some(json!({
                    "TIMEZONE": "Etc/GMT",
                    "CLIENT_PREFETCH_THREADS": 4
                }))
            }
        };

        let body = client
            .post(&self.get_session_url("login-request"))
            .json(&req)
            .send().await
            .map_err(|e| SnowflakeError::AuthenticationError(e.into()))?;

        let text = body
            .text().await
            .map_err(|e| SnowflakeError::AuthenticationError(e.into()))?;

        let res: DataResponse<serde_json::Value> = serde_json::from_str(&text)
            .map_err(|e| {
                log::error!("Failed to authenticate due to deserialization error.");
                SnowflakeError::new_deserialization_error_with_value(e.into(), text.to_owned())
            })?;

        if !res.success {
            if let Some(message) = res.message {
                return Err(SnowflakeError::AuthenticationError(anyhow!(message)));
            }
            else {
                return Err(SnowflakeError::AuthenticationError(
                    anyhow!("Failed to authenticate, but no reason was given by Snowflake API")
                ));
            }
        }

        let data: LoginResponse = serde_json::from_value(res.data)
            .map_err(|e| {
                log::error!(
                    "Failed to authenticate due to data deserialization error."
                );
                SnowflakeError::new_deserialization_error_with_value(e.into(), text.to_owned())
            })?;

        let session_headers = Snowstorm::get_headers(Some(data.token.as_str()))
            .map_err(SnowflakeError::GeneralError)?;

        let mut session_builder = reqwest::Client::builder()
            .gzip(true)
            .deflate(true)
            .default_headers(session_headers);

        if let Some(proxy) = &self.proxy {
            session_builder = session_builder.proxy(reqwest::Proxy::https(proxy).unwrap());
        }

        let session_client = session_builder
            .build()
            .map_err(|e| SnowflakeError::GeneralError(e.into()))?;

        let session = Session::new(
            session_client,
            &self.get_host(),
            account_name,
            (!region.is_empty()).then_some(*region)
        );

        if let Some(role) = &self.role {
            _ = session.execute::<VecResult>(&format!("USE ROLE {role}")).await?
        }

        if let Some(database) = &self.database {
            _ = session.execute::<VecResult>(&format!("USE DATABASE {database}")).await?
        }

        if let Some(schema) = &self.schema {
            _ = session.execute::<VecResult>(&format!("USE SCHEMA {schema}")).await?
        }

        if let Some(warehouse) = &self.warehouse {
            _ = session.execute::<VecResult>(&format!("USE WAREHOUSE {warehouse}")).await?
        }

        Ok(session)
    }

    #[inline]
    fn get_host(&self) -> String {
        format!("https://{}.snowflakecomputing.com", &self.account)
    }

    fn get_session_url(&self, command: &str) -> String {
        let uuid = uuid::Uuid::new_v4();
        let guid = uuid::Uuid::new_v4();
        let url = format!("{}/session/v1/{command}?request_id={uuid}&request_guid={guid}", self.get_host());
        log::debug!("Using session url {url}");
        url
    }

    fn get_headers(token: Option<&str>) -> Result<HeaderMap, anyhow::Error> {
        let mut headers = HeaderMap::with_capacity(4);
        headers.append(ACCEPT, "application/snowflake".parse()?);
        headers.append(AUTHORIZATION, format!("Snowflake Token=\"{}\"", token.unwrap_or("None")).parse()?);
        headers.append(CONTENT_TYPE, "application/json".parse()?);
        headers.append(USER_AGENT, concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION")).parse()?);
        Ok(headers)
    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_dsn_builder() -> Result<(), anyhow::Error> {
        Snowstorm::try_new_with_dsn("fail://".to_owned())
            .expect_err("Should have failed due to invalid scheme");

        Snowstorm::try_new_with_dsn("snowflake://host".to_owned())
            .expect_err("Should have failed due to missing username");

        Snowstorm::try_new_with_dsn("snowflake://user@account".to_owned())
            .expect_err("Should have failed due to missing password");


        let user = "test_user";
        let password = "test_password@%_$%";
        let account = "test_account.region.platform";
        let role = "my_role";
        let schema = "my_schema";
        let database = "my_database";
        let warehouse = "my_warehouse";

        let dsn = format!(
            "snowflake://{}:{}@{}/?role={}&database={}&schema={}&warehouse={}",
            user, password, account, role, database, schema, warehouse
        );
        let client = Snowstorm::try_new_with_dsn(dsn).expect("Client should have been created");

        assert_eq!(client.user, user);
        assert_eq!(client.password, password);
        assert_eq!(client.account, account);
        assert_eq!(client.role, Some(role.to_owned()));
        assert_eq!(client.schema, Some(schema.to_owned()));
        assert_eq!(client.database, Some(database.to_owned()));
        assert_eq!(client.warehouse, Some(warehouse.to_owned()));

        Ok(())
    }

}
