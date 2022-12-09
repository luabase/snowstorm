mod support;

use rotenv_codegen::dotenv;
use snowstorm::{errors::SnowflakeError, Snowstorm};
use support::{common_init, new_full_client, new_valid_client};

#[tokio::test]
async fn authenticate_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    client.connect().await.expect("Session should have been created");
    Ok(())
}

#[tokio::test]
async fn authenticate_with_dsn_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    client.connect().await.expect("Session should have been created");

    Ok(())
}

#[tokio::test]
async fn authenticate_fail() -> Result<(), anyhow::Error> {
    common_init();

    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = "wrong_password";

    let client = Snowstorm::new(account.into(), user.into(), password.into());
    let session = client.connect().await;
    assert_err!(session, Err(SnowflakeError::AuthenticationError(_)));
    Ok(())
}
