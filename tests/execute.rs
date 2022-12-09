mod support;

use snowstorm::errors::SnowflakeError;
use support::{common_init, new_full_client, new_valid_client};

#[tokio::test]
async fn execute_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    session.execute("SHOW GRANTS").await.expect("Result should have been returned");
    Ok(())
}

#[tokio::test]
async fn execute_select_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute("SELECT * FROM ethereum_transactions LIMIT 10").await.unwrap();
    assert_eq!(res.rowset.len(), 10);
    Ok(())
}

#[tokio::test]
async fn execute_error() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    let result = session.execute("INVALID STATEMENT").await;
    assert_err!(result, Err(SnowflakeError::ExecutionError(_)));
    Ok(())
}
