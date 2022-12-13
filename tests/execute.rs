mod support;

use snowstorm::{errors::SnowflakeError, responses::query::HashMapResult, responses::query::VecResult};
use support::{common_init, new_full_client, new_valid_client};

#[tokio::test]
async fn execute_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    session.execute::<VecResult>("SHOW GRANTS").await.expect("Result should have been returned");
    Ok(())
}

#[tokio::test]
async fn execute_select_into_vec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<VecResult>("SELECT * FROM ethereum_transactions LIMIT 10").await.unwrap();
    assert_eq!(res.rowset.len(), 10);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_hashmap_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<HashMapResult>("SELECT * FROM ethereum_transactions LIMIT 10").await.unwrap();
    assert_eq!(res.rowset.len(), 10);
    Ok(())
}

#[tokio::test]
async fn execute_error() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    let result = session.execute::<VecResult>("INVALID STATEMENT").await;
    assert_err!(result, Err(SnowflakeError::ExecutionError(_)));
    Ok(())
}
