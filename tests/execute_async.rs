mod support;

use snowstorm::errors::SnowflakeError;
use snowstorm::responses::result::vec::VecResult;
use support::{common_init, new_full_client, new_valid_client};

#[tokio::test]
async fn execute_async_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client();
    let session = client
        .unwrap()
        .connect()
        .await
        .expect("Session should have been created");
    session
        .execute_async::<VecResult>("SHOW GRANTS")
        .await
        .expect("Result should have been returned");
    Ok(())
}

#[tokio::test]
async fn execute_async_fail() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    match session.execute_async::<VecResult>("INVALID QUERY").await {
        Ok(_) => panic!("Error should've occurred"),
        Err(e) => match e {
            SnowflakeError::ExecutionError(_, r) => {
                let r = r.unwrap();
                assert_eq!(r.error_type.unwrap(), "COMPILATION");
                assert_eq!(r.internal_error, false);
            }
            _ => panic!("SnowflakeError::ExecutionError should've been raised"),
        },
    }

    Ok(())
}

#[tokio::test]
async fn execute_async_select_into_vec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute_async::<VecResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}
