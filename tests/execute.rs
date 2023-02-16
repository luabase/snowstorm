mod support;

use snowstorm::errors::SnowflakeError;
use snowstorm::responses::result::{hashmap::HashMapResult, jsonmap::JsonMapResult, vec::VecResult, jsonvec::JsonVecResult};
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
async fn execute_fail() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    match session.execute::<VecResult>("INVALID QUERY").await {
        Ok(_) => panic!("Error should've occurred"),
        Err(e) => {
            match e {
                SnowflakeError::ExecutionError(_, r) => {
                    let r = r.unwrap();
                    assert_eq!(r.error_type.unwrap(), "COMPILATION");
                    assert_eq!(r.internal_error, false);
                }
                _ => panic!("SnowflakeError::ExecutionError should've been raised")
            }

        }
    }

    Ok(())
}

#[tokio::test]
async fn execute_select_into_vec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<VecResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST").await.unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_hashmap_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<HashMapResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST").await.unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_jsonmap_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<JsonMapResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST").await.unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_jsonvec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<JsonVecResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST").await.unwrap();
    assert_eq!(res.rowset.len(), res.total);
    log::info!("{:?}", res);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_chunked_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session.execute::<VecResult>("SELECT * FROM LUABASE.CLICKHOUSE.ETHEREUM_TRANSACTIONS LIMIT 2000")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), 2000);
    Ok(())
}

#[tokio::test]
async fn execute_error() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    let result = session.execute::<VecResult>("INVALID STATEMENT").await;
    assert_err!(result, Err(SnowflakeError::ExecutionError(_, _)));
    Ok(())
}
