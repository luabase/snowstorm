mod support;

use std::time::Duration;

use snowstorm::errors::SnowflakeError;
use snowstorm::responses::{
    result::{hashmap::HashMapResult, jsonmap::JsonMapResult, jsonvec::JsonVecResult, vec::VecResult},
    types::value::Value,
};
use support::{common_init, new_full_client, new_valid_client};

#[tokio::test]
async fn execute_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    session
        .execute::<VecResult>("SHOW GRANTS")
        .await
        .expect("Result should have been returned");
    Ok(())
}

#[tokio::test]
async fn execute_fail() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_valid_client();
    let session = client.connect().await.expect("Session should have been created");
    match session.execute::<VecResult>("INVALID QUERY").await {
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
async fn execute_select_into_vec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<VecResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_hashmap_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<HashMapResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_jsonmap_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<JsonMapResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), res.total);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_jsonvec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<JsonVecResult>("SELECT * FROM LUABASE.CLICKHOUSE.TYPES_TEST")
        .await
        .unwrap();
    assert_eq!(res.rowset.len(), res.total);
    log::info!("{:?}", res);
    Ok(())
}

#[tokio::test]
async fn execute_select_into_chunked_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<VecResult>("SELECT * FROM LUABASE.CLICKHOUSE.ETHEREUM_TRANSACTIONS LIMIT 2000")
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

#[tokio::test]
async fn execute_select_ordered_into_vec_success() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client()
        .expect("Client should have been created")
        .max_parallel_downloads(25);
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<VecResult>(
            "SELECT BLOCK_NUMBER FROM LUABASE.CLICKHOUSE.ETHEREUM_TRANSACTIONS ORDER BY BLOCK_NUMBER ASC LIMIT 100000",
        )
        .await
        .unwrap();

    let mut prev = None;
    for row in &res.rowset {
        let block_num = match row.get(0).expect("Query should return rows.") {
            Value::Nullable(Some(val)) => match **val {
                Value::Integer(i) => i,
                _ => panic!("Non integer block number."),
            },
            _ => panic!("Non nullable block number."),
        };

        match prev {
            Some(prev_block_num) => assert!(prev_block_num <= block_num),
            None => {}
        }

        prev = Some(block_num);
    }
    Ok(())
}

#[tokio::test]
async fn execute_timeout() -> Result<(), anyhow::Error> {
    common_init();

    let client = new_full_client()
        .expect("Client should have been created")
        .timeout(Duration::from_secs(1));
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<VecResult>(
            "SELECT BLOCK_NUMBER FROM LUABASE.CLICKHOUSE.ETHEREUM_TRANSACTIONS ORDER BY BLOCK_NUMBER ASC LIMIT 100000",
        )
        .await;

    assert_err!(res, Err(SnowflakeError::ExecutionError(_, _)));
    Ok(())
}
