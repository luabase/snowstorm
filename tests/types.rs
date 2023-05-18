mod support;

use snowstorm::responses::{result::vec::VecResult, types::value::Value};
use support::{common_init, new_full_client};

#[tokio::test]
async fn execute_decimal_arrow_test() -> Result<(), anyhow::Error> {
    common_init();

    // THe data for this test is created with:
    //
    // create or replace table compression_test
    // ( num_26_11 number(26, 11) );
    // insert into compression_test values (0.999);

    let client = new_full_client().expect("Client should have been created");
    let session = client.connect().await.expect("Session should have been created");
    let res = session
        .execute::<VecResult>("SELECT * FROM SNOWSTORM_TEST_DATA.PUBLIC.COMPRESSION_TEST")
        .await
        .unwrap();

    assert_eq!(res.rowset.len(), 1);
    let first_row = res.rowset.get(0).unwrap();
    assert_eq!(first_row.len(), 1);
    let col = first_row.get(0).unwrap();
    let nullable_inner = match col {
        Value::Nullable(Some(n)) => n,
        _ => panic!("Unexpected value type"),
    };
    let decimal = match **nullable_inner {
        Value::Decimal(d) => d,
        _ => panic!("Unexpected value type"),
    };
    assert_eq!(decimal.into_parts(), (99900000000, 11, false));

    Ok(())
}