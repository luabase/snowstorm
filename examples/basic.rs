use rotenv_codegen::dotenv;
use snowstorm::{responses::result::vec::VecResult, Snowstorm};

#[tokio::main]
async fn main() {
    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = dotenv!("SNOWFLAKE_PASSWORD");

    let client = Snowstorm::new(account.into(), user.into(), password.into());
    let session = client.connect().await.unwrap();
    _ = session.execute::<VecResult>("USE ROLE ACCOUNTADMIN").await;
    _ = session.execute::<VecResult>("USE DATABASE LUABASE").await;
    _ = session.execute::<VecResult>("USE SCHEMA CLICKHOUSE").await;
    _ = session.execute::<VecResult>("USE WAREHOUSE IMPORT_TEST").await;

    let res = session.execute::<VecResult>("SHOW GRANTS").await.unwrap();
    for row in res.rowset.into_iter() {
        println!("{row:?}");
    }
}
