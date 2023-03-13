use rotenv_codegen::dotenv;
use snowstorm::{responses::result::vec::VecResult, Snowstorm};

#[tokio::main]
async fn main() {
    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = dotenv!("SNOWFLAKE_PASSWORD");
    let role = dotenv!("SNOWFLAKE_ROLE");
    let database = dotenv!("SNOWFLAKE_DATABASE");
    let schema = dotenv!("SNOWFLAKE_SCHEMA");
    let warehouse = dotenv!("SNOWFLAKE_WAREHOUSE");

    let client = Snowstorm::new(account.into(), user.into(), password.into());
    let session = client.connect().await.unwrap();
    _ = session.execute::<VecResult>(&format!("USE ROLE {role}")).await;
    _ = session.execute::<VecResult>(&format!("USE DATABASE {database}")).await;
    _ = session.execute::<VecResult>(&format!("USE SCHEMA {schema}")).await;
    _ = session
        .execute::<VecResult>(&format!("USE WAREHOUSE {warehouse}"))
        .await;

    let res = session.execute::<VecResult>("SHOW GRANTS").await.unwrap();
    for row in res.rowset.into_iter() {
        println!("{row:?}");
    }
}
