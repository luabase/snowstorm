use log::Level;
use logging_timer::{finish, timer};
use rotenv::dotenv;
use rotenv_codegen::dotenv;
use snowstorm::responses::result::vec::VecResult;
use snowstorm::Snowstorm;

#[tokio::main]
async fn main() {
    dotenv().ok();
    _ = simple_logger::init_with_env();

    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = dotenv!("SNOWFLAKE_PASSWORD");
    let role = dotenv!("SNOWFLAKE_ROLE");
    let database = dotenv!("SNOWFLAKE_DATABASE");
    let schema = dotenv!("SNOWFLAKE_SCHEMA");
    let warehouse = dotenv!("SNOWFLAKE_WAREHOUSE");

    let dsn = format!(
        "snowflake://{user}:{password}@{account}/?role={role}&database={database}&schema={schema}&warehouse={warehouse}"
    );

    let client = Snowstorm::try_new_with_dsn(dsn).unwrap().proxy("http://127.0.0.1:9090");
    let session = client.connect().await.unwrap();

    let query = "SELECT BLOCK_NUMBER, NONCE, TRANSACTION_INDEX, GAS_PRICE, MAX_PRIORITY_FEE_PER_GAS, TRANSACTION_TYPE, STATUS, VALUE, GAS, MAX_FEE_PER_GAS, BLOCK_TIMESTAMP FROM luabase.clickhouse.ethereum_transactions LIMIT 1000000";
    log::info!("Running query {}", query);

    for _ in 0..1 {
        let timer = timer!(Level::Info; "QUERY");
        let res = session.execute::<VecResult>(query).await;
        finish!(timer, "Loaded {} transactions", res.unwrap().rowset.len());
    }
}
