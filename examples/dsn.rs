use rotenv_codegen::dotenv;
use snowstorm::Snowstorm;

#[tokio::main]
async fn main() {
    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = dotenv!("SNOWFLAKE_PASSWORD");
    let role = dotenv!("SNOWFLAKE_ROLE");
    let database = dotenv!("SNOWFLAKE_DATABASE");
    let schema = dotenv!("SNOWFLAKE_SCHEMA");
    let warehouse = dotenv!("SNOWFLAKE_WAREHOUSE");

    let dsn = format!(
        "snowflake://{}:{}@{}/?role={}&database={}&schema={}&warehouse={}",
        user, password, account, role, database, schema, warehouse
    );

    let client = Snowstorm::try_new_with_dsn(dsn.into()).unwrap();
    let session = client.connect().await.unwrap();
    let res = session.execute("SELECT * FROM ethereum_transactions LIMIT 10").await.unwrap();
    for row in res.rowset.into_iter() {
        println!("{:?}", row);
    }
}
