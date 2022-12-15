use rotenv::dotenv;
use rotenv_codegen::dotenv;
use simple_logger;
use snowstorm::{Snowstorm, errors::SnowflakeError, responses::query::VecResult};

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
        "snowflake://{}:{}@{}/?role={}&database={}&schema={}&warehouse={}",
        user, password, account, role, database, schema, warehouse
    );

    let client = Snowstorm::try_new_with_dsn(dsn.into()).unwrap();
    let session = client.connect().await.unwrap();
    let res = session.execute::<VecResult>("SELECT * FROM ethereum_transactions LIMIT 10").await;

    match res {
        Ok(r) => {
            for row in r.rowset.into_iter() {
                println!("{:?}", row);
            }
        },
        Err(e) => {
            match e {
                SnowflakeError::ExecutionError(msg, details) => {
                    println!("Error: {:?}", msg);
                    println!("Detail: {:?}", details);
                },
                _ => println!("Error: {:?}", e)
            }

        }
    }
}
