use rotenv::dotenv;
use rotenv_codegen::dotenv;
use snowstorm::{Snowstorm, errors::SnowflakeError};
use snowstorm::responses::result::hashmap::HashMapResult;
use snowstorm::responses::types::value::Value;

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

    let client = Snowstorm::try_new_with_dsn(dsn).unwrap();
    let session = client.connect().await.unwrap();

    let tables = vec!["TEST", "NUMBER_TEST", "TIME_TEST", "TIMESTAMP_NTZ_TEST", "TIMESTAMP_TZ_TEST", "TIMESTAMP_LTZ_TEST"];

    for table in tables {
        let query = format!("SELECT * FROM snowstorm_test_data.public.{table}");
        println!("+++ Running query {query}");

        let res = session.execute::<HashMapResult>(&query).await;

        match res {
            Ok(r) => {
                for row in r.rowset.into_iter() {
                    let mut vec: Vec<(String, Value)> = row.into_iter().collect();
                    vec.sort_by_key(|k| k.0.clone());
                    for kv in vec.iter() {
                        println!("{}: {}", kv.0, kv.1);
                    }
                    println!("---");
                }
            },
            Err(e) => {
                match e {
                    SnowflakeError::ExecutionError(msg, details) => {
                        println!("Error: {msg}");
                        println!("Detail: {:?}", details);
                    },
                    _ => println!("Error: {e}")
                }

            }
        }
    }
}
