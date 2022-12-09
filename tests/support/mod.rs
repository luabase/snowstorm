use rotenv::dotenv;
use rotenv_codegen::dotenv;
use simple_logger;
use snowstorm::{errors::SnowflakeError, Snowstorm};

pub fn common_init() {
    dotenv().ok();
    _ = simple_logger::init_with_env();
}

pub fn new_valid_client() -> Snowstorm {
    let account = dotenv!("SNOWFLAKE_ACCOUNT");
    let user = dotenv!("SNOWFLAKE_USER");
    let password = dotenv!("SNOWFLAKE_PASSWORD");

    let client = Snowstorm::new(account.into(), user.into(), password.into());
    client
}

pub fn new_full_client() -> Result<Snowstorm, SnowflakeError> {
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

    let client = Snowstorm::try_new_with_dsn(dsn);
    client
}

#[macro_export]
macro_rules! assert_err {
    ($expression:expr, $($pattern:tt)+) => {
        match $expression {
            $($pattern)+ => (),
            ref e => panic!("expected `{}` but got `{:?}`", stringify!($($pattern)+), e),
        }
    }
}
