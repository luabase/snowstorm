# Snowstorm
Snowflake Connector for Rust

## Usage

Create a new client and establish a session:

```rust
#[tokio::main]
async fn main() {
    let client = Snowstorm::new("account_id.us-central1.gcp".into(), "my_user".into(), "very_secure_password".into());
    let session = client.connect().await.unwrap();
    _ = session.execute::<VecResult>("USE ROLE MY_ROLE").await;
    _ = session.execute::<VecResult>("USE DATABASE SOME_DATABASE").await;
    _ = session.execute::<VecResult>("USE SCHEMA COOL_SCHEMA").await;
    _ = session.execute::<VecResult>("USE WAREHOUSE MASTER_WAREHOUSE").await;
}
```

or

```rust
#[tokio::main]
async fn main() {
    let dsn = "snowflake://my_user:very_secure_password@my_account_id.us-central1.gcp/?role=MY_ROLE&database=SOME_DATABASE&schema=COOL_SCHEMA&warehouse=MASTER_WAREHOUSE";
    let client = Snowstorm::try_new_with_dsn(dsn.into()).unwrap();
    let session = client.connect().await.unwrap();
}
```

Execute queries using the session created above:

```rust
let res = session.execute::<HashMapResult>("SELECT * FROM cool_schema LIMIT 10").await.unwrap();
for row in res.rowset.into_iter() {
    println!("{:?}", row);
}
```
