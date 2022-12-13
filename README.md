# Snowstorm
Snowflake Connector for Rust

## Usage

Create a new client and establish a session:

```rust
#[tokio::main]
async fn main() {
    let client = Snowstorm::new("account_id.us-central1.gcp".into(), "my_user".into(), "very_secure_password".into());
    let session = client.connect().await.unwrap();
    _ = session.execute::<VecResult>("USE ROLE ACCOUNTADMIN").await;
    _ = session.execute::<VecResult>("USE DATABASE LUABASE").await;
    _ = session.execute::<VecResult>("USE SCHEMA CLICKHOUSE").await;
    _ = session.execute::<VecResult>("USE WAREHOUSE IMPORT_TEST").await;
}
```

or

```rust
#[tokio::main]
async fn main() {
    let dsn = "snowflake://my_user:very_secure_password@my_account_id.us-central1.gcp/?role=ACCOUNTADMIN&database=LUABASE&schema=CLICKHOUSE&warehouse=IMPORT_TEST";
    let client = Snowstorm::try_new_with_dsn(dsn.into()).unwrap();
    let session = client.connect().await.unwrap();
}
```

Execute queries using the session created above:

```rust
let res = session.execute::<HashMapResult>("SELECT * FROM ethereum_transactions LIMIT 10").await.unwrap();
for row in res.rowset.into_iter() {
    println!("{:?}", row);
}
```
