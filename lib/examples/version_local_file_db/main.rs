//To use the local engine you will need to add the local engine feature to Cargo.toml 
//eg to use the latest from github (until beta 9 is released):

//    surrealdb = { git = "https://github.com/surrealdb/surrealdb", features = ["kv-rocksdb"] }
	
// NB: currently (pre beta 9) you cannot use cargo to add the feature:
// cargo add surrealdb -F kv-rocksdb #does not work

// To use the RocksDB You will need to ensure that your Operating System has "clang" installed. 

use surrealdb::engine::local::Db;
use surrealdb::Surreal;

// using a global static for the DataBase Connection is recommended 
// NB the SurrealDB library has an internal Database Connection pool
pub static DB: Surreal<Db> = Surreal::init();

#[tokio::main]
async fn main() -> surrealdb::Result<()> {
	let version = DB.version().await?;

	println!("{version:?}");

	Ok(())
}
