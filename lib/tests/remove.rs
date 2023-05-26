mod parse;

use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn remove_statement_table() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE test SCHEMALESS;
		REMOVE TABLE test;
		INFO FOR DB;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: {}
		}",
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn remove_statement_analyzer() -> Result<(), Error> {
	let sql = "
		DEFINE ANALYZER english TOKENIZERS space,case FILTERS lowercase,snowball(english);
		REMOVE ANALYZER english;
		INFO FOR DB;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	// Analyzer is defined
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// Analyzer is removed
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			analyzers: {},
			logins: {},
			tokens: {},
			functions: {},
			params: {},
			scopes: {},
			tables: {}
		}",
	);
	assert_eq!(tmp, val);
	Ok(())
}

///
/// Test REMOVE USER statement
///

#[tokio::test]
async fn remove_statement_user_kv() -> Result<(), Error> {
	let sql = "
		DEFINE USER test ON KV PASSWORD 'test';
		
		INFO FOR USER test;

		REMOVE USER test ON KV;

		INFO FOR USER test;
	";

	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;

	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert_eq!(res[3].result.as_ref().unwrap_err().to_string(), "The root user 'test' does not exist"); // User was successfully deleted
	Ok(())
}

#[tokio::test]
async fn remove_statement_user_ns() -> Result<(), Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

	// Create a NS user and remove it.
	let sql = "
	USE NS ns;
	DEFINE USER test ON NS PASSWORD 'test';
	INFO FOR USER test ON NS;

	REMOVE USER test ON NS;
	INFO FOR USER test ON NS;
	";
	
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert_eq!(res[4].result.as_ref().unwrap_err().to_string(), "The namespace user 'test' does not exist in 'ns'"); // User was successfully deleted
	
	// If it tries to remove a NS user without specifying a NS, it should fail.
	let sql = [
		"USE NS ns;
		DEFINE USER test ON NS PASSWORD 'test';",

		"REMOVE USER test ON NS;",
	];

	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[1].result.is_ok());
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[1], &ses, None, false).await?;
	assert_eq!(res[0].result.as_ref().unwrap_err().to_string(), "Specify a namespace to use"); // NS was not specified


	Ok(())
}

#[tokio::test]
async fn remove_statement_user_db() -> Result<(), Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

	// Create a DB user and remove it.
	let sql = "
	USE NS ns;
	USE DB db;
	DEFINE USER test ON DB PASSWORD 'test';
	INFO FOR USER test ON DB;

	REMOVE USER test ON DB;
	INFO FOR USER test ON DB;
	";
	
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert_eq!(res[5].result.as_ref().unwrap_err().to_string(), "The database user 'test' does not exist in 'db'"); // User was successfully deleted
	
	// If it tries to remove a DB user without specifying a DB, it should fail.
	let sql = [
		"USE NS ns;
		USE DB db;
		DEFINE USER test ON DB PASSWORD 'test';",

		"USE NS ns; REMOVE USER test ON DB;",
	];

	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[2].result.is_ok());
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[1], &ses, None, false).await?;
	assert_eq!(res[1].result.as_ref().unwrap_err().to_string(), "Specify a database to use"); // DB was not specified


	Ok(())
}

#[tokio::test]
async fn remove_statement_user_check_permissions_kv() -> Result<(), Error> {
	//
	// Check permissions using a KV session
	//

	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';",
		
		// Remove users
		"REMOVE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		REMOVE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		REMOVE USER test_db ON DB PASSWORD 'test';"
	];
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();

	// Create users
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	// Remove users
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[1], &ses, None, false).await?;
	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_check_permissions_ns() -> Result<(), Error> {
	//
	// Check permissions using a NS session
	//
	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';",
		
		// Remove users
		"REMOVE USER test_kv ON KV;
		
		USE NS ns;
		REMOVE USER test_ns ON NS;
		
		USE NS ns;
		USE DB db;
		REMOVE USER test_db ON DB;"
	];
	// Prepare datastore
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	// Remove users with the NS session
	let ses = Session::for_ns("ns");
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[1], &ses, None, false).await?;
	assert_eq!(res[0].result.as_ref().unwrap_err().to_string(), "You don't have permission to perform this query type"); // NS users can't remove KV users
	assert!(res[1].result.is_ok());
	assert_eq!(res[2].result.as_ref().unwrap_err().to_string(), "You don't have permission to perform this query type"); // NS users can't remove NS users
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	Ok(())
}

#[tokio::test]
async fn define_statement_user_check_permissions_db() -> Result<(), Error> {
	//
	// Check permissions using a DB session
	//
	let sql = [
		// Create users
		"DEFINE USER test_kv ON KV PASSWORD 'test';
		
		USE NS ns;
		DEFINE USER test_ns ON NS PASSWORD 'test';
		
		USE NS ns;
		USE DB db;
		DEFINE USER test_db ON DB PASSWORD 'test';
		",
		
		// Remove users
		"REMOVE USER test_kv ON KV;
		
		USE NS ns;
		REMOVE USER test_ns ON NS;
		
		USE NS ns;
		USE DB db;
		REMOVE USER test_db ON DB;
		"
	];
	// Prepare datastore
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv();
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[0], &ses, None, false).await?;
	assert!(res[0].result.is_ok());
	assert!(res[1].result.is_ok());
	assert!(res[2].result.is_ok());
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert!(res[5].result.is_ok());

	// Remove users with the NS session
	let ses = Session::for_db("ns", "db");
	let res: &mut Vec<surrealdb::dbs::Response> = &mut dbs.execute(&sql[1], &ses, None, false).await?;
	assert_eq!(res[0].result.as_ref().unwrap_err().to_string(), "You don't have permission to perform this query type"); // DB users can't remove KV users
	assert!(res[1].result.is_ok());
	assert_eq!(res[2].result.as_ref().unwrap_err().to_string(), "You don't have permission to perform this query type"); // DB users can't remove NS users
	assert!(res[3].result.is_ok());
	assert!(res[4].result.is_ok());
	assert_eq!(res[5].result.as_ref().unwrap_err().to_string(), "You don't have permission to perform this query type"); // DB users can't remove DB users

	Ok(())
}
