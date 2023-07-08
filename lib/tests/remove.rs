mod parse;

use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::key::bc::Bc;
use surrealdb::key::bd::Bd;
use surrealdb::key::bf::Bf;
use surrealdb::key::bi::Bi;
use surrealdb::key::bk::Bk;
use surrealdb::key::bl::Bl;
use surrealdb::key::bo::Bo;
use surrealdb::key::bp::Bp;
use surrealdb::key::bs::Bs;
use surrealdb::key::bt::Bt;
use surrealdb::key::bu::Bu;
use surrealdb::key::index::Index;
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
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		DEFINE ANALYZER english TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		REMOVE ANALYZER english;
		INFO FOR DB;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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

macro_rules! check_empty_range {
	($tx:expr, $rng:expr) => {{
		let r = $tx.getr($rng, 1).await?;
		assert!(r.is_empty());
	}};
}

macro_rules! check_none_val {
	($tx:expr, $key:expr) => {{
		let r = $tx.get($key).await?;
		assert!(r.is_none());
	}};
}

#[tokio::test]
async fn remove_statement_index() -> Result<(), Error> {
	let sql = "
		DEFINE INDEX uniq_isbn ON book FIELDS isbn UNIQUE;
		DEFINE INDEX idx_author ON book FIELDS author;
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_title ON book FIELDS title SEARCH ANALYZER simple BM25 HIGHLIGHTS;
		CREATE book:1 SET title = 'Rust Web Programming', isbn = '978-1803234694', author = 'Maxwell Flitton';
		REMOVE INDEX uniq_isbn ON book;
		REMOVE INDEX idx_author ON book;
		REMOVE INDEX ft_title ON book;
		INFO FOR TABLE book;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	for _ in 0..8 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	// Check infos output
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			events: {},
			fields: {},
			indexes: {},
			tables: {},
		}",
	);
	assert_eq!(tmp, val);

	let mut tx = dbs.transaction(false, false).await?;
	for ix in ["uniq_isbn", "idx_author", "ft_title"] {
		check_empty_range!(&mut tx, Bc::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bd::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bf::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bi::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bk::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bl::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bo::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bp::range("test", "test", "book", ix));
		check_none_val!(&mut tx, Bs::new("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bt::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Bu::range("test", "test", "book", ix));
		check_empty_range!(&mut tx, Index::range("test", "test", "book", ix));
	}
	Ok(())
}
