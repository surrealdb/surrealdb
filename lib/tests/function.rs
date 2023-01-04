mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn function_array_combine() -> Result<(), Error> {
	let sql = r#"
		RETURN array::combine([], []);
		RETURN array::combine(3, true);
		RETURN array::combine([1,2], [2,3]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ [1,2], [1,3], [2,2], [2,3] ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_complement() -> Result<(), Error> {
	let sql = r#"
		RETURN array::complement([], []);
		RETURN array::complement(3, true);
		RETURN array::complement([1,2,3,4], [3,4,5,6]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_concat() -> Result<(), Error> {
	let sql = r#"
		RETURN array::concat([], []);
		RETURN array::concat(3, true);
		RETURN array::concat([1,2,3,4], [3,4,5,6]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,3,4,5,6]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_difference() -> Result<(), Error> {
	let sql = r#"
		RETURN array::difference([], []);
		RETURN array::difference(3, true);
		RETURN array::difference([1,2,3,4], [3,4,5,6]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,5,6]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_distinct() -> Result<(), Error> {
	let sql = r#"
		RETURN array::distinct([]);
		RETURN array::distinct("some text");
		RETURN array::distinct([1,2,1,3,3,4]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_flatten() -> Result<(), Error> {
	let sql = r#"
		RETURN array::flatten([]);
		RETURN array::flatten("some text");
		RETURN array::flatten([[1,2], [3,4]]);
		RETURN array::flatten([[1,2], [3, 4], 'SurrealDB', [5, 6, [7, 8]]]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1, 2, 3, 4, 'SurrealDB', 5, 6, [7, 8]]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_insert() -> Result<(), Error> {
	let sql = r#"
		RETURN array::insert([], 1);
		RETURN array::insert([3], 1, 1);
		RETURN array::insert([1,2,3,4], 5, -1);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,1]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,5,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_intersect() -> Result<(), Error> {
	let sql = r#"
		RETURN array::intersect([], []);
		RETURN array::intersect(3, true);
		RETURN array::intersect([1,2,3,4], [3,4,5,6]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_union() -> Result<(), Error> {
	let sql = r#"
		RETURN array::union([], []);
		RETURN array::union(3, true);
		RETURN array::union([1,2,1,6], [1,3,4,5,6]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,6,3,4,5]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_slice() -> Result<(), Error> {
	let sql = r#"
		RETURN string::slice("the quick brown fox jumps over the lazy dog.");
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, 60);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", -9, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", -100, -100);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog.'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'the quick brown fox jumps over the lazy dog'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'fox jumps over the lazy dog'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'lazy dog'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("''");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_not() -> Result<(), Error> {
	let sql = r#"
		RETURN not(true);
		RETURN not(not(true));
		RETURN not(false);
		RETURN not(not(false));
		RETURN not(0);
		RETURN not(1);
		RETURN not("hello");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::True;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::False;
	assert_eq!(tmp, val);
	//
	Ok(())
}
