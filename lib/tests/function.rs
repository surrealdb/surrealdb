mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

// --------------------------------------------------
// array
// --------------------------------------------------

#[tokio::test]
async fn function_array_all() -> Result<(), Error> {
	let sql = r#"
		RETURN array::all([]);
		RETURN array::all("some text");
		RETURN array::all([1,2,"text",3,NONE,3,4]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_any() -> Result<(), Error> {
	let sql = r#"
		RETURN array::any([]);
		RETURN array::any("some text");
		RETURN array::any([1,2,"text",3,NONE,3,4]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::any(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::combine(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::complement(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::concat(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::difference(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::distinct(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::flatten(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
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
async fn function_array_group() -> Result<(), Error> {
	let sql = r#"
		RETURN array::group([]);
		RETURN array::group(3);
		RETURN array::group([ [1,2,3,4], [3,4,5,6] ]);
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::group(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,5,6]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_insert() -> Result<(), Error> {
	let sql = r#"
		RETURN array::insert([], 1);
		RETURN array::insert([3], 1, 5);
		RETURN array::insert([3], 1, 1);
		RETURN array::insert([1,2,3,4], 5, -1);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::intersect(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_max() -> Result<(), Error> {
	let sql = r#"
		RETURN array::max([]);
		RETURN array::max("some text");
		RETURN array::max([1,2,"text",3,3,4]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::max(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'text'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_min() -> Result<(), Error> {
	let sql = r#"
		RETURN array::min([]);
		RETURN array::min("some text");
		RETURN array::min([1,2,"text",3,3,4]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::min(). Argument 1 was the wrong type. Expected a array but failed to convert 'some text' into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_remove() -> Result<(), Error> {
	let sql = r#"
		RETURN array::remove([3], 0);
		RETURN array::remove([3], 2);
		RETURN array::remove([3,4,5], 1);
		RETURN array::remove([1,2,3,4], -1);
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
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,5]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3]");
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
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Incorrect arguments for function array::union(). Argument 1 was the wrong type. Expected a array but failed to convert 3 into a array"
	));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,6,3,4,5]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// count
// --------------------------------------------------

#[tokio::test]
async fn function_count() -> Result<(), Error> {
	let sql = r#"
		RETURN count();
		RETURN count(true);
		RETURN count(false);
		RETURN count(15 > 10);
		RETURN count(15 < 10);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// crypto
// --------------------------------------------------

#[tokio::test]
async fn function_crypto_md5() -> Result<(), Error> {
	let sql = r#"
		RETURN crypto::md5('tobie');
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("4768b3fc7ac751e03a614e2349abf3bf");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_crypto_sha1() -> Result<(), Error> {
	let sql = r#"
		RETURN crypto::sha1('tobie');
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("c6be709a1b6429472e0c5745b411f1693c4717be");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_crypto_sha256() -> Result<(), Error> {
	let sql = r#"
		RETURN crypto::sha256('tobie');
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("33fe1859daba927ea5674813adc1cf34b9e2795f2b7e91602fae19c0d0c493af");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_crypto_sha512() -> Result<(), Error> {
	let sql = r#"
		RETURN crypto::sha512('tobie');
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("39f0160c946c4c53702112d6ef3eea7957ea8e1c78787a482a89f8b0a8860a20ecd543432e4a187d9fdcd1c415cf61008e51a7e8bf2f22ac77e458789c9cdccc");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// duration
// --------------------------------------------------

#[tokio::test]
async fn function_duration_days() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::days(7d);
		RETURN duration::days(4w3d);
		RETURN duration::days(4h);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(7);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(31);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_hours() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::hours(7h);
		RETURN duration::hours(4d3h);
		RETURN duration::hours(30m);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(7);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(99);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_micros() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::micros(150µs);
		RETURN duration::micros(1m100µs);
		RETURN duration::micros(100ns);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(150);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(60000100);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_millis() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::millis(150ms);
		RETURN duration::millis(1m100ms);
		RETURN duration::millis(100µs);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(150);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(60100);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_mins() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::mins(30m);
		RETURN duration::mins(1h30m);
		RETURN duration::mins(45s);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(30);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(90);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_nanos() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::nanos(200ns);
		RETURN duration::nanos(30ms100ns);
		RETURN duration::nanos(0ns);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(200);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(30000100);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_secs() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::secs(25s);
		RETURN duration::secs(1m25s);
		RETURN duration::secs(350ms);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(25);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(85);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_weeks() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::weeks(7w);
		RETURN duration::weeks(1y3w);
		RETURN duration::weeks(4d);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(7);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(55);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_years() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::years(7y);
		RETURN duration::years(7y4w30d);
		RETURN duration::years(4w);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(7);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(7);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_days() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::days(3);
		RETURN duration::from::days(50);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3d");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("7w1d");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_hours() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::hours(3);
		RETURN duration::from::hours(30);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3h");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1d6h");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_micros() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::micros(300);
		RETURN duration::from::micros(50500);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("300µs");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("50ms500µs");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_millis() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::millis(30);
		RETURN duration::from::millis(1500);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("30ms");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1s500ms");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_mins() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::mins(3);
		RETURN duration::from::mins(100);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3m");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1h40m");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_nanos() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::nanos(30);
		RETURN duration::from::nanos(5005000);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("30ns");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("5ms5µs");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_secs() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::secs(3);
		RETURN duration::from::secs(100);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3s");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1m40s");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_duration_from_weeks() -> Result<(), Error> {
	let sql = r#"
		RETURN duration::from::weeks(3);
		RETURN duration::from::weeks(60);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3w");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1y7w6d");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// geo
// --------------------------------------------------

// --------------------------------------------------
// is
// --------------------------------------------------

// --------------------------------------------------
// math
// --------------------------------------------------

// --------------------------------------------------
// meta
// --------------------------------------------------

// --------------------------------------------------
// not
// --------------------------------------------------

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
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// parse
// --------------------------------------------------

#[tokio::test]
async fn function_parse_email_host() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::email::host("john.doe@example.com");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("example.com");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_email_user() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::email::user("john.doe@example.com");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("john.doe");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_domain() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::domain("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("www.surrealdb.com");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_fragment() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::fragment("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("somefragment");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_host() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::host("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("www.surrealdb.com");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_path() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::path("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("/path/to/page");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_port() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::port("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(80);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_query() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::query("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("query=param");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_url_scheme() -> Result<(), Error> {
	let sql = r#"
		RETURN parse::url::scheme("https://user:pass@www.surrealdb.com:80/path/to/page?query=param#somefragment");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("https");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// rand
// --------------------------------------------------

// --------------------------------------------------
// string
// --------------------------------------------------

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

// --------------------------------------------------
// time
// --------------------------------------------------

#[tokio::test]
async fn function_time_day() -> Result<(), Error> {
	let sql = r#"
		RETURN time::day();
		RETURN time::day("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(22);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_floor() -> Result<(), Error> {
	let sql = r#"
		RETURN time::floor("1987-06-22T08:30:45Z", 1w);
		RETURN time::floor("1987-06-22T08:30:45Z", 1y);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-18T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1986-12-28T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_format() -> Result<(), Error> {
	let sql = r#"
		RETURN time::format("1987-06-22T08:30:45Z", "%Y-%m-%d");
		RETURN time::format("1987-06-22T08:30:45Z", "%T");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-22'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'08:30:45'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_group() -> Result<(), Error> {
	let sql = r#"
		RETURN time::group("1987-06-22T08:30:45Z", 'hour');
		RETURN time::group("1987-06-22T08:30:45Z", 'month');
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-22T08:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-01T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_hour() -> Result<(), Error> {
	let sql = r#"
		RETURN time::hour();
		RETURN time::hour("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(8);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_minute() -> Result<(), Error> {
	let sql = r#"
		RETURN time::minute();
		RETURN time::minute("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(30);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_month() -> Result<(), Error> {
	let sql = r#"
		RETURN time::month();
		RETURN time::month("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(6);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_nano() -> Result<(), Error> {
	let sql = r#"
		RETURN time::nano();
		RETURN time::nano("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(551349045000000000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_now() -> Result<(), Error> {
	let sql = r#"
		RETURN time::now();
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_datetime());
	//
	Ok(())
}

#[tokio::test]
async fn function_time_round() -> Result<(), Error> {
	let sql = r#"
		RETURN time::round("1987-06-22T08:30:45Z", 1w);
		RETURN time::round("1987-06-22T08:30:45Z", 1y);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-25T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1986-12-28T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_second() -> Result<(), Error> {
	let sql = r#"
		RETURN time::second();
		RETURN time::second("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(45);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_unix() -> Result<(), Error> {
	let sql = r#"
		RETURN time::unix();
		RETURN time::unix("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(551349045);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_wday() -> Result<(), Error> {
	let sql = r#"
		RETURN time::wday();
		RETURN time::wday("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_week() -> Result<(), Error> {
	let sql = r#"
		RETURN time::week();
		RETURN time::week("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(26);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_yday() -> Result<(), Error> {
	let sql = r#"
		RETURN time::yday();
		RETURN time::yday("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(173);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_year() -> Result<(), Error> {
	let sql = r#"
		RETURN time::year();
		RETURN time::year("1987-06-22T08:30:45Z");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1987);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_micros() -> Result<(), Error> {
	let sql = r#"
		RETURN time::from::micros(384025770384840);
		RETURN time::from::micros(2840257704384440);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1982-03-03T17:49:30.384840Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'2060-01-02T08:28:24.384440Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_millis() -> Result<(), Error> {
	let sql = r#"
		RETURN time::from::millis(384025773840);
		RETURN time::from::millis(2840257704440);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1982-03-03T17:49:33.840Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'2060-01-02T08:28:24.440Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_secs() -> Result<(), Error> {
	let sql = r#"
		RETURN time::from::secs(384053840);
		RETURN time::from::secs(2845704440);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1982-03-04T01:37:20Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'2060-03-05T09:27:20Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_unix() -> Result<(), Error> {
	let sql = r#"
		RETURN time::from::unix(384053840);
		RETURN time::from::unix(2845704440);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1982-03-04T01:37:20Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'2060-03-05T09:27:20Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// type
// --------------------------------------------------

#[tokio::test]
async fn function_type_bool() -> Result<(), Error> {
	let sql = r#"
		RETURN type::bool("true");
		RETURN type::bool("false");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_datetime() -> Result<(), Error> {
	let sql = r#"
		RETURN type::datetime("1987-06-22");
		RETURN type::datetime("2022-08-01");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'1987-06-22T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'2022-08-01T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_decimal() -> Result<(), Error> {
	let sql = r#"
		RETURN type::decimal("13.1043784018");
		RETURN type::decimal("13.5719384719384719385639856394139476937756394756");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("13.1043784018");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("13.5719384719384719385639856394139476937756394756");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_duration() -> Result<(), Error> {
	let sql = r#"
		RETURN type::duration("1h30m");
		RETURN type::duration("1h30m30s50ms");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1h30m");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1h30m30s50ms");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_float() -> Result<(), Error> {
	let sql = r#"
		RETURN type::float("13.1043784018");
		RETURN type::float("13.5719384719384719385639856394139476937756394756");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(13.1043784018f64);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(13.571938471938472f64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_int() -> Result<(), Error> {
	let sql = r#"
		RETURN type::int("194719");
		RETURN type::int("1457105732053058");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(194719i64);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1457105732053058i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_number() -> Result<(), Error> {
	let sql = r#"
		RETURN type::number("194719.1947104740");
		RETURN type::number("1457105732053058.3957394823281756381849375");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("194719.1947104740");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1457105732053058.3957394823281756381849375");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_point() -> Result<(), Error> {
	let sql = r#"
		RETURN type::point([1.345, 6.789]);
		RETURN type::point([51.509865, -0.136439]);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			type: 'Point',
			coordinates: [
				1.345,
				6.789
			]
		}",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			type: 'Point',
			coordinates: [
				51.509865,
				-0.136439
			]
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_string() -> Result<(), Error> {
	let sql = r#"
		RETURN type::string(30s);
		RETURN type::string(13.58248);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("30s");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("13.58248");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_table() -> Result<(), Error> {
	let sql = r#"
		RETURN type::table("person");
		RETURN type::table("animal");
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Table("person".into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Table("animal".into());
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_thing() -> Result<(), Error> {
	let sql = r#"
		CREATE type::thing('person', 'test');
		CREATE type::thing('person', 1434619);
		CREATE type::thing('city', '8e60244d-95f6-4f95-9e30-09a98977efb0');
		CREATE type::thing('temperature', ['London', '2022-09-30T20:25:01.406828Z']);
	"#;
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:1434619,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
