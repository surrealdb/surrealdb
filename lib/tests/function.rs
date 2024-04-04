mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::{self, Number, Value};

async fn test_queries(sql: &str, desired_responses: &[&str]) -> Result<(), Error> {
	let db = new_ds().await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let response = db.execute(sql, &session, None).await?;
	for (i, r) in response.into_iter().map(|r| r.result).enumerate() {
		let v = r?;
		if let Some(desired_response) = desired_responses.get(i) {
			let desired_value = sql::Value::parse(desired_response);
			// If both values are NaN, they are equal from a test PoV
			if !desired_value.is_nan() || !v.is_nan() {
				assert_eq!(
					v,
					desired_value,
					"Received response did not match \
	expected.
	Query response #{},
	Desired response: {desired_value},
	Actual response: {v}",
					i + 1
				);
			}
		} else {
			panic!("Response index {i} out of bounds of desired responses.");
		}
	}
	Ok(())
}

async fn check_test_is_error(sql: &str, expected_errors: &[&str]) -> Result<(), Error> {
	let db = new_ds().await?;
	let session = Session::owner().with_ns("test").with_db("test");
	let response = db.execute(sql, &session, None).await?;
	if response.len() != expected_errors.len() {
		panic!(
			"Wrong number of responses {} - expected {}.",
			response.len(),
			expected_errors.len()
		);
	}
	for (i, r) in response.into_iter().map(|r| r.result).enumerate() {
		if let Some(expected_error) = expected_errors.get(i) {
			if let Err(e) = r {
				assert_eq!(e.to_string().as_str(), *expected_error)
			} else {
				panic!("Response index {i} is not an error.");
			}
		} else {
			panic!("Response index {i} out of bounds of expected responses.");
		}
	}
	Ok(())
}

#[tokio::test]
async fn error_on_invalid_function() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut query = sql::Query::default();
	query.0 .0 = vec![sql::Statement::Value(sql::Value::Function(Box::new(
		sql::Function::Normal("this is an invalid function name".to_string(), Vec::new()),
	)))];
	let session = Session::owner().with_ns("test").with_db("test");
	let mut resp = dbs.process(query, &session, None).await.unwrap();
	assert_eq!(resp.len(), 1);
	match resp.pop().unwrap().result {
		Err(Error::InvalidFunction {
			..
		}) => {}
		x => panic!("returned wrong result {:#?}", x),
	}
	Ok(())
}

// --------------------------------------------------
// array
// --------------------------------------------------

#[tokio::test]
async fn function_array_add() -> Result<(), Error> {
	let sql = r#"
		RETURN array::add([], 3);
		RETURN array::add(3, true);
		RETURN array::add([1,2], 2);
		RETURN array::add([1,2], 3);
		RETURN array::add([1,2], [2,3]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::add(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_all() -> Result<(), Error> {
	let sql = r#"
		RETURN array::all([]);
		RETURN array::all("some text");
		RETURN array::all([1,2,"text",3,NONE,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::all(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::any(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_append() -> Result<(), Error> {
	let sql = r#"
		RETURN array::append([], 3);
		RETURN array::append(3, true);
		RETURN array::append([1,2], [2,3]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::append(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,[2,3]]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_at() -> Result<(), Error> {
	let sql = r#"
		RETURN array::at(["hello", "world"], 0);
		RETURN array::at(["hello", "world"], -1);
		RETURN array::at(["hello", "world"], 3);
		RETURN array::at(["hello", "world"], -3);
		RETURN array::at([], 0);
		RETURN array::at([], 3);
		RETURN array::at([], -3);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Strand("hello".into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Strand("world".into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::None);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_boolean_and() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::boolean_and([false, true, false, true], [false, false, true, true]);
RETURN array::boolean_and([0, 1, 0, 1], [0, 0, 1, 1]);
RETURN array::boolean_and([true, false], [false]);
RETURN array::boolean_and([true, true], [false]);"#,
		&[
			"[false, false, false, true]",
			"[false, false, false, true]",
			"[false, false]",
			"[false, false]",
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_boolean_not() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::boolean_not([false, true, 0, 1]);"#,
		&["[true, false, true, false]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_boolean_or() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::boolean_or([false, true, false, true], [false, false, true, true]);
RETURN array::boolean_or([0, 1, 0, 1], [0, 0, 1, 1]);
RETURN array::boolean_or([true, false], [false]);
RETURN array::boolean_or([true, true], [false]);"#,
		&[
			"[false, true, true, true]",
			"[false, true, true, true]",
			"[true, false]",
			"[true, true]",
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_boolean_xor() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::boolean_xor([false, true, false, true], [false, false, true, true]);"#,
		&["[false, true, true, false]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_combine() -> Result<(), Error> {
	let sql = r#"
		RETURN array::combine([], []);
		RETURN array::combine(3, true);
		RETURN array::combine([1,2], [2,3]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::combine(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ [1,2], [1,3], [2,2], [2,3] ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_clump() -> Result<(), Error> {
	let sql = r#"
		RETURN array::clump([0, 1, 2, 3], 2);
		RETURN array::clump([0, 1, 2], 2);
		RETURN array::clump([0, 1, 2], 3);
		RETURN array::clump([0, 1, 2, 3, 4, 5], 3);
	"#;
	let desired_responses =
		["[[0, 1], [2, 3]]", "[[0, 1], [2]]", "[[0, 1, 2]]", "[[0, 1, 2], [3, 4, 5]]"];
	test_queries(sql, &desired_responses).await?;
	Ok(())
}

#[tokio::test]
async fn function_array_complement() -> Result<(), Error> {
	let sql = r#"
		RETURN array::complement([], []);
		RETURN array::complement(3, true);
		RETURN array::complement([1,2,3,4], [3,4,5,6]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::complement(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
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
		RETURN array::concat();
		RETURN array::concat([], []);
		RETURN array::concat(3, true);
		RETURN array::concat([1,2,3,4], [3,4,5,6]);
		RETURN array::concat([1,2,3,4], [3,4,5,6], [5,6,7,8], [7,8,9,0]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::concat(). Expected at least one argument"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::concat(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,3,4,5,6]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,3,4,5,6,5,6,7,8,7,8,9,0]");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::difference(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::distinct(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_filter_index() -> Result<(), Error> {
	let sql = r#"RETURN array::filter_index([0, 1, 2], 1);
RETURN array::filter_index([0, 0, 2], 0);
RETURN array::filter_index(["hello_world", "hello world", "hello wombat", "hello world"], "hello world");
RETURN array::filter_index(["nothing here"], 0);"#;
	let desired_responses = ["[1]", "[0, 1]", "[1, 3]", "[]"];
	test_queries(sql, &desired_responses).await?;
	Ok(())
}

#[tokio::test]
async fn function_array_find_index() -> Result<(), Error> {
	let sql = r#"RETURN array::find_index([5, 6, 7], 7);
RETURN array::find_index(["hello world", null, true], null);
RETURN array::find_index([0, 1, 2], 3);"#;
	let desired_responses = ["2", "1", "null"];
	test_queries(sql, &desired_responses).await?;
	Ok(())
}

#[tokio::test]
async fn function_array_first() -> Result<(), Error> {
	let sql = r#"
		RETURN array::first(["hello", "world"]);
		RETURN array::first([["hello", "world"], 10]);
		RETURN array::first([]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Strand("hello".into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Array(vec!["hello", "world"].into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::flatten(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::group(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::intersect(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_join_arr() -> Result<(), Error> {
	let sql = r#"
		RETURN array::join([], "");
		RETURN array::join(["hello", "world"], ", ");
		RETURN array::join(["again", "again", "again"], " and ");
		RETURN array::join([42, true, "1.61"], " and ");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("hello, world");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("again and again and again");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("42 and true and 1.61");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_last() -> Result<(), Error> {
	let sql = r#"
		RETURN array::last(["hello", "world"]);
		RETURN array::last([["hello", "world"], 10]);
		RETURN array::last([]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Strand("world".into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = 10.into();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_len() -> Result<(), Error> {
	let sql = r#"
		RETURN array::len([]);
		RETURN array::len("some text");
		RETURN array::len([1,2,"text",3,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::len(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(6);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_logical_and() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::logical_and([true, false, true, false], [true, true, false, false]);
RETURN array::logical_and([1, 0, 1, 0], ["true", "true", "false", "false"]);
RETURN array::logical_and([0, 1], []);"#,
		&["[true, false, false, false]", r#"[1, 0, "false", 0]"#, "[0, null]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_logical_or() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::logical_or([true, false, true, false], [true, true, false, false]);
RETURN array::logical_or([1, 0, 1, 0], ["true", "true", "false", "false"]);
RETURN array::logical_or([0, 1], []);"#,
		&["[true, true, true, false]", r#"[1, "true", 1, 0]"#, "[0, 1]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_logical_xor() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::logical_xor([true, false, true, false], [true, true, false, false]);
RETURN array::logical_xor([1, 0, 1, 0], ["true", "true", "false", "false"]);
RETURN array::logical_xor([0, 1], []);"#,
		&["[false, true, true, false]", r#"[false, "true", 1, 0]"#, "[0, 1]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_matches() -> Result<(), Error> {
	test_queries(
		r#"RETURN array::matches([0, 1, 2], 1);
RETURN array::matches([[], [0]], []);
RETURN array::matches([{id: "ohno:0"}, {id: "ohno:1"}], {id: "ohno:1"});"#,
		&["[false, true, false]", "[true, false]", "[false, true]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_array_max() -> Result<(), Error> {
	let sql = r#"
		RETURN array::max([]);
		RETURN array::max("some text");
		RETURN array::max([1,2,"text",3,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::max(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::min(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_pop() -> Result<(), Error> {
	let sql = r#"
		RETURN array::pop([]);
		RETURN array::pop("some text");
		RETURN array::pop([1,2,"text",3,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::pop(). Argument 1 was the wrong type. Expected a array but found 'some text'"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(4);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_prepend() -> Result<(), Error> {
	let sql = r#"
		RETURN array::prepend([], 3);
		RETURN array::prepend(3, true);
		RETURN array::prepend([1,2], [2,3]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::prepend(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[[2,3],1,2]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_push() -> Result<(), Error> {
	let sql = r#"
		RETURN array::push([], 3);
		RETURN array::push(3, true);
		RETURN array::push([1,2], [2,3]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::push(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,[2,3]]");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_array_reverse() -> Result<(), Error> {
	let sql = r#"
		RETURN array::reverse([]);
		RETURN array::reverse(3);
		RETURN array::reverse([1,2,"text",3,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::reverse(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[4,3,3,'text',2,1]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_slice() -> Result<(), Error> {
	let sql = r#"
		RETURN array::slice([]);
		RETURN array::slice(3);
		RETURN array::slice([1,2,"text",3,3,4]);
		RETURN array::slice([1,2,"text",3,3,4], 1);
		RETURN array::slice([1,2,"text",3,3,4], 3);
		RETURN array::slice([1,2,"text",3,3,4], 3, -1);
		RETURN array::slice([1,2,"text",3,3,4], -1);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::slice(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,'text',3,3,4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[2,'text',3,3,4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,3,4]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3,3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[4]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_sort() -> Result<(), Error> {
	let sql = r#"
		RETURN array::sort([]);
		RETURN array::sort(3, false);
		RETURN array::sort([4,2,"text",1,3,4]);
		RETURN array::sort([4,2,"text",1,3,4], true);
		RETURN array::sort([4,2,"text",1,3,4], false);
		RETURN array::sort([4,2,"text",1,3,4], "asc");
		RETURN array::sort([4,2,"text",1,3,4], "desc");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::sort(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,4,'text']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,4,'text']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['text',4,4,3,2,1]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,4,'text']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['text',4,4,3,2,1]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_sort_asc() -> Result<(), Error> {
	let sql = r#"
		RETURN array::sort::asc([]);
		RETURN array::sort::asc(3);
		RETURN array::sort::asc([4,2,"text",1,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::sort::asc(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,3,4,4,'text']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_sort_desc() -> Result<(), Error> {
	let sql = r#"
		RETURN array::sort::desc([]);
		RETURN array::sort::desc(3);
		RETURN array::sort::desc([4,2,"text",1,3,4]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::sort::desc(). Argument 1 was the wrong type. Expected a array but found 3"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['text',4,4,3,2,1]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_array_transpose() -> Result<(), Error> {
	let sql = r#"
		RETURN array::transpose([[0, 1], [2, 3]]);
		RETURN array::transpose([[0, 1, 2], [3, 4]]);
		RETURN array::transpose([[0, 1], [2, 3, 4]]);
		RETURN array::transpose([[0, 1], [2, 3], [4, 5]]);
		RETURN array::transpose([[0, 1, 2], "oops", [null, "sorry"]]);
	"#;
	let desired_responses = [
		"[[0, 2], [1, 3]]",
		"[[0, 3], [1, 4], [2]]",
		"[[0, 2], [1, 3], [4]]",
		"[[0, 2, 4], [1, 3, 5]]",
		"[[0, \"oops\", null], [1, \"sorry\"], [2]]",
	];
	test_queries(sql, &desired_responses).await?;
	Ok(())
}

#[tokio::test]
async fn function_array_union() -> Result<(), Error> {
	let sql = r#"
		RETURN array::union([], []);
		RETURN array::union(3, true);
		RETURN array::union([1,2,1,6], [1,3,4,5,6]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function array::union(). Argument 1 was the wrong type. Expected a array but found 3",
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1,2,6,3,4,5]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// bytes
// --------------------------------------------------

#[tokio::test]
async fn function_bytes_len() -> Result<(), Error> {
	let sql = r#"
		RETURN bytes::len(<bytes>"");
		RETURN bytes::len(true);
		RETURN bytes::len(<bytes>"π");
		RETURN bytes::len(<bytes>"ππ");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("0");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function bytes::len(). Argument 1 was the wrong type. Expected a bytes but found true"
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("2");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("4");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
// encoding
// --------------------------------------------------

#[tokio::test]
async fn function_encoding_base64_decode() -> Result<(), Error> {
	let sql = r#"
		RETURN encoding::base64::decode("");
		RETURN encoding::base64::decode("aGVsbG8") = <bytes>"hello";
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Bytes(Vec::new().into());
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_encoding_base64_encode() -> Result<(), Error> {
	let sql = r#"
		RETURN encoding::base64::encode(<bytes>"");
		RETURN encoding::base64::encode(<bytes>"hello");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("''");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'aGVsbG8'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// geo
// --------------------------------------------------

#[tokio::test]
async fn function_parse_geo_area() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::area({
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		});
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1029944667.4192368);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_geo_bearing() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::bearing(
			{
				type: 'Point',
				coordinates: [-0.136439, 51.509865]
			},
			{
				type: 'Point',
				coordinates: [ -73.971321, 40.776676]
			}
		);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(-71.63409590760736);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_geo_centroid() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::centroid({
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		});
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			type: 'Point',
			coordinates: [
				-0.10231019499999999,
				51.49576478
			]
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_geo_distance() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::distance(
			{
				type: 'Point',
				coordinates: [-0.136439, 51.509865]
			},
			{
				type: 'Point',
				coordinates: [ -73.971321, 40.776676]
			}
		);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(5562851.11270021);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_geo_hash_encode() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::hash::encode({
			type: 'Point',
			coordinates: [-0.136439, 51.509865]
		});
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("gcpvhchdswz9");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_geo_hash_decode() -> Result<(), Error> {
	let sql = r#"
		RETURN geo::hash::decode('gcpvhchdswz9');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			type: 'Point',
			coordinates: [
				-0.13643911108374596,
				51.50986502878368
			]
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// math
// --------------------------------------------------

#[tokio::test]
async fn function_math_abs() -> Result<(), Error> {
	let sql = r#"
		RETURN math::abs(0);
		RETURN math::abs(100);
		RETURN math::abs(-100);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(100);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(100);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_bottom() -> Result<(), Error> {
	let sql = r#"
		RETURN math::bottom([1,2,3], 0);
		RETURN math::bottom([1,2,3], 1);
		RETURN math::bottom([1,2,3], 2);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function math::bottom(). The second argument must be an integer greater than 0."
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[1]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[2,1]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_ceil() -> Result<(), Error> {
	let sql = r#"
		RETURN math::ceil(101);
		RETURN math::ceil(101.5);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(102);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_fixed() -> Result<(), Error> {
	let sql = r#"
		RETURN math::fixed(101, 0);
		RETURN math::fixed(101, 2);
		RETURN math::fixed(101.5, 2);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function math::fixed(). The second argument must be an integer greater than 0."
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101.50);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_floor() -> Result<(), Error> {
	let sql = r#"
		RETURN math::floor(101);
		RETURN math::floor(101.5);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_interquartile() -> Result<(), Error> {
	let sql = r#"
		RETURN math::interquartile([]);
		RETURN math::interquartile([101, 213, 202]);
		RETURN math::interquartile([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(56.0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(56.0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_max() -> Result<(), Error> {
	let sql = r#"
		RETURN math::max([]);
		RETURN math::max([101, 213, 202]);
		RETURN math::max([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_mean() -> Result<(), Error> {
	let sql = r#"
		RETURN math::mean([]);
		RETURN math::mean([101, 213, 202]);
		RETURN math::mean([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(172);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(172.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_median() -> Result<(), Error> {
	let sql = r#"
		RETURN math::median([]);
		RETURN math::median([101, 213, 202]);
		RETURN math::median([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(202);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(202.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_midhinge() -> Result<(), Error> {
	let sql = r#"
		RETURN math::midhinge([]);
		RETURN math::midhinge([101, 213, 202]);
		RETURN math::midhinge([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(179.5);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(180);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_min() -> Result<(), Error> {
	let sql = r#"
		RETURN math::min([]);
		RETURN math::min([101, 213, 202]);
		RETURN math::min([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_mode() -> Result<(), Error> {
	let sql = r#"
		RETURN math::mode([]);
		RETURN math::mode([101, 213, 202]);
		RETURN math::mode([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_nearestrank() -> Result<(), Error> {
	let sql = r#"
		RETURN math::nearestrank([], 75);
		RETURN math::nearestrank([101, 213, 202], 75);
		RETURN math::nearestrank([101.5, 213.5, 202.5], 75);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_percentile() -> Result<(), Error> {
	let sql = r#"
		RETURN math::percentile([], 99);
		RETURN math::percentile([101, 213, 202], 99);
		RETURN math::percentile([101.5, 213.5, 202.5], 99);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(212.78);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(213.28);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_pow() -> Result<(), Error> {
	let sql = r#"
		RETURN math::pow(101, 3);
		RETURN math::pow(101.5, 3);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1030301);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1045678.375);
	assert_eq!(tmp, val);

	let sql = r#"
		RETURN math::pow(101, 50);
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let res = res.remove(0).result;
	assert!(matches!(res, Err(Error::TryPow(_, _))));
	//
	Ok(())
}

#[tokio::test]
async fn function_math_product() -> Result<(), Error> {
	let sql = r#"
		RETURN math::product([]);
		RETURN math::product([101, 213, 202]);
		RETURN math::product([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(4345626);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(4388225.625);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_round() -> Result<(), Error> {
	let sql = r#"
		RETURN math::round(101);
		RETURN math::round(101.5);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(101);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(102);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_spread() -> Result<(), Error> {
	let sql = r#"
		RETURN math::spread([]);
		RETURN math::spread([101, 213, 202]);
		RETURN math::spread([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(112);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(112.0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_sqrt() -> Result<(), Error> {
	let sql = r#"
		RETURN math::sqrt(101);
		RETURN math::sqrt(101.5);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(10.04987562112089);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("10.07472083980494220820325739456714210123675076934383520155548236146713380225253351613768233376490240");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_stddev() -> Result<(), Error> {
	let sql = r#"
		RETURN math::stddev([]);
		RETURN math::stddev([101, 213, 202]);
		RETURN math::stddev([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(61.73329733620261);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(61.73329733620261);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_sum() -> Result<(), Error> {
	let sql = r#"
		RETURN math::sum([]);
		RETURN math::sum([101, 213, 202]);
		RETURN math::sum([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(516);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(517.5);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_top() -> Result<(), Error> {
	let sql = r#"
		RETURN math::top([1,2,3], 0);
		RETURN math::top([1,2,3], 1);
		RETURN math::top([1,2,3], 2);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Incorrect arguments for function math::top(). The second argument must be an integer greater than 0."
		),
		"{tmp:?}"
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[3]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[2,3]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_trimean() -> Result<(), Error> {
	let sql = r#"
		RETURN math::trimean([]);
		RETURN math::trimean([101, 213, 202]);
		RETURN math::trimean([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(190.75);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(191.25);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_math_variance() -> Result<(), Error> {
	let sql = r#"
		RETURN math::variance([]);
		RETURN math::variance([101, 213, 202]);
		RETURN math::variance([101.5, 213.5, 202.5]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_nan());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(3811);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(3811.0);
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// meta
// --------------------------------------------------

#[tokio::test]
async fn function_parse_meta_id() -> Result<(), Error> {
	let sql = r#"
		RETURN meta::id(r"person:tobie");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("tobie");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_parse_meta_table() -> Result<(), Error> {
	let sql = r#"
		RETURN meta::table(r"person:tobie");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("person");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// object
// --------------------------------------------------

#[tokio::test]
async fn function_object_entries() -> Result<(), Error> {
	let sql = r#"
		RETURN object::entries({ a: 1, b: 2 });
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ [ 'a', 1 ], [ 'b', 2 ] ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_object_from_entries() -> Result<(), Error> {
	let sql = r#"
		RETURN object::from_entries([ [ 'a', 1 ], [ 'b', 2 ] ]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("{ a: 1, b: 2 }");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_object_keys() -> Result<(), Error> {
	let sql = r#"
		RETURN object::keys({ a: 1, b: 2 });
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ 'a', 'b' ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_object_len() -> Result<(), Error> {
	let sql = r#"
		RETURN object::len({ a: 1, b: 2 });
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("2");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_object_values() -> Result<(), Error> {
	let sql = r#"
		RETURN object::values({ a: 1, b: 2 });
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ 1, 2 ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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

#[tokio::test]
async fn function_rand() -> Result<(), Error> {
	let sql = r#"
		RETURN rand();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_float());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_bool() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::bool();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_bool());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_enum() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::enum(["one", "two", "three"]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_float() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::float();
		RETURN rand::float(5, 10);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_float());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_float());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_guid() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::guid();
		RETURN rand::guid(10);
		RETURN rand::guid(10, 15);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_int() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::int();
		RETURN rand::int(5, 10);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_int());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_int());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_string() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::string();
		RETURN rand::string(10);
		RETURN rand::string(10, 15);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_time() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::time();
		RETURN rand::time(1577836800, 1893456000);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_datetime());
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_datetime());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_ulid() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::ulid();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_strand());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::uuid();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_v4() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::uuid::v4();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_v7() -> Result<(), Error> {
	let sql = r#"
		RETURN rand::uuid::v7();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

// --------------------------------------------------
// string
// --------------------------------------------------

#[tokio::test]
async fn function_string_concat() -> Result<(), Error> {
	let sql = r#"
		RETURN string::concat();
		RETURN string::concat("test");
		RETURN string::concat("this", " ", "is", " ", "a", " ", "test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_contains() -> Result<(), Error> {
	let sql = r#"
		RETURN string::contains("", "");
		RETURN string::contains("a", "");
		RETURN string::contains("abcdefg", "");
		RETURN string::contains("abcdefg", "bcd");
		RETURN string::contains("abcdefg", "abcd");
		RETURN string::contains("abcdefg", "xxabcd");
		RETURN string::contains("abcdefg", "hij");
		RETURN string::contains("ประเทศไทย中华", "ประเ");
		RETURN string::contains("ประเทศไทย中华", "ะเ");
		RETURN string::contains("ประเทศไทย中华", "ไท华");
		RETURN string::contains("1234567ah012345678901ah", "hah");
		RETURN string::contains("00abc01234567890123456789abc", "bcabc");
		RETURN string::contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaba");
		RETURN string::contains("* \t", " ");
		RETURN string::contains("* \t", "?");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 15);
	// 1
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 2
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 3
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 4
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 5
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 6
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 7
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 8
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 9
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 10
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 11
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 12
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 13
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 14
	let tmp = res.remove(0).result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 15
	let tmp = res.remove(0).result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_ends_with() -> Result<(), Error> {
	let sql = r#"
		RETURN string::endsWith("", "");
		RETURN string::endsWith("", "test");
		RETURN string::endsWith("this is a test", "test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
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
	Ok(())
}

#[test_log::test(tokio::test)]
async fn function_search_analyzer() -> Result<(), Error> {
	let sql = r#"
        DEFINE FUNCTION fn::stripHtml($html: string) {
            RETURN string::replace($html, /<[^>]*>/, "");
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['This', 'is', 'a', 'sample', 'of', 'HTML']");
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[test_log::test(tokio::test)]
async fn function_search_analyzer_invalid_arguments() -> Result<(), Error> {
	let sql = r#"
        DEFINE FUNCTION fn::unsupportedFunction() {
            RETURN 1;
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedFunction TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	match res.remove(0).result {
		Err(Error::InvalidArguments {
			name,
			message,
		}) => {
			assert_eq!(&name, "fn::unsupportedFunction");
			assert_eq!(&message, "The function expects 0 arguments.");
		}
		_ => panic!("Should have fail!"),
	}
	Ok(())
}

#[test_log::test(tokio::test)]
async fn function_search_analyzer_invalid_return_type() -> Result<(), Error> {
	let sql = r#"
        DEFINE FUNCTION fn::unsupportedReturnedType($html: string) {
            RETURN 1;
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::unsupportedReturnedType TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	for _ in 0..2 {
		let tmp = res.remove(0).result;
		assert!(tmp.is_ok());
	}
	//
	match res.remove(0).result {
		Err(Error::InvalidFunction {
			name,
			message,
		}) => {
			assert_eq!(&name, "unsupportedReturnedType");
			assert_eq!(&message, "The function should return a string.");
		}
		r => panic!("Unexpected result: {:?}", r),
	}
	Ok(())
}

#[test_log::test(tokio::test)]
async fn function_search_analyzer_invalid_function_name() -> Result<(), Error> {
	let sql = r#"
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::doesNotExist TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	match res.remove(0).result {
		Err(Error::FcNotFound {
			value,
		}) => {
			assert_eq!(&value, "doesNotExist");
		}
		r => panic!("Unexpected result: {:?}", r),
	}
	Ok(())
}

#[tokio::test]
async fn function_parse_is_alphanum() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::alphanum("abcdefg123");
		RETURN string::is::alphanum("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_alpha() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::alpha("abcdefg");
		RETURN string::is::alpha("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_ascii() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::ascii("abcdefg123");
		RETURN string::is::ascii("this is a test 😀");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_datetime() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::datetime("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S");
		RETURN string::is::datetime("2012-06-22 23:56:04", "%T");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_domain() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::domain("surrealdb.com");
		RETURN string::is::domain("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_email() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::email("info@surrealdb.com");
		RETURN string::is::email("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_hexadecimal() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::hexadecimal("ff009e");
		RETURN string::is::hexadecimal("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_latitude() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::latitude("51.509865");
		RETURN string::is::latitude("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_longitude() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::longitude("-90.136439");
		RETURN string::is::longitude("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_numeric() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::numeric("13136439");
		RETURN string::is::numeric("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_semver() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::semver("1.0.0-rc.1");
		RETURN string::is::semver("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_url() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::url("https://surrealdb.com/docs");
		RETURN string::is::url("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_parse_is_uuid() -> Result<(), Error> {
	let sql = r#"
		RETURN string::is::uuid(u"e72bee20-f49b-11ec-b939-0242ac120002");
		RETURN string::is::uuid("this is a test!");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_string_join() -> Result<(), Error> {
	let sql = r#"
		RETURN string::join("");
		RETURN string::join("test");
		RETURN string::join(" ", "this", "is", "a", "test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_len() -> Result<(), Error> {
	let sql = r#"
		RETURN string::len("");
		RETURN string::len("test");
		RETURN string::len("test this string");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(4);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(16);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_lowercase() -> Result<(), Error> {
	let sql = r#"
		RETURN string::lowercase("");
		RETURN string::lowercase("TeSt");
		RETURN string::lowercase("THIS IS A TEST");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// "<[^>]*>" , ""

#[tokio::test]
async fn function_string_replace_with_regex() -> Result<(), Error> {
	let sql = r#"
		RETURN string::replace('<p>This is a <em>sample</em> string with <a href="\\#">HTML</a> tags.</p>', /<[^>]*>/, "");
		RETURN string::replace('<p>This one is already <strong>compiled!<strong></p>', /<[^>]*>/, "");
"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("This is a sample string with HTML tags.");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("This one is already compiled!");
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn function_string_matches() -> Result<(), Error> {
	let sql = r#"
		RETURN string::matches("foo", /foo/);
		RETURN string::matches("bar", /foo/);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn function_string_repeat() -> Result<(), Error> {
	let sql = r#"
		RETURN string::repeat("", 3);
		RETURN string::repeat("test", 3);
		RETURN string::repeat("test this", 3);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("testtesttest");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("test thistest thistest this");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_replace() -> Result<(), Error> {
	let sql = r#"
		RETURN string::replace("", "", "");
		RETURN string::replace('this is a test', 'a test', 'awesome');
		RETURN string::replace("this is an 😀 emoji test", "😀", "awesome 👍");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is awesome");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is an awesome 👍 emoji test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_reverse() -> Result<(), Error> {
	let sql = r#"
		RETURN string::reverse("");
		RETURN string::reverse("test");
		RETURN string::reverse("test this string");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("tset");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("gnirts siht tset");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_similarity_fuzzy() -> Result<(), Error> {
	let sql = r#"
		RETURN string::similarity::fuzzy("", "");
		RETURN string::similarity::fuzzy("some", "text");
		RETURN string::similarity::fuzzy("text", "TEXT");
		RETURN string::similarity::fuzzy("TEXT", "TEXT");
		RETURN string::similarity::fuzzy("this could be a tricky test", "this test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(83));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(91));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(174));
	//
	Ok(())
}

#[tokio::test]
async fn function_string_similarity_smithwaterman() -> Result<(), Error> {
	let sql = r#"
		RETURN string::similarity::smithwaterman("", "");
		RETURN string::similarity::smithwaterman("some", "text");
		RETURN string::similarity::smithwaterman("text", "TEXT");
		RETURN string::similarity::smithwaterman("TEXT", "TEXT");
		RETURN string::similarity::smithwaterman("this could be a tricky test", "this test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(83));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(91));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, Value::from(174));
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_string_slug() -> Result<(), Error> {
	let sql = r#"
		RETURN string::slug("");
		RETURN string::slug("this is a test");
		RETURN string::slug("blog - this is a test with 😀 emojis");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this-is-a-test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("blog-this-is-a-test-with-grinning-emojis");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_split() -> Result<(), Error> {
	let sql = r#"
		RETURN string::split("", "");
		RETURN string::split("this, is, a, list", ", ");
		RETURN string::split("this - is - another - test", " - ");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['', '']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['this', 'is', 'a', 'list']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['this', 'is', 'another', 'test']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_starts_with() -> Result<(), Error> {
	let sql = r#"
		RETURN string::startsWith("", "");
		RETURN string::startsWith("", "test");
		RETURN string::startsWith("test this string", "test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
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
	Ok(())
}

#[tokio::test]
async fn function_string_trim() -> Result<(), Error> {
	let sql = r#"
		RETURN string::trim("");
		RETURN string::trim("test");
		RETURN string::trim("   this is a test with text   ");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("this is a test with text");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_uppercase() -> Result<(), Error> {
	let sql = r#"
		RETURN string::uppercase("");
		RETURN string::uppercase("tEsT");
		RETURN string::uppercase("this is a test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("TEST");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("THIS IS A TEST");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_words() -> Result<(), Error> {
	let sql = r#"
		RETURN string::words("");
		RETURN string::words("test");
		RETURN string::words("this is a test");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['test']");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("['this', 'is', 'a', 'test']");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// time
// --------------------------------------------------

#[tokio::test]
async fn function_time_ceil() -> Result<(), Error> {
	let sql = r#"
		RETURN time::ceil(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::ceil(d"1987-06-22T08:30:45Z", 1y);
		RETURN time::ceil(d"2023-05-11T03:09:00Z", 1s);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-25T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-12-28T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2023-05-11T03:09:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_day() -> Result<(), Error> {
	let sql = r#"
		RETURN time::day();
		RETURN time::day(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::floor(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::floor(d"1987-06-22T08:30:45Z", 1y);
		RETURN time::floor(d"2023-05-11T03:09:00Z", 1s);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-18T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1986-12-28T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2023-05-11T03:09:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_format() -> Result<(), Error> {
	let sql = r#"
		RETURN time::format(d"1987-06-22T08:30:45Z", "%Y-%m-%d");
		RETURN time::format(d"1987-06-22T08:30:45Z", "%T");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::group(d"1987-06-22T08:30:45Z", 'hour');
		RETURN time::group(d"1987-06-22T08:30:45Z", 'month');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-22T08:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-01T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_hour() -> Result<(), Error> {
	let sql = r#"
		RETURN time::hour();
		RETURN time::hour(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_time_min() -> Result<(), Error> {
	let sql = r#"
		RETURN time::min([d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z"]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-22T08:30:45Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_max() -> Result<(), Error> {
	let sql = r#"
		RETURN time::max([d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z"]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1988-06-22T08:30:45Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_minute() -> Result<(), Error> {
	let sql = r#"
		RETURN time::minute();
		RETURN time::minute(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::month(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::nano(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_time_micros() -> Result<(), Error> {
	let sql = r#"
		RETURN time::micros();
		RETURN time::micros(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(551349045000000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_millis() -> Result<(), Error> {
	let sql = r#"
		RETURN time::millis();
		RETURN time::millis(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	assert!(tmp.is_number());
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(551349045000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_now() -> Result<(), Error> {
	let sql = r#"
		RETURN time::now();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::round(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::round(d"1987-06-22T08:30:45Z", 1y);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-25T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1986-12-28T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_second() -> Result<(), Error> {
	let sql = r#"
		RETURN time::second();
		RETURN time::second(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::unix(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::wday(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::week(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::yday(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN time::year(d"1987-06-22T08:30:45Z");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_time_from_nanos() -> Result<(), Error> {
	let sql = r#"
		RETURN time::from::nanos(384025770384840000);
		RETURN time::from::nanos(2840257704384440000);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1982-03-03T17:49:30.384840Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2060-01-02T08:28:24.384440Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1982-03-03T17:49:30.384840Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2060-01-02T08:28:24.384440Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1982-03-03T17:49:33.840Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2060-01-02T08:28:24.440Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1982-03-04T01:37:20Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2060-03-05T09:27:20Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1982-03-04T01:37:20Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2060-03-05T09:27:20Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'1987-06-22T00:00:00Z'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("d'2022-08-01T00:00:00Z'");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Number(Number::Decimal("13.1043784018".parse().unwrap()));
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::Number(Number::Decimal(
		"13.571938471938471938563985639413947693775639".parse().unwrap(),
	));
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
async fn function_type_is_array() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::array([1, 2, 3]);
		RETURN type::is::array("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_bool() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::bool(true);
		RETURN type::is::bool("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_bytes() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::bytes(<bytes>"");
		RETURN type::is::bytes("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_collection() -> Result<(), Error> {
	let sql = r#"
		LET $collection = <geometry<collection>> { type: 'GeometryCollection', geometries: [{ type: 'MultiPoint', coordinates: [[10, 11.2], [10.5, 11.9]] }] };
		RETURN type::is::collection($collection);
		RETURN type::is::collection("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_datetime() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::datetime(<datetime> d"2023-09-04T11:22:38.247Z");
		RETURN type::is::datetime("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_decimal() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::decimal(1.0dec);
		RETURN type::is::decimal("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_duration() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::duration(20s);
		RETURN type::is::duration("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_float() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::float(1.0f);
		RETURN type::is::float("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_geometry() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::geometry((-0.118092, 51.509865));
		RETURN type::is::geometry("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_int() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::int(123);
		RETURN type::is::int("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_line() -> Result<(), Error> {
	let sql = r#"
		LET $line = <geometry<line>> { type: 'LineString', coordinates: [[10, 11.2], [10.5, 11.9]] };
		RETURN type::is::line($line);
		RETURN type::is::line("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_none() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::none(none);
		RETURN type::is::none("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_null() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::null(null);
		RETURN type::is::null("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multiline() -> Result<(), Error> {
	let sql = r#"
		LET $multiline = <geometry<multiline>> { type: 'MultiLineString', coordinates: [[[10, 11.2], [10.5, 11.9]], [[11, 12.2], [11.5, 12.9], [12, 13]]] };
		RETURN type::is::multiline($multiline);
		RETURN type::is::multiline("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multipoint() -> Result<(), Error> {
	let sql = r#"
		LET $multipoint = <geometry<multipoint>> { type: 'MultiPoint', coordinates: [[10, 11.2], [10.5, 11.9]] };
		RETURN type::is::multipoint($multipoint);
		RETURN type::is::multipoint("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multipolygon() -> Result<(), Error> {
	let sql = r#"
		LET $multipolygon = <geometry<multipolygon>> { type: 'MultiPolygon', coordinates: [[[[10, 11.2], [10.5, 11.9], [10.8, 12], [10, 11.2]]], [[[9, 11.2], [10.5, 11.9], [10.3, 13], [9, 11.2]]]] };
		RETURN type::is::multipolygon($multipolygon);
		RETURN type::is::multipolygon("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_number() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::number(123);
		RETURN type::is::number(123.0f);
		RETURN type::is::number(123.0dec);
		RETURN type::is::number("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_object() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::object({ test: 123 });
		RETURN type::is::object("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_point() -> Result<(), Error> {
	let sql = r#"
		LET $point = <geometry<point>> { type: "Point", coordinates: [-0.118092, 51.509865] };
		RETURN type::is::point($point);
		RETURN type::is::point("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_polygon() -> Result<(), Error> {
	let sql = r#"
		LET $polygon = <geometry<polygon>> { type: 'Polygon', coordinates: [[[-0.38314819, 51.37692386], [0.1785278, 51.37692386], [0.1785278, 51.6146057], [-0.38314819, 51.6146057], [-0.38314819, 51.37692386]]] };
		RETURN type::is::polygon($polygon);
		RETURN type::is::polygon("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_record() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::record(person:john);
		RETURN type::is::record("123");
		RETURN type::is::record(person:john, 'person');
		RETURN type::is::record(person:john, 'user');
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_string() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::string("testing!");
		RETURN type::is::string(123);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_uuid() -> Result<(), Error> {
	let sql = r#"
		RETURN type::is::uuid(<uuid> u"018a6065-a80a-765e-b640-9fcb330a2f4f");
		RETURN type::is::uuid("123");
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from(false);
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		RETURN type::point([-0.136439, 51.509865]);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
				-0.136439,
				51.509865
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
		RETURN type::string(13);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("30s");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::from("13");
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
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
		CREATE type::thing(<string> person:john);
		CREATE type::thing('city', '8e60244d-95f6-4f95-9e30-09a98977efb0');
		CREATE type::thing('temperature', ['London', '2022-09-30T20:25:01.406828Z']);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
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
				id: person:john,
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

#[tokio::test]
async fn function_type_range() -> Result<(), Error> {
	let sql = r#"
		RETURN type::range('person');
		RETURN type::range('person',1);
		RETURN type::range('person',null,10);
		RETURN type::range('person',1,10);
		RETURN type::range('person',1,10, { begin: "excluded", end: "included"});
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("person:..");
	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse("person:1..");
	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse("person:..10");
	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse("person:1..10");
	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse("person:1>..=10");
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn function_vector_add() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::add([1, 2, 3], [1, 2, 3]);
		RETURN vector::add([1, 2, 3], [-1, -2, -3]);
	"#,
		&["[2, 4, 6]", "[0, 0, 0]"],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::add([1, 2, 3], [4, 5]);
		RETURN vector::add([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::add(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::add(). The two vectors must be of the same dimension."
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_angle() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::angle([1,0,0], [0,1,0]);
		RETURN vector::angle([5, 10, 15], [10, 5, 20]);
		RETURN vector::angle([-3, 2, 5], [4, -1, 2]);
		RETURN vector::angle([NaN, 2, 3], [-1, -2, NaN]);
	"#,
		&["1.5707963267948966", "0.36774908225917935", "1.7128722906354115", "NaN"],
	)
	.await?;

	check_test_is_error(
		r#"
		RETURN vector::angle([1, 2, 3], [4, 5]);
		RETURN vector::angle([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::angle(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::angle(). The two vectors must be of the same dimension."
		],
	).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_cross() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::cross([1, 2, 3], [4, 5, 6]);
		RETURN vector::cross([1, 2, 3], [-4, -5, -6]);
		RETURN vector::cross([1, NaN, 3], [NaN, -5, -6]);
	"#,
		&["[-3, 6, -3]", "[3, -6, 3]", "[NaN, NaN, NaN]"],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::cross([1, 2, 3], [4, 5]);
		RETURN vector::cross([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3.",
			"Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3."
		],
	)
		.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_dot() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::dot([1, 2, 3], [1, 2, 3]);
		RETURN vector::dot([1, 2, 3], [-1, -2, -3]);
		"#,
		&["14", "-14"],
	)
	.await?;

	check_test_is_error(
		r#"
		RETURN vector::dot([1, 2, 3], [4, 5]);
		RETURN vector::dot([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::dot(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::dot(). The two vectors must be of the same dimension."
		],
	).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_magnitude() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::magnitude([]);
		RETURN vector::magnitude([1]);
		RETURN vector::magnitude([5]);
		RETURN vector::magnitude([1,2,3,3,3,4,5]);
	"#,
		&["0", "1", "5", "8.54400374531753"],
	)
	.await
}

#[tokio::test]
async fn function_vector_normalize() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::normalize([]);
		RETURN vector::normalize([1]);
		RETURN vector::normalize([5]);
		RETURN vector::normalize([4,3]);
	"#,
		&["[]", "[1]", "[1]", "[0.8,0.6]"],
	)
	.await
}

#[tokio::test]
async fn function_vector_multiply() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::multiply([1, 2, 3], [1, 2, 3]);
		RETURN vector::multiply([1, 2, 3], [-1, -2, -3]);
	"#,
		&["[1, 4, 9]", "[-1, -4, -9]"],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::multiply([1, 2, 3], [4, 5]);
		RETURN vector::multiply([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::multiply(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::multiply(). The two vectors must be of the same dimension."
		],
	)
		.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_project() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::project([1, 2, 3], [4, 5, 6]);
		RETURN vector::project([1, -2, 3], [-4, 5, 6]);
		RETURN vector::project([NaN, -2, 3], [-4, NaN, NaN]);
	"#,
		&[
			"[1.6623376623376624, 2.077922077922078, 2.4935064935064934]",
			"[-0.2077922077922078, 0.25974025974025977, 0.3116883116883117]",
			"[NaN, NaN, NaN]",
		],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::project([1, 2, 3], [4, 5]);
		RETURN vector::project([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::project(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::project(). The two vectors must be of the same dimension."
		],
	)
		.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_divide() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::divide([10, NaN, 20, 30, 0], [0, 1, 2, 0, 4]);
		RETURN vector::divide([10, -20, 30, 0], [0, -1, 2, -3]);
	"#,
		&["[NaN, NaN, 10, NaN, 0]", "[NaN, 20, 15, 0]"],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::divide([1, 2, 3], [4, 5]);
		RETURN vector::divide([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::divide(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::divide(). The two vectors must be of the same dimension."
		],
	)
		.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_subtract() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::subtract([1, 2, 3], [1, 2, 3]);
		RETURN vector::subtract([1, 2, 3], [-1, -2, -3]);
	"#,
		&["[0, 0, 0]", "[2, 4, 6]"],
	)
	.await?;
	check_test_is_error(
		r#"
		RETURN vector::subtract([1, 2, 3], [4, 5]);
		RETURN vector::subtract([1, 2], [4, 5, 5]);
	"#,
		&[
			"Incorrect arguments for function vector::subtract(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::subtract(). The two vectors must be of the same dimension."
		],
	)
		.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_similarity_cosine() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::similarity::cosine([1, 2, 3], [1, 2, 3]);
		RETURN vector::similarity::cosine([1, 2, 3], [-1, -2, -3]);
		RETURN vector::similarity::cosine([NaN, 1, 2, 3], [NaN, 1, 2, 3]);
		RETURN vector::similarity::cosine([10, 50, 200], [400, 100, 20]);
	"#,
		&["1.0", "-1.0", "NaN", "0.15258215962441316"],
	)
	.await?;

	check_test_is_error(
	r"RETURN vector::similarity::cosine([1, 2, 3], [4, 5]);
		RETURN vector::similarity::cosine([1, 2], [4, 5, 5]);",
	&[
		"Incorrect arguments for function vector::similarity::cosine(). The two vectors must be of the same dimension.",
		"Incorrect arguments for function vector::similarity::cosine(). The two vectors must be of the same dimension."
	]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_similarity_jaccard() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::similarity::jaccard([1, 2, 3], [3, 2, 1]);
		RETURN vector::similarity::jaccard([1, 2, 3], [-3, -2, -1]);
		RETURN vector::similarity::jaccard([1, -2, 3, -4], [4, 3, 2, 1]);
		RETURN vector::similarity::jaccard([NaN, 1, 2, 3], [NaN, 2, 3, 4]);
		RETURN vector::similarity::jaccard([0,1,2,5,6], [0,2,3,4,5,7,9]);
	"#,
		&["1.0", "0", "0.3333333333333333", "0.6", "0.3333333333333333"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_similarity_pearson() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::similarity::pearson([1, 2, 3, 4, 5], [1, 2.5, 3.5, 4.2, 5.1]);
		RETURN vector::similarity::pearson([NaN, 1, 2, 3, 4, 5], [NaN, 1, 2.5, 3.5, 4.2, 5.1]);
		RETURN vector::similarity::pearson([1,2,3], [1,5,7]);
	"#,
		&["0.9894065340659606", "NaN", "0.9819805060619659"],
	)
	.await?;

	check_test_is_error(
		r"RETURN vector::similarity::pearson([1, 2, 3], [4, 5]);
		RETURN vector::similarity::pearson([1, 2], [4, 5, 5]);",
		&[
			"Incorrect arguments for function vector::similarity::pearson(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::similarity::pearson(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_distance_euclidean() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::distance::euclidean([1, 2, 3], [1, 2, 3]);
		RETURN vector::distance::euclidean([NaN, 2, 3], [-1, NaN, -3]);
		RETURN vector::distance::euclidean([1, 2, 3], [-1, -2, -3]);
		RETURN vector::distance::euclidean([10, 50, 200], [400, 100, 20]);
		RETURN vector::distance::euclidean([10, 20, 15, 10, 5], [12, 24, 18, 8, 7]);
	"#,
		&["0", "NaN", "7.483314773547883", "432.43496620879307", "6.082762530298219"],
	)
	.await?;
	check_test_is_error(
		r"RETURN vector::distance::euclidean([1, 2, 3], [4, 5]);
			RETURN vector::distance::euclidean([1, 2], [4, 5, 5]);",
		&[
			"Incorrect arguments for function vector::distance::euclidean(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::distance::euclidean(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_distance_manhattan() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::distance::manhattan([1, 2, 3], [4, 5, 6]);
		RETURN vector::distance::manhattan([1, 2, 3], [-4, -5, -6]);
		RETURN vector::distance::manhattan([1.1, 2, 3.3], [4, 5.5, 6.6]);
		RETURN vector::distance::manhattan([NaN, 1, 2, 3], [NaN, 4, 5, 6]);
		RETURN vector::distance::manhattan([10, 20, 15, 10, 5], [12, 24, 18, 8, 7]);
	"#,
		&["9", "21", "9.7", "NaN", "13"],
	)
	.await?;

	check_test_is_error(
		r"RETURN vector::distance::manhattan([1, 2, 3], [4, 5]);
			RETURN vector::distance::manhattan([1, 2], [4, 5, 5]);",
		&[
			"Incorrect arguments for function vector::distance::manhattan(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::distance::manhattan(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_distance_hamming() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::distance::hamming([1, 2, 2], [1, 2, 3]);
		RETURN vector::distance::hamming([-1, -2, -3], [-2, -2, -2]);
		RETURN vector::distance::hamming([1.1, 2.2, -3.3], [1.1, 2, -3.3]);
		RETURN vector::distance::hamming([NaN, 1, 2, 3], [NaN, 1, 2, 3]);
		RETURN vector::distance::hamming([0, 0, 0, 0, 0, 1], [0, 0, 0, 0, 1, 0]);
	"#,
		&["1", "2", "1", "0", "2"],
	)
	.await?;

	check_test_is_error(
		r"RETURN vector::distance::hamming([1, 2, 3], [4, 5]);
			RETURN vector::distance::hamming([1, 2], [4, 5, 5]);",
		&[
			"Incorrect arguments for function vector::distance::hamming(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::distance::hamming(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_distance_minkowski() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::distance::minkowski([1, 2, 3], [4, 5, 6], 3);
		RETURN vector::distance::minkowski([-1, -2, -3], [-4, -5, -6], 3);
		RETURN vector::distance::minkowski([1.1, 2.2, 3], [4, 5.5, 6.6], 3);
		RETURN vector::distance::minkowski([NaN, 1, 2, 3], [NaN, 4, 5, 6], 3);
		RETURN vector::distance::minkowski([10, 20, 15, 10, 5], [12, 24, 18, 8, 7], 1);
		RETURN vector::distance::minkowski([10, 20, 15, 10, 5], [12, 24, 18, 8, 7], 2);
	"#,
		&[
			"4.3267487109222245",
			"4.3267487109222245",
			"4.747193170917638",
			"NaN",
			"13.0",
			"6.082762530298219",
		],
	)
	.await?;

	check_test_is_error(
		r"RETURN vector::distance::minkowski([1, 2, 3], [4, 5], 3);
	RETURN vector::distance::minkowski([1, 2], [4, 5, 5], 3);",
		&[
			"Incorrect arguments for function vector::distance::minkowski(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::distance::minkowski(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_distance_chebyshev() -> Result<(), Error> {
	test_queries(
		r#"
		RETURN vector::distance::chebyshev([1, 2, 3], [4, 5, 6]);
		RETURN vector::distance::chebyshev([-1, -2, -3], [-4, -5, -6]);
		RETURN vector::distance::chebyshev([1.1, 2.2, 3], [4, 5.5, 6.6]);
		RETURN vector::distance::chebyshev([NaN, 1, 2, 3], [NaN, 4, 5, 6]);
		RETURN vector::distance::chebyshev([2, 4, 5, 3, 8, 2], [3, 1, 5, -3, 7, 2]);
	"#,
		&["3.0", "3.0", "3.5999999999999996", "3.0", "6.0"],
	)
	.await?;

	check_test_is_error(
		r"RETURN vector::distance::chebyshev([1, 2, 3], [4, 5]);
	RETURN vector::distance::chebyshev([1, 2], [4, 5, 5]);",
		&[
			"Incorrect arguments for function vector::distance::chebyshev(). The two vectors must be of the same dimension.",
			"Incorrect arguments for function vector::distance::chebyshev(). The two vectors must be of the same dimension."
		]).await?;
	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_head() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("HEAD"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.respond_with(ResponseTemplate::new(200))
		.expect(1)
		.mount(&server)
		.await;

	test_queries(&format!("RETURN http::head('{}/some/path')", server.uri()), &["NONE"]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_get() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.and(header("a-test-header", "with-a-test-value"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some text result"))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN http::get("{}/some/path",{{ 'a-test-header': 'with-a-test-value'}})"#,
		server.uri()
	);
	test_queries(&query, &["'some text result'"]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_put() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("PUT"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
			"some-response": "some-value"
		})))
		.expect(1)
		.mount(&server)
		.await;

	let query =
		format!(r#"RETURN http::put("{}/some/path",{{ 'some-key': 'some-value' }})"#, server.uri());
	test_queries(&query, &[r#"{ "some-response": 'some-value' }"#]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_post() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("POST"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
			"some-response": "some-value"
		})))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN http::post("{}/some/path",{{ 'some-key': 'some-value' }})"#,
		server.uri()
	);
	test_queries(&query, &[r#"{ "some-response": 'some-value' }"#]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_patch() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("PATCH"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
			"some-response": "some-value"
		})))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN http::patch("{}/some/path",{{ 'some-key': 'some-value' }})"#,
		server.uri()
	);
	test_queries(&query, &[r#"{ "some-response": 'some-value' }"#]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_delete() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("DELETE"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.and(header("a-test-header", "with-a-test-value"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some text result"))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN http::delete("{}/some/path",{{ 'a-test-header': 'with-a-test-value'}})"#,
		server.uri()
	);
	test_queries(&query, &["'some text result'"]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(feature = "http")]
#[tokio::test]
pub async fn function_http_error() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.and(header("a-test-header", "with-a-test-value"))
		.respond_with(ResponseTemplate::new(500).set_body_string("some text result"))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN http::get("{}/some/path",{{ 'a-test-header': 'with-a-test-value'}})"#,
		server.uri()
	);

	let res = test_queries(&query, &["NONE"]).await;
	match res {
		Err(Error::Http(text)) => {
			assert_eq!(text, "Internal Server Error");
		}
		e => panic!("query didn't return correct response: {:?}", e),
	}

	server.verify().await;

	Ok(())
}

#[cfg(all(feature = "http", feature = "scripting"))]
#[tokio::test]
pub async fn function_http_get_from_script() -> Result<(), Error> {
	use wiremock::{
		matchers::{header, method, path},
		Mock, ResponseTemplate,
	};

	let server = wiremock::MockServer::start().await;
	Mock::given(method("GET"))
		.and(path("/some/path"))
		.and(header("user-agent", "SurrealDB"))
		.and(header("a-test-header", "with-a-test-value"))
		.respond_with(ResponseTemplate::new(200).set_body_string("some text result"))
		.expect(1)
		.mount(&server)
		.await;

	let query = format!(
		r#"RETURN function() {{
			return await surrealdb.functions.http.get("{}/some/path",{{ 'a-test-header': 'with-a-test-value'}});
		}}"#,
		server.uri()
	);
	test_queries(&query, &["'some text result'"]).await?;

	server.verify().await;

	Ok(())
}

#[cfg(not(feature = "http"))]
#[tokio::test]
pub async fn function_http_disabled() -> Result<(), Error> {
	let res = test_queries("RETURN http::head({})", &["NONE"]).await;
	assert!(matches!(res, Err(Error::HttpDisabled)));
	let res = test_queries("RETURN http::put({})", &["NONE"]).await;
	assert!(matches!(res, Err(Error::HttpDisabled)));
	let res = test_queries("RETURN http::post({})", &["NONE"]).await;
	assert!(matches!(res, Err(Error::HttpDisabled)));
	let res = test_queries("RETURN http::patch({})", &["NONE"]).await;
	assert!(matches!(res, Err(Error::HttpDisabled)));
	let res = test_queries("RETURN http::delete({})", &["NONE"]).await;
	assert!(matches!(res, Err(Error::HttpDisabled)));

	Ok(())
}

// Tests for custom defined functions

#[tokio::test]
async fn function_custom_optional_args() -> Result<(), Error> {
	let sql = r#"
		DEFINE FUNCTION fn::zero_arg() { [] };
		DEFINE FUNCTION fn::one_arg($a: bool) { [$a] };
		DEFINE FUNCTION fn::last_option($a: bool, $b: option<bool>) { [$a, $b] };
		DEFINE FUNCTION fn::middle_option($a: bool, $b: option<bool>, $c: bool) { [$a, $b, $c] };

		RETURN fn::zero_arg();
		RETURN fn::one_arg();
		RETURN fn::last_option();
		RETURN fn::middle_option();

		RETURN fn::zero_arg(true);
		RETURN fn::one_arg(true);
		RETURN fn::last_option(true);
		RETURN fn::last_option(true, false);
		RETURN fn::middle_option(true, false, true);
		RETURN fn::middle_option(true, NONE, true);
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 14);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::InvalidArguments { name, message }) if name == "fn::one_arg" && message == "The function expects 1 argument." => (),
		_ => panic!("Query should have failed with error: Incorrect arguments for function fn::a(). The function expects 1 argument.")
	}
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::InvalidArguments { name, message }) if name == "fn::last_option" && message == "The function expects 1 to 2 arguments." => (),
		_ => panic!("Query should have failed with error: Incorrect arguments for function fn::last_option(). The function expects 1 to 2 arguments.")
	}
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::InvalidArguments { name, message }) if name == "fn::middle_option" && message == "The function expects 3 arguments." => (),
		_ => panic!("Query should have failed with error: Incorrect arguments for function fn::middle_option(). The function expects 3 arguments.")
	}
	//
	match res.remove(0).result {
		Err(surrealdb::error::Db::InvalidArguments { name, message }) if name == "fn::zero_arg" && message == "The function expects 0 arguments." => (),
		_ => panic!("Query should have failed with error: Incorrect arguments for function fn::zero_arg(). The function expects 0 arguments.")
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[true]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[true, NONE]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[true, false]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[true, false, true]");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[true, NONE, true]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_outside_database() -> Result<(), Error> {
	let sql = "RETURN fn::does_not_exist();";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	match res.remove(0).result {
		Err(Error::DbEmpty) => (),
		_ => panic!("Query should have failed with error: Specify a database to use"),
	}

	Ok(())
}
