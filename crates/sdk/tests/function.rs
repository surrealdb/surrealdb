mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::err::Error;
use surrealdb_core::sql::{Expr, FunctionCall};
use surrealdb_core::val::{Array, Number, Table, Value};
use surrealdb_core::{sql, strand, syn};

use crate::helpers::Test;

async fn test_queries(sql: &str, desired_responses: &[&str]) -> Result<()> {
	Test::new(sql).await?.expect_vals(desired_responses)?;
	Ok(())
}

async fn check_test_is_error(sql: &str, expected_errors: &[&str]) -> Result<()> {
	Test::new(sql).await?.expect_errors(expected_errors)?;
	Ok(())
}

/// Macro from the [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#760) crate
/// to assert equality of floats within a specifiable delta.
macro_rules! assert_delta {
	($x:expr, $y:expr) => {
		assert_delta!($x, $y, 1e-5);
	};
	($x:expr, $y:expr, $d:expr) => {
		if ($x - $y).abs() > $d {
			panic!(
				"assertion failed: actual: `{}`, expected: `{}`: \
				actual not within < {} of expected",
				$x, $y, $d
			);
		}
	};
}

#[tokio::test]
async fn error_on_invalid_function() -> Result<()> {
	let dbs = new_ds().await?;
	let query = sql::Ast {
		expressions: vec![sql::TopLevelExpr::Expr(Expr::FunctionCall(Box::new(FunctionCall {
			receiver: sql::Function::Normal("this is an invalid function name".to_string()),
			arguments: Vec::new(),
		})))],
	};
	let session = Session::owner().with_ns("test").with_db("test");
	let mut resp = dbs.process(query, &session, None).await.unwrap();
	assert_eq!(resp.len(), 1);
	let err = resp.pop().unwrap().result.unwrap_err();
	if !matches!(err.downcast_ref(), Some(Error::InvalidFunction { .. })) {
		panic!("returned wrong result {:#?}", err)
	}
	Ok(())
}

// --------------------------------------------------
// rand
// --------------------------------------------------

#[tokio::test]
async fn function_rand_time() -> Result<()> {
	let sql = r#"
		RETURN rand::time();
		RETURN rand::time(1577836800, 1893456000);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_datetime());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_datetime());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_ulid() -> Result<()> {
	let sql = r#"
		RETURN rand::ulid();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_strand());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_ulid_from_datetime() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
        CREATE ONLY test:[rand::ulid()] SET created = time::now(), num = 1;
        SLEEP 100ms;
        LET $rec = CREATE ONLY test:[rand::ulid()] SET created = time::now(), num = 2;
        SLEEP 100ms;
        CREATE ONLY test:[rand::ulid()] SET created = time::now(), num = 3;
		SELECT VALUE num FROM test:[rand::ulid($rec.created - 50ms)]..;
	"#;
	let mut test = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = test.next()?.result;
	tmp.unwrap();
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, syn::value("[2, 3]").unwrap());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid() -> Result<()> {
	let sql = r#"
		RETURN rand::uuid();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_from_datetime() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
        CREATE ONLY test:[rand::uuid()] SET created = time::now(), num = 1;
        SLEEP 100ms;
        LET $rec = CREATE ONLY test:[rand::uuid()] SET created = time::now(), num = 2;
        SLEEP 100ms;
        CREATE ONLY test:[rand::uuid()] SET created = time::now(), num = 3;
		SELECT VALUE num FROM test:[rand::uuid($rec.created - 50ms)]..;
	"#;
	let mut test = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = test.next()?.result;
	tmp.unwrap();
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, syn::value("[2, 3]").unwrap());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_v4() -> Result<()> {
	let sql = r#"
		RETURN rand::uuid::v4();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_v7() -> Result<()> {
	let sql = r#"
		RETURN rand::uuid::v7();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_uuid());
	//
	Ok(())
}

#[tokio::test]
async fn function_rand_uuid_v7_from_datetime() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
        CREATE ONLY test:[rand::uuid::v7()] SET created = time::now(), num = 1;
        SLEEP 100ms;
        LET $rec = CREATE ONLY test:[rand::uuid::v7()] SET created = time::now(), num = 2;
        SLEEP 100ms;
        CREATE ONLY test:[rand::uuid::v7()] SET created = time::now(), num = 3;
		SELECT VALUE num FROM test:[rand::uuid::v7($rec.created - 50ms)]..;
	"#;
	let mut test = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = test.next()?.result;
	tmp.unwrap();
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_none());
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, syn::value("[2, 3]").unwrap());
	//
	Ok(())
}

// --------------------------------------------------
// record
// --------------------------------------------------

#[tokio::test]
async fn function_record_exists() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
		RETURN record::exists(r"person:tobie");
		CREATE ONLY person:tobie;
		RETURN record::exists(r"person:tobie");
	"#;
	let mut test = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = test.next()?.result;
	tmp.unwrap();
	// RETURN record::exists(r"person:tobie");
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	// CREATE ONLY person:tobie;
	let tmp = test.next()?.result?;
	assert!(tmp.is_object());
	// RETURN record::exists(r"person:tobie");
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_record_id() -> Result<()> {
	let sql = r#"
		RETURN record::id(r"person:tobie");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("tobie");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_record_table() -> Result<()> {
	let sql = r#"
		RETURN record::table(r"person:tobie");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("person");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// string
// --------------------------------------------------

#[tokio::test]
async fn function_string_concat() -> Result<()> {
	let sql = r#"
		RETURN string::concat();
		RETURN string::concat("test");
		RETURN string::concat("this", " ", "is", " ", "a", " ", "test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_contains() -> Result<()> {
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
	let mut test = Test::new(sql).await?;
	// 1
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 2
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 3
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 4
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 5
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 6
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 7
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 8
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 9
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 10
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 11
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 12
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 13
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	// 14
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	// 15
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_ends_with() -> Result<()> {
	let sql = r#"
		RETURN string::ends_with("", "");
		RETURN string::ends_with("", "test");
		RETURN string::ends_with("this is a test", "test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[test_log::test(tokio::test)]
async fn function_search_analyzer() -> Result<()> {
	let sql = r#"
        DEFINE FUNCTION fn::stripHtml($html: string) {
            RETURN string::replace($html, /<[^>]*>/, "");
        };
        DEFINE ANALYZER htmlAnalyzer FUNCTION fn::stripHtml TOKENIZERS blank,class;
		RETURN search::analyze('htmlAnalyzer', '<p>This is a <em>sample</em> of HTML</p>');
	"#;
	let mut test = Test::new(sql).await?;

	//
	for _ in 0..2 {
		test.next()?.result?;
	}
	//
	let tmp = test.next()?.result?;
	let val: Value = syn::value("['This', 'is', 'a', 'sample', 'of', 'HTML']").unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn function_string_html_encode() -> Result<()> {
	let sql = r#"
		RETURN string::html::encode("<div>Hello world!</div>");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("&lt;div&gt;Hello&#32;world!&lt;&#47;div&gt;");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_html_sanitize() -> Result<()> {
	let sql = r#"
		RETURN string::html::sanitize("XSS<script>attack</script>");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("XSS");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_alphanum() -> Result<()> {
	let sql = r#"
		RETURN string::is::alphanum("abcdefg123");
		RETURN string::is::alphanum("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_alpha() -> Result<()> {
	let sql = r#"
		RETURN string::is::alpha("abcdefg");
		RETURN string::is::alpha("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_ascii() -> Result<()> {
	let sql = r#"
		RETURN string::is::ascii("abcdefg123");
		RETURN string::is::ascii("this is a test 😀");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_datetime() -> Result<()> {
	let sql = r#"
		RETURN string::is::datetime("2015-09-05 23:56:04", "%Y-%m-%d %H:%M:%S");
		RETURN string::is::datetime("2012-06-22 23:56:04", "%T");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_domain() -> Result<()> {
	let sql = r#"
		RETURN string::is::domain("surrealdb.com");
		RETURN string::is::domain("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_email() -> Result<()> {
	let sql = r#"
		RETURN string::is::email("info@surrealdb.com");
		RETURN string::is::email("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_hexadecimal() -> Result<()> {
	let sql = r#"
		RETURN string::is::hexadecimal("ff009e");
		RETURN string::is::hexadecimal("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_ip() -> Result<()> {
	let sql = r#"
		RETURN string::is::ip("127.0.0.1");
		RETURN string::is::ip("127.0.0");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_ipv4() -> Result<()> {
	let sql = r#"
		RETURN string::is::ipv4("127.0.0.1");
		RETURN string::is::ipv4("127.0.0");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_ipv6() -> Result<()> {
	let sql = r#"
		RETURN string::is::ipv6("::1");
		RETURN string::is::ipv6("200t:db8::");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_latitude() -> Result<()> {
	let sql = r#"
		RETURN string::is::latitude("51.509865");
		RETURN string::is::latitude("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_longitude() -> Result<()> {
	let sql = r#"
		RETURN string::is::longitude("-90.136439");
		RETURN string::is::longitude("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_numeric() -> Result<()> {
	let sql = r#"
		RETURN string::is::numeric("13136439");
		RETURN string::is::numeric("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_semver() -> Result<()> {
	let sql = r#"
		RETURN string::is::semver("1.0.0-rc.1");
		RETURN string::is::semver("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_url() -> Result<()> {
	let sql = r#"
		RETURN string::is::url("https://surrealdb.com/docs");
		RETURN string::is::url("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_ulid() -> Result<()> {
	let sql = r#"
		RETURN string::is::ulid("01J8G788MNX1VT3KE1TK40W350");
		RETURN string::is::ulid("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_uuid() -> Result<()> {
	let sql = r#"
		RETURN string::is::uuid("e72bee20-f49b-11ec-b939-0242ac120002");
		RETURN string::is::uuid("this is a test!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_is_record() -> Result<()> {
	let sql = r#"
		RETURN string::is::record("test:123");
		RETURN string::is::record("invalid record id!");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_join() -> Result<()> {
	let sql = r#"
		RETURN string::join("");
		RETURN string::join("test");
		RETURN string::join(" ", "this", "is", "a", "test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_len() -> Result<()> {
	let sql = r#"
		RETURN string::len("");
		RETURN string::len("test");
		RETURN string::len("test this string");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(0);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(4);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(16);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_lowercase() -> Result<()> {
	let sql = r#"
		RETURN string::lowercase("");
		RETURN string::lowercase("TeSt");
		RETURN string::lowercase("THIS IS A TEST");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is a test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

// "<[^>]*>" , ""

#[tokio::test]
async fn function_string_replace_with_regex() -> Result<()> {
	let sql = r#"
		RETURN string::replace('<p>This is a <em>sample</em> string with <a href="\\#">HTML</a> tags.</p>', /<[^>]*>/, "");
		RETURN string::replace('<p>This one is already <strong>compiled!<strong></p>', /<[^>]*>/, "");
"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("This is a sample string with HTML tags.");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("This one is already compiled!");
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn function_string_matches() -> Result<()> {
	let sql = r#"
		RETURN string::matches("foo", /foo/);
		RETURN string::matches("bar", /foo/);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn function_string_repeat() -> Result<()> {
	let sql = r#"
		RETURN string::repeat("", 3);
		RETURN string::repeat("test", 3);
		RETURN string::repeat("test this", 3);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("testtesttest");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("test thistest thistest this");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_replace() -> Result<()> {
	let sql = r#"
		RETURN string::replace("", "", "");
		RETURN string::replace('this is a test', 'a test', 'awesome');
		RETURN string::replace("this is an 😀 emoji test", "😀", "awesome 👍");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is awesome");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is an awesome 👍 emoji test");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_reverse() -> Result<()> {
	let sql = r#"
		RETURN string::reverse("");
		RETURN string::reverse("test");
		RETURN string::reverse("test this string");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("tset");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("gnirts siht tset");
	assert_eq!(tmp, val);
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#786)
#[tokio::test]
async fn function_string_distance_hamming() -> Result<()> {
	let sql = r#"
		RETURN string::distance::hamming("", "");
		RETURN string::distance::hamming("hamming", "hamming");
		RETURN string::distance::hamming("hamming", "hammers");
		RETURN string::distance::hamming("hamming", "h香mmüng");;
		RETURN string::distance::hamming("Friedrich Nietzs", "Jean-Paul Sartre");
	"#;
	let mut test = Test::new(sql).await?;
	// hamming_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// hamming_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// hamming_diff
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// hamming_diff_multibyte
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(2));
	// hamming_names
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(14));

	check_test_is_error(
		r#"RETURN string::distance::hamming("ham", "hamming");"#,
		&[
			"Incorrect arguments for function string::distance::hamming(). Strings must be of equal length.",
		],
	)
	.await?;

	Ok(())
}

#[tokio::test]
async fn function_string_distance_damerau() -> Result<()> {
	let sql = r#"
		RETURN string::distance::damerau_levenshtein("", "");
		RETURN string::distance::damerau_levenshtein("damerau", "damerau");
		RETURN string::distance::damerau_levenshtein("", "damerau");
		RETURN string::distance::damerau_levenshtein("damerau", "");
		RETURN string::distance::damerau_levenshtein("ca", "abc");
		RETURN string::distance::damerau_levenshtein("damerau", "aderua");
		RETURN string::distance::damerau_levenshtein("aderua", "damerau");
		RETURN string::distance::damerau_levenshtein("öঙ香", "abc");
		RETURN string::distance::damerau_levenshtein("abc", "öঙ香");
		RETURN string::distance::damerau_levenshtein("damerau", "aderuaxyz");
		RETURN string::distance::damerau_levenshtein("aderuaxyz", "damerau");
		RETURN string::distance::damerau_levenshtein("Stewart", "Colbert");
		RETURN string::distance::damerau_levenshtein("abcdefghijkl", "bacedfgihjlk");
		RETURN string::distance::damerau_levenshtein(
			"The quick brown fox jumped over the angry dog.",
			"Lehem ipsum dolor sit amet, dicta latine an eam."
		);
		RETURN string::distance::damerau_levenshtein("foobar", "ofobar");
		RETURN string::distance::damerau_levenshtein("specter", "spectre");
		RETURN string::distance::damerau_levenshtein("a cat", "an abct");
	"#;
	let mut test = Test::new(sql).await?;
	// damerau_levenshtein_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// damerau_levenshtein_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// damerau_levenshtein_first_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(7));
	// damerau_levenshtein_second_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(7));
	// damerau_levenshtein_diff
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(2));
	// damerau_levenshtein_diff_short
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// damerau_levenshtein_diff_reversed
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// damerau_levenshtein_diff_multibyte
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// damerau_levenshtein_diff_unequal_length
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(6));
	// damerau_levenshtein_diff_unequal_length_reversed
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(6));
	// damerau_levenshtein_diff_comedians
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(5));
	// damerau_levenshtein_many_transpositions
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(4));
	// damerau_levenshtein_diff_longer
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(36));
	// damerau_levenshtein_beginning_transposition
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1));
	// damerau_levenshtein_end_transposition
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1));
	// damerau_levenshtein_unrestricted_edit
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#1223)
#[tokio::test]
async fn function_string_distance_normalized_damerau_levenshtein() -> Result<()> {
	let sql = r#"
		RETURN string::distance::normalized_damerau_levenshtein("levenshtein", "löwenbräu");
		RETURN string::distance::normalized_damerau_levenshtein("", "");
		RETURN string::distance::normalized_damerau_levenshtein("", "flower");
		RETURN string::distance::normalized_damerau_levenshtein("tree", "");
		RETURN string::distance::normalized_damerau_levenshtein("sunglasses", "sunglasses");
	"#;
	let mut test = Test::new(sql).await?;
	// normalized_damerau_levenshtein_diff_short
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.27272);
	// normalized_damerau_levenshtein_for_empty_strings
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	// normalized_damerau_levenshtein_first_empty
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	// normalized_damerau_levenshtein_second_empty
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	// normalized_damerau_levenshtein_identical_strings
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#989)
#[tokio::test]
async fn function_string_distance_levenshtein() -> Result<()> {
	let sql = r#"
    RETURN string::distance::levenshtein("", "");
    RETURN string::distance::levenshtein("levenshtein", "levenshtein");
    RETURN string::distance::levenshtein("kitten", "sitting");
    RETURN string::distance::levenshtein("hello, world", "bye, world");
    RETURN string::distance::levenshtein("öঙ香", "abc");
    RETURN string::distance::levenshtein("abc", "öঙ香");
    RETURN string::distance::levenshtein(
        "The quick brown fox jumped over the angry dog.",
        "Lorem ipsum dolor sit amet, dicta latine an eam."
    );
    RETURN string::distance::levenshtein("", "sitting");
    RETURN string::distance::levenshtein("kitten", "");
"#;
	let mut test = Test::new(sql).await?;
	// levenshtein_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// levenshtein_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// levenshtein_diff_short
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// levenshtein_diff_with_space
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(5));
	// levenshtein_diff_multibyte
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// levenshtein_diff_longer
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(37));
	// levenshtein_first_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(7));
	// levenshtein_second_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(6));
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#1032)
#[tokio::test]
async fn function_string_distance_normalized_levenshtein() -> Result<()> {
	let sql = r#"
		RETURN string::distance::normalized_levenshtein("kitten", "sitting");
		RETURN string::distance::normalized_levenshtein("", "");
		RETURN string::distance::normalized_levenshtein("", "second");
		RETURN string::distance::normalized_levenshtein("first", "");
		RETURN string::distance::normalized_levenshtein("identical", "identical");
	"#;
	let mut test = Test::new(sql).await?;
	// normalized_levenshtein_diff_short
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.57142);
	// normalized_levenshtein_for_empty_strings
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	// normalized_levenshtein_first_empty
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	// normalized_levenshtein_second_empty
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	// normalized_levenshtein_identical_strings
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#1057)
/// which, in turn, are taken from [`aceakash/string-similarity`](https://github.com/aceakash/string-similarity/blob/f83ba3cd7bae874c20c429774e911ae8cff8bced/src/spec/index.spec.js#L11)
#[tokio::test]
async fn function_string_distance_osa_distance() -> Result<()> {
	let sql = r#"
        RETURN string::distance::osa_distance("", "");
        RETURN string::distance::osa_distance("damerau", "damerau");
        RETURN string::distance::osa_distance("", "damerau");
        RETURN string::distance::osa_distance("damerau", "");
        RETURN string::distance::osa_distance("ca", "abc");
        RETURN string::distance::osa_distance("damerau", "aderua");
        RETURN string::distance::osa_distance("aderua", "damerau");
        RETURN string::distance::osa_distance("öঙ香", "abc");
        RETURN string::distance::osa_distance("abc", "öঙ香");
        RETURN string::distance::osa_distance("damerau", "aderuaxyz");
        RETURN string::distance::osa_distance("aderuaxyz", "damerau");
        RETURN string::distance::osa_distance("Stewart", "Colbert");
        RETURN string::distance::osa_distance("abcdefghijkl", "bacedfgihjlk");
        RETURN string::distance::osa_distance(
            "The quick brown fox jumped over the angry dog.",
            "Lehem ipsum dolor sit amet, dicta latine an eam."
        );
        RETURN string::distance::osa_distance("foobar", "ofobar");
        RETURN string::distance::osa_distance("specter", "spectre");
        RETURN string::distance::osa_distance("a cat", "an abct");
    "#;
	let mut test = Test::new(sql).await?;
	// osa_distance_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// osa_distance_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	// osa_distance_first_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(7));
	// osa_distance_second_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(7));
	// osa_distance_diff
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// osa_distance_diff_short
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// osa_distance_diff_reversed
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// osa_distance_diff_multibyte
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(3));
	// osa_distance_diff_unequal_length
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(6));
	// osa_distance_diff_unequal_length_reversed
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(6));
	// osa_distance_diff_comedians
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(5));
	// osa_distance_many_transpositions
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(4));
	// osa_distance_diff_longer
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(36));
	// osa_distance_beginning_transposition
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1));
	// osa_distance_end_transposition
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1));
	// osa_distance_restricted_edit
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(4));
	//
	Ok(())
}

#[tokio::test]
async fn function_string_similarity_fuzzy() -> Result<()> {
	let sql = r#"
		RETURN string::similarity::fuzzy("", "");
		RETURN string::similarity::fuzzy("some", "text");
		RETURN string::similarity::fuzzy("text", "TEXT");
		RETURN string::similarity::fuzzy("TEXT", "TEXT");
		RETURN string::similarity::fuzzy("this could be a tricky test", "this test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(83));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(91));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(174));
	//
	Ok(())
}

#[tokio::test]
async fn function_string_similarity_smithwaterman() -> Result<()> {
	let sql = r#"
		RETURN string::similarity::smithwaterman("", "");
		RETURN string::similarity::smithwaterman("some", "text");
		RETURN string::similarity::smithwaterman("text", "TEXT");
		RETURN string::similarity::smithwaterman("TEXT", "TEXT");
		RETURN string::similarity::smithwaterman("this could be a tricky test", "this test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(83));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(91));
	//
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(174));
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#829)
#[tokio::test]
async fn function_string_similarity_jaro() -> Result<()> {
	let sql = r#"
		RETURN string::similarity::jaro("", "");
		RETURN string::similarity::jaro("", "jaro");
		RETURN string::similarity::jaro("distance", "");
		RETURN string::similarity::jaro("jaro", "jaro");
		RETURN string::similarity::jaro("a", "b");
		RETURN string::similarity::jaro("a", "a");

		RETURN string::similarity::jaro("testabctest", "testöঙ香test");
		RETURN string::similarity::jaro("testöঙ香test", "testabctest");
		RETURN string::similarity::jaro("dixon", "dicksonx");
		RETURN string::similarity::jaro("a", "ab");
		RETURN string::similarity::jaro("ab", "a");
		RETURN string::similarity::jaro("dwayne", "duane");
		RETURN string::similarity::jaro("martha", "marhta");
		RETURN string::similarity::jaro("a jke", "jane a k");
		RETURN string::similarity::jaro("Friedrich Nietzsche", "Jean-Paul Sartre");
	"#;
	let mut test = Test::new(sql).await?;
	// jaro_both_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));
	// jaro_first_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_second_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));
	// jaro_diff_one_character
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_same_one_character
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));

	// jaro_multibyte
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.818, 0.001);
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.818, 0.001);
	// jaro_diff_short
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.767, 0.001);
	// jaro_diff_one_and_two
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.83, 0.01);
	// jaro_diff_two_and_one
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.83, 0.01);
	// jaro_diff_no_transposition
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.822, 0.001);
	// jaro_diff_with_transposition
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.944, 0.001);
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.6, 0.001);
	// jaro_names
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.392, 0.001);
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#904)
#[tokio::test]
async fn function_string_similarity_jaro_winkler() -> Result<()> {
	let sql = r#"
		RETURN string::similarity::jaro_winkler("", "");
		RETURN string::similarity::jaro_winkler("", "jaro-winkler");
		RETURN string::similarity::jaro_winkler("distance", "");
		RETURN string::similarity::jaro_winkler("Jaro-Winkler", "Jaro-Winkler");
		RETURN string::similarity::jaro_winkler("a", "b");
		RETURN string::similarity::jaro_winkler("a", "a");

		RETURN string::similarity::jaro_winkler("testabctest", "testöঙ香test");
		RETURN string::similarity::jaro_winkler("testöঙ香test", "testabctest");
		RETURN string::similarity::jaro_winkler("dixon", "dicksonx");
		RETURN string::similarity::jaro_winkler("dicksonx", "dixon");
		RETURN string::similarity::jaro_winkler("dwayne", "duane");
		RETURN string::similarity::jaro_winkler("martha", "marhta");
		RETURN string::similarity::jaro_winkler("a jke", "jane a k");
		RETURN string::similarity::jaro_winkler("Friedrich Nietzsche", "Fran-Paul Sartre");
		RETURN string::similarity::jaro_winkler("cheeseburger", "cheese fries");
		RETURN string::similarity::jaro_winkler("Thorkel", "Thorgier");
		RETURN string::similarity::jaro_winkler("Dinsdale", "D");
		RETURN string::similarity::jaro_winkler("thequickbrownfoxjumpedoverx", "thequickbrownfoxjumpedovery");
	"#;
	let mut test = Test::new(sql).await?;
	// jaro_winkler_both_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));
	// jaro_winkler_first_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_winkler_second_empty
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_winkler_same
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));
	// jaro_winkler_diff_one_character
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(0.0));
	// jaro_winkler_same_one_character
	let tmp = test.next()?.result?;
	assert_eq!(tmp, Value::from(1.0));

	// jaro_winkler_multibyte
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.89, 0.001);
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.89, 0.001);
	// jaro_winkler_diff_short
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.813, 0.001);
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.813, 0.001);
	// jaro_winkler_diff_no_transposition
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.84, 0.001);
	// jaro_winkler_diff_with_transposition
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.961, 0.001);
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.6, 0.001);
	// jaro_winkler_names
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.452, 0.001);
	// jaro_winkler_long_prefix
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.866, 0.001);
	// jaro_winkler_more_names
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.868, 0.001);
	// jaro_winkler_length_of_one
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.738, 0.001);
	// jaro_winkler_very_long_prefix
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.98519);
	//
	Ok(())
}

/// Test cases taken from [`strsim`](https://docs.rs/strsim/0.11.1/src/strsim/lib.rs.html#1254)
#[tokio::test]
async fn function_string_similarity_sorensen_dice() -> Result<()> {
	let sql = r#"
		RETURN string::similarity::sorensen_dice("a", "a");
		RETURN string::similarity::sorensen_dice("a", "b");
		RETURN string::similarity::sorensen_dice("", "");
		RETURN string::similarity::sorensen_dice("a", "");
		RETURN string::similarity::sorensen_dice("", "a");
		RETURN string::similarity::sorensen_dice("apple event", "apple    event");
		RETURN string::similarity::sorensen_dice("iphone", "iphone x");
		RETURN string::similarity::sorensen_dice("french", "quebec");
		RETURN string::similarity::sorensen_dice("france", "france");
		RETURN string::similarity::sorensen_dice("fRaNce", "france");
		RETURN string::similarity::sorensen_dice("healed", "sealed");
		RETURN string::similarity::sorensen_dice("web applications", "applications of the web");
		RETURN string::similarity::sorensen_dice("this will have a typo somewhere", "this will huve a typo somewhere");
		RETURN string::similarity::sorensen_dice(
			"Olive-green table for sale, in extremely good condition.",
			"For sale: table in very good  condition, olive green in colour."
		);
		RETURN string::similarity::sorensen_dice(
			"Olive-green table for sale, in extremely good condition.",
			"For sale: green Subaru Impreza, 210,000 miles"
		);
		RETURN string::similarity::sorensen_dice(
			"Olive-green table for sale, in extremely good condition.",
			"Wanted: mountain bike with at least 21 gears."
		);
		RETURN string::similarity::sorensen_dice("this has one extra word", "this has one word");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.90909);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 1.0);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.2);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.8);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.78788);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.92);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.60606);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.25581);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.14118);
	//
	let tmp: f64 = test.next()?.result?.into_float().unwrap();
	assert_delta!(tmp, 0.77419);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_slice() -> Result<()> {
	let sql = r#"
		RETURN string::slice("the quick brown fox jumps over the lazy dog.");
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, 60);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 0, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", 16, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", -9, -1);
		RETURN string::slice("the quick brown fox jumps over the lazy dog.", -100, -100);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'the quick brown fox jumps over the lazy dog.'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'fox jumps over the lazy dog.'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'the quick brown fox jumps over the lazy dog.'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'the quick brown fox jumps over the lazy dog'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'fox jumps over the lazy dog'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'lazy dog'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("''").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_slug() -> Result<()> {
	let sql = r#"
		RETURN string::slug("");
		RETURN string::slug("this is a test");
		RETURN string::slug("blog - this is a test with 😀 emojis");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this-is-a-test");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("blog-this-is-a-test-with-grinning-emojis");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_split() -> Result<()> {
	let sql = r#"
		RETURN string::split("", "");
		RETURN string::split("this, is, a, list", ", ");
		RETURN string::split("this - is - another - test", " - ");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("['', '']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("['this', 'is', 'a', 'list']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("['this', 'is', 'another', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_starts_with() -> Result<()> {
	let sql = r#"
		RETURN string::starts_with("", "");
		RETURN string::starts_with("", "test");
		RETURN string::starts_with("test this string", "test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_trim() -> Result<()> {
	let sql = r#"
		RETURN string::trim("");
		RETURN string::trim("test");
		RETURN string::trim("   this is a test with text   ");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("test");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("this is a test with text");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_uppercase() -> Result<()> {
	let sql = r#"
		RETURN string::uppercase("");
		RETURN string::uppercase("tEsT");
		RETURN string::uppercase("this is a test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("TEST");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("THIS IS A TEST");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_string_words() -> Result<()> {
	let sql = r#"
		RETURN string::words("");
		RETURN string::words("test");
		RETURN string::words("this is a test");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Array::new().into();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("['test']").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("['this', 'is', 'a', 'test']").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// time
// --------------------------------------------------

#[tokio::test]
async fn function_time_ceil() -> Result<()> {
	let sql = r#"
		RETURN time::ceil(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::ceil(d"1987-06-22T08:30:45Z", 1y);
		RETURN time::ceil(d"2023-05-11T03:09:00Z", 1s);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-25T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-12-28T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2023-05-11T03:09:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_day() -> Result<()> {
	let sql = r#"
		RETURN time::day();
		RETURN time::day(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(22);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_floor() -> Result<()> {
	let sql = r#"
		RETURN time::floor(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::floor(d"1987-06-22T08:30:45Z", 1y);
		RETURN time::floor(d"2023-05-11T03:09:00Z", 1s);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-18T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1986-12-28T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2023-05-11T03:09:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_format() -> Result<()> {
	let sql = r#"
		RETURN time::format(d"1987-06-22T08:30:45Z", "%Y-%m-%d");
		RETURN time::format(d"1987-06-22T08:30:45Z", "%T");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'1987-06-22'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("'08:30:45'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_group() -> Result<()> {
	let sql = r#"
		RETURN time::group(d"1987-06-22T08:30:45Z", 'hour');
		RETURN time::group(d"1987-06-22T08:30:45Z", 'month');
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-22T08:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-01T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_hour() -> Result<()> {
	let sql = r#"
		RETURN time::hour();
		RETURN time::hour(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(8);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_is_leap_year() -> Result<()> {
	let sql = r#"
		RETURN time::is::leap_year();
		RETURN time::is::leap_year(d"1987-06-22T08:30:45Z");
		RETURN time::is::leap_year(d"1988-06-22T08:30:45Z");
		RETURN d'2024-09-03T02:33:15.349397Z'.is_leap_year();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_bool());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_min() -> Result<()> {
	let sql = r#"
		RETURN time::min([d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z"]);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-22T08:30:45Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_max() -> Result<()> {
	let sql = r#"
		RETURN time::max([d"1987-06-22T08:30:45Z", d"1988-06-22T08:30:45Z"]);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1988-06-22T08:30:45Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_minute() -> Result<()> {
	let sql = r#"
		RETURN time::minute();
		RETURN time::minute(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(30);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_month() -> Result<()> {
	let sql = r#"
		RETURN time::month();
		RETURN time::month(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(6);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_nano() -> Result<()> {
	let sql = r#"
		RETURN time::nano();
		RETURN time::nano(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(551349045000000000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_micros() -> Result<()> {
	let sql = r#"
		RETURN time::micros();
		RETURN time::micros(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(551349045000000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_millis() -> Result<()> {
	let sql = r#"
		RETURN time::millis();
		RETURN time::millis(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(551349045000i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_now() -> Result<()> {
	let sql = r#"
		RETURN time::now();
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_datetime());
	//
	Ok(())
}

#[tokio::test]
async fn function_time_round() -> Result<()> {
	let sql = r#"
		RETURN time::round(d"1987-06-22T08:30:45Z", 1w);
		RETURN time::round(d"1987-06-22T08:30:45Z", 1y);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-25T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1986-12-28T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_second() -> Result<()> {
	let sql = r#"
		RETURN time::second();
		RETURN time::second(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(45);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_unix() -> Result<()> {
	let sql = r#"
		RETURN time::unix();
		RETURN time::unix(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(551349045);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_wday() -> Result<()> {
	let sql = r#"
		RETURN time::wday();
		RETURN time::wday(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(1);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_week() -> Result<()> {
	let sql = r#"
		RETURN time::week();
		RETURN time::week(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(26);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_yday() -> Result<()> {
	let sql = r#"
		RETURN time::yday();
		RETURN time::yday(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(173);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_year() -> Result<()> {
	let sql = r#"
		RETURN time::year();
		RETURN time::year(d"1987-06-22T08:30:45Z");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	assert!(tmp.is_number());
	//
	let tmp = test.next()?.result?;
	let val = Value::from(1987);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_nanos() -> Result<()> {
	let sql = r#"
		RETURN time::from::nanos(384025770384840000);
		RETURN time::from::nanos(2840257704384440000);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1982-03-03T17:49:30.384840Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2060-01-02T08:28:24.384440Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_micros() -> Result<()> {
	let sql = r#"
		RETURN time::from::micros(384025770384840);
		RETURN time::from::micros(2840257704384440);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1982-03-03T17:49:30.384840Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2060-01-02T08:28:24.384440Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_millis() -> Result<()> {
	let sql = r#"
		RETURN time::from::millis(384025773840);
		RETURN time::from::millis(2840257704440);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1982-03-03T17:49:33.840Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2060-01-02T08:28:24.440Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_secs() -> Result<()> {
	let sql = r#"
		RETURN time::from::secs(384053840);
		RETURN time::from::secs(2845704440);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1982-03-04T01:37:20Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2060-03-05T09:27:20Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_ulid() -> Result<()> {
	let sql = r#"
		RETURN time::from::ulid("01J8G788MNX1VT3KE1TK40W350");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2024-09-23T19:55:34.933Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_unix() -> Result<()> {
	let sql = r#"
		RETURN time::from::unix(384053840);
		RETURN time::from::unix(2845704440);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1982-03-04T01:37:20Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2060-03-05T09:27:20Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_time_from_unix_limit_and_beyond() -> Result<()> {
	test_queries(
		r#"
		RETURN time::year(time::from::unix(-8334601228800));
		RETURN time::year(time::from::unix(8210266876799));
		"#,
		&["-262143", "262142"],
	)
	.await?;

	check_test_is_error(
		r#"
		RETURN time::from::unix(-8334601228801);
		RETURN time::from::unix(8210266876800);
	"#,
		&[
			"Incorrect arguments for function time::from::unix(). The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z.",
			"Incorrect arguments for function time::from::unix(). The argument must be a number of seconds relative to January 1, 1970 0:00:00 UTC that produces a datetime between -262143-01-01T00:00:00Z and +262142-12-31T23:59:59Z."
		],
	).await?;

	Ok(())
}

#[tokio::test]
async fn function_time_from_uuid() -> Result<()> {
	let sql = r#"
		RETURN time::from::uuid(u'01922074-2295-7cf6-906f-bcd0810639b0');
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2024-09-23T19:55:34.933Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// type
// --------------------------------------------------

#[tokio::test]
async fn function_type_bool() -> Result<()> {
	let sql = r#"
		RETURN type::bool("true");
		RETURN type::bool("false");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Bool(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_datetime() -> Result<()> {
	let sql = r#"
		RETURN type::datetime("1987-06-22");
		RETURN type::datetime("2022-08-01");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'1987-06-22T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("d'2022-08-01T00:00:00Z'").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_decimal() -> Result<()> {
	let sql = r#"
		RETURN type::decimal("0.0");
		RETURN type::decimal("13.1043784018");
		RETURN type::decimal("13.5719384719384719385639856394139476937756394756");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Number(Number::Decimal("0".parse().unwrap()));
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Number(Number::Decimal("13.1043784018".parse().unwrap()));
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Number(Number::Decimal(
		"13.571938471938471938563985639413947693775639".parse().unwrap(),
	));
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_duration() -> Result<()> {
	let sql = r#"
		RETURN type::duration("1h30m");
		RETURN type::duration("1h30m30s50ms");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("1h30m").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("1h30m30s50ms").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_float() -> Result<()> {
	let sql = r#"
		RETURN type::float("13.1043784018");
		RETURN type::float("13.5719384719384719385639856394139476937756394756");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(13.1043784018f64);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(13.571938471938472f64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_int() -> Result<()> {
	let sql = r#"
		RETURN type::int("194719");
		RETURN type::int("1457105732053058");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(194719i64);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(1457105732053058i64);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_array() -> Result<()> {
	let sql = r#"
		RETURN type::is::array([1, 2, 3]);
		RETURN type::is::array("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_bool() -> Result<()> {
	let sql = r#"
		RETURN type::is::bool(true);
		RETURN type::is::bool("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_bytes() -> Result<()> {
	let sql = r#"
		RETURN type::is::bytes(<bytes>"");
		RETURN type::is::bytes("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_collection() -> Result<()> {
	let sql = r#"
		LET $collection = <geometry<collection>> {
			type: 'GeometryCollection',
			geometries: [{ type: 'MultiPoint', coordinates: [[10, 11.2], [10.5, 11.9]] }]
		};
		RETURN type::is::collection($collection);
		RETURN type::is::collection("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_datetime() -> Result<()> {
	let sql = r#"
		RETURN type::is::datetime(<datetime> d"2023-09-04T11:22:38.247Z");
		RETURN type::is::datetime("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_decimal() -> Result<()> {
	let sql = r#"
		RETURN type::is::decimal(1.0dec);
		RETURN type::is::decimal("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_duration() -> Result<()> {
	let sql = r#"
		RETURN type::is::duration(20s);
		RETURN type::is::duration("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_float() -> Result<()> {
	let sql = r#"
		RETURN type::is::float(1.0f);
		RETURN type::is::float("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_geometry() -> Result<()> {
	let sql = r#"
		RETURN type::is::geometry((-0.118092, 51.509865));
		RETURN type::is::geometry("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_int() -> Result<()> {
	let sql = r#"
		RETURN type::is::int(123);
		RETURN type::is::int("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_line() -> Result<()> {
	let sql = r#"
		LET $line = <geometry<line>> { type: 'LineString', coordinates: [[10, 11.2], [10.5, 11.9]] };
		RETURN type::is::line($line);
		RETURN type::is::line("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_none() -> Result<()> {
	let sql = r#"
		RETURN type::is::none(none);
		RETURN type::is::none("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_null() -> Result<()> {
	let sql = r#"
		RETURN type::is::null(null);
		RETURN type::is::null("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multiline() -> Result<()> {
	let sql = r#"
		LET $multiline = <geometry<multiline>> {
			type: 'MultiLineString',
			coordinates: [[[10, 11.2], [10.5, 11.9]], [[11, 12.2], [11.5, 12.9], [12, 13]]]
		};
		RETURN type::is::multiline($multiline);
		RETURN type::is::multiline("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multipoint() -> Result<()> {
	let sql = r#"
		LET $multipoint = <geometry<multipoint>> { type: 'MultiPoint', coordinates: [[10, 11.2], [10.5, 11.9]] };
		RETURN type::is::multipoint($multipoint);
		RETURN type::is::multipoint("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_multipolygon() -> Result<()> {
	let sql = r#"
		LET $multipolygon = <geometry<multipolygon>> {
			type: 'MultiPolygon',
			coordinates: [[[[10, 11.2], [10.5, 11.9], [10.8, 12], [10, 11.2]]], [[[9, 11.2], [10.5, 11.9], [10.3, 13], [9, 11.2]]]]
		};
		RETURN type::is::multipolygon($multipolygon);
		RETURN type::is::multipolygon("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_number() -> Result<()> {
	let sql = r#"
		RETURN type::is::number(123);
		RETURN type::is::number(123.0f);
		RETURN type::is::number(123.0dec);
		RETURN type::is::number("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_object() -> Result<()> {
	let sql = r#"
		RETURN type::is::object({ test: 123 });
		RETURN type::is::object("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_point() -> Result<()> {
	let sql = r#"
		LET $point = <geometry<point>> { type: "Point", coordinates: [-0.118092, 51.509865] };
		RETURN type::is::point($point);
		RETURN type::is::point("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_polygon() -> Result<()> {
	let sql = r#"
		LET $polygon = <geometry<polygon>> {
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386],
					[0.1785278, 51.37692386],
					[0.1785278, 51.6146057],
					[-0.38314819, 51.6146057],
					[-0.38314819, 51.37692386]
				]
			]
		};
		RETURN type::is::polygon($polygon);
		RETURN type::is::polygon("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_range() -> Result<()> {
	let sql = r#"
		RETURN type::is::range(1..5);
		RETURN type::is::range("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_record() -> Result<()> {
	let sql = r#"
		RETURN type::is::record(person:john);
		RETURN type::is::record("123");
		RETURN type::is::record(person:john, 'person');
		RETURN type::is::record(person:john, 'user');
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_string() -> Result<()> {
	let sql = r#"
		RETURN type::is::string("testing!");
		RETURN type::is::string(123);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_is_uuid() -> Result<()> {
	let sql = r#"
		RETURN type::is::uuid(<uuid> u"018a6065-a80a-765e-b640-9fcb330a2f4f");
		RETURN type::is::uuid("123");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from(true);
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from(false);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_number() -> Result<()> {
	let sql = r#"
		RETURN type::number("194719.1947104740");
		RETURN type::number("1457105732053058.3957394823281756381849375");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("194719.1947104740").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value("1457105732053058.3957394823281756381849375").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_point() -> Result<()> {
	let sql = r#"
		RETURN type::point([1.345, 6.789]);
		RETURN type::point([-0.136439, 51.509865]);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"{
			type: 'Point',
			coordinates: [
				1.345,
				6.789
			]
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"{
			type: 'Point',
			coordinates: [
				-0.136439,
				51.509865
			]
		}",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_string() -> Result<()> {
	let sql = r#"
		RETURN type::string(30s);
		RETURN type::string(13);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("30s");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("13");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_string_lossy() -> Result<()> {
	// First bytes are a bit invalid, second are fine
	let sql = r#"
		type::string_lossy(<bytes>[83, 117, 114, 255, 114, 101, 97, 254, 108, 68, 66]);
		type::string_lossy(<bytes>[ 83, 117, 114, 114, 101, 97, 108, 68, 66 ]);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::from("Sur�rea�lDB");
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::from("SurrealDB");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_table() -> Result<()> {
	let sql = r#"
		RETURN type::table("person");
		RETURN type::table("animal");
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = Value::Table(Table::from_strand(strand!("person").to_owned()));
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = Value::Table(Table::from_strand(strand!("animal").to_owned()));
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_type_thing() -> Result<()> {
	let sql = r#"
		USE NS test DB test;
		CREATE type::thing('person', 'test');
		CREATE type::thing('person', 1434619);
		CREATE type::thing(<string> person:john);
		CREATE type::thing('city', '8e60244d-95f6-4f95-9e30-09a98977efb0');
		CREATE type::thing('temperature', ['London', '2022-09-30T20:25:01.406828Z']);
	"#;
	let mut test = Test::new(sql).await?;
	// USE NS test DB test;
	let tmp = test.next()?.result;
	tmp.unwrap();
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"[
			{
				id: person:test,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"[
			{
				id: person:1434619,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"[
			{
				id: person:john,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"[
			{
				id: city:⟨8e60244d-95f6-4f95-9e30-09a98977efb0⟩,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		"[
			{
				id: temperature:['London', '2022-09-30T20:25:01.406828Z'],
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// value
// --------------------------------------------------

#[tokio::test]
async fn function_value_diff() -> Result<()> {
	let sql = r#"
		RETURN value::diff({ a: 1, b: 2 }, { c: 3, b: 2 });
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value(
		r#"
		[
			{
				op: 'remove',
				path: '/a'
			},
			{
				op: 'add',
				path: '/c',
				value: 3
			}
		]
	"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn function_value_patch() -> Result<()> {
	let sql = r#"
		RETURN value::patch({ a: 1, b: 2 }, [
			{
				op: 'remove',
				path: '/a'
			},
			{
				op: 'add',
				path: '/c',
				value: 3
			}
		]);
	"#;
	let mut test = Test::new(sql).await?;
	//
	let tmp = test.next()?.result?;
	let val = syn::value("{ b: 2, c: 3 }").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

// --------------------------------------------------
// vector
// --------------------------------------------------

#[tokio::test]
async fn function_vector_add() -> Result<()> {
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
			"Incorrect arguments for function vector::add(). The two vectors must be of the same dimension.",
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_angle() -> Result<()> {
	Test::new(
		r#"
		RETURN vector::angle([1,0,0], [0,1,0]);
		RETURN vector::angle([5, 10, 15], [10, 5, 20]);
		RETURN vector::angle([-3, 2, 5], [4, -1, 2]);
		RETURN vector::angle([NaN, 2, 3], [-1, -2, NaN]);
	"#,
	)
	.await?
	.expect_vals(&["1.5707963267948966", "0.36774908225917935", "1.7128722906354115"])?
	.expect_value(Value::Number(Number::NAN))?;

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
async fn function_vector_cross() -> Result<()> {
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
			"Incorrect arguments for function vector::cross(). Both vectors must have a dimension of 3.",
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_dot() -> Result<()> {
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
			"Incorrect arguments for function vector::dot(). The two vectors must be of the same dimension.",
		],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_magnitude() -> Result<()> {
	test_queries(
		r#"
		RETURN vector::magnitude([]);
		RETURN vector::magnitude([1]);
		RETURN vector::magnitude([5]);
		RETURN vector::magnitude([1,2,3,3,3,4,5]);
	"#,
		&["0f", "1f", "5f", "8.54400374531753"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_normalize() -> Result<()> {
	test_queries(
		r#"
		RETURN vector::normalize([]);
		RETURN vector::normalize([1]);
		RETURN vector::normalize([5]);
		RETURN vector::normalize([4,3]);
	"#,
		&["[]", "[1f]", "[1f]", "[0.8,0.6]"],
	)
	.await?;
	Ok(())
}

#[tokio::test]
async fn function_vector_multiply() -> Result<()> {
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
async fn function_vector_project() -> Result<()> {
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
async fn function_vector_divide() -> Result<()> {
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
async fn function_vector_subtract() -> Result<()> {
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
async fn function_vector_similarity_cosine() -> Result<()> {
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
async fn function_vector_similarity_jaccard() -> Result<()> {
	test_queries(
		r#"
		RETURN vector::similarity::jaccard([1, 2, 3], [3, 2, 1]);
		RETURN vector::similarity::jaccard([1, 2, 3], [-3, -2, -1]);
		RETURN vector::similarity::jaccard([1, -2, 3, -4], [4, 3, 2, 1]);
		RETURN vector::similarity::jaccard([NaN, 1, 2, 3], [NaN, 2, 3, 4]);
		RETURN vector::similarity::jaccard([0,1,2,5,6], [0,2,3,4,5,7,9]);
	"#,
		&["1.0", "0f", "0.3333333333333333", "0.6", "0.3333333333333333"],
	)
	.await
}

#[tokio::test]
async fn function_vector_similarity_pearson() -> Result<()> {
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
async fn function_vector_distance_euclidean() -> Result<()> {
	test_queries(
		r#"
		RETURN vector::distance::euclidean([1, 2, 3], [1, 2, 3]);
		RETURN vector::distance::euclidean([NaN, 2, 3], [-1, NaN, -3]);
		RETURN vector::distance::euclidean([1, 2, 3], [-1, -2, -3]);
		RETURN vector::distance::euclidean([10, 50, 200], [400, 100, 20]);
		RETURN vector::distance::euclidean([10, 20, 15, 10, 5], [12, 24, 18, 8, 7]);
	"#,
		&["0f", "NaN", "7.483314773547883", "432.43496620879307", "6.082762530298219"],
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
async fn function_vector_distance_manhattan() -> Result<()> {
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
async fn function_vector_distance_hamming() -> Result<()> {
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
async fn function_vector_distance_minkowski() -> Result<()> {
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
async fn function_vector_distance_chebyshev() -> Result<()> {
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
pub async fn function_http_head() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_get() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_put() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_post() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_patch() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_delete() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_error() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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

	Test::new(&query).await?.expect_error(
		"There was an error processing a remote HTTP request: 500 Internal Server Error",
	)?;

	server.verify().await;

	Ok(())
}

#[cfg(all(feature = "http", feature = "scripting"))]
#[tokio::test]
pub async fn function_http_get_from_script() -> Result<()> {
	use wiremock::matchers::{header, method, path};
	use wiremock::{Mock, ResponseTemplate};

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
pub async fn function_http_disabled() -> Result<()> {
	Test::new(
		r#"
	RETURN http::get({});
	RETURN http::head({});
	RETURN http::put({});
	RETURN http::post({});
	RETURN http::patch({});
	RETURN http::delete({});
	"#,
	)
	.await?
	.expect_errors(&[
		"Remote HTTP request functions are not enabled",
		"Remote HTTP request functions are not enabled",
		"Remote HTTP request functions are not enabled",
		"Remote HTTP request functions are not enabled",
		"Remote HTTP request functions are not enabled",
		"Remote HTTP request functions are not enabled",
	])?;
	Ok(())
}

// Tests for custom defined functions

#[tokio::test]
async fn function_outside_database() -> Result<()> {
	let sql = "RETURN fn::does_not_exist();";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;

	match res.remove(0).result.unwrap_err().downcast() {
		Ok(Error::DbEmpty) => (),
		_ => panic!("Query should have failed with error: Specify a database to use"),
	}

	Ok(())
}

#[tokio::test]
async fn function_idiom_chaining() -> Result<()> {
	let sql = r#"
		{ a: 1, b: 2 }.entries().flatten();
		"ABC".lowercase();
		true.is_number();
		true.is_bool();
		true.doesnt_exist();
		field.bla.nested.is_none();
		// String is one of the types in the initial match statement,
		// this test ensures that the dispatch macro does not exit early
		"string".is_bool();
		["1", "2"].join('').chain(|$v| <int> $v);
	"#;
	Test::new(sql)
		.await?
		.expect_val("['a', 1, 'b', 2]")?
		.expect_val("'abc'")?
		.expect_val("false")?
		.expect_val("true")?
        .expect_error("There was a problem running the doesnt_exist() function. no such method found for the bool type")?
	    .expect_val("true")?
		.expect_val("false")?
        .expect_val("12")?;
	Ok(())
}

// tests for custom functions with return types
#[tokio::test]
async fn function_custom_typed_returns() -> Result<()> {
	let sql = r#"
		DEFINE FUNCTION fn::two() -> int {2};
		DEFINE FUNCTION fn::two_bad_type() -> string {2};
		RETURN fn::two();
		RETURN fn::two_bad_type();
	"#;
	let error = "Couldn't coerce return value from function `two_bad_type`: Expected `string` but found `2`";
	Test::new(sql)
		.await?
		.expect_val("None")?
		.expect_val("None")?
		.expect_val("2")?
		.expect_error(error)?;
	Ok(())
}
