#![cfg(feature = "scripting")]

mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use rust_decimal::Decimal;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Number;
use surrealdb::sql::Value;

#[tokio::test]
async fn script_function_error() -> Result<(), Error> {
	let sql = "
		SELECT * FROM function() {
			throw 'error';
		};
		SELECT * FROM function() {
			throw new Error('error');
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Problem with embedded script function. An exception occurred: error"
	));
	//
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == "Problem with embedded script function. An exception occurred: error"
	));
	//
	Ok(())
}

#[tokio::test]
async fn script_function_simple() -> Result<(), Error> {
	let sql = r#"
		CREATE person:test SET scores = function() {
			return [6.6, 8.4, 7.3].map(v => v * 10);
		}, bio = function() {
			return "Line 1\nLine 2";
		};
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(r#"[{ bio: "Line 1\nLine 2", id: person:test, scores: [66, 84, 73] }]"#);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_context() -> Result<(), Error> {
	let sql = "
		CREATE film:test SET
			ratings = [
				{ rating: 6.3 },
				{ rating: 8.7 },
			],
			display = function() {
				return this.ratings.filter(r => {
					return r.rating >= 7;
				}).map(r => {
					return { ...r, rating: Math.round(r.rating * 10) };
				});
			}
		;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: film:test,
				ratings: [
					{ rating: 6.3 },
					{ rating: 8.7 },
				],
				display: [
					{ rating: 87 },
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_arguments() -> Result<(), Error> {
	let sql = "
		LET $value = 'SurrealDB';
		LET $words = ['awesome', 'advanced', 'cool'];
		CREATE article:test SET summary = function($value, $words) {
			return `${arguments[0]} is ${arguments[1].join(', ')}`;
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
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
		"[
			{
				id: article:test,
				summary: 'SurrealDB is awesome, advanced, cool',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_types() -> Result<(), Error> {
	let sql = "
		CREATE article:test SET
			created_at = function() {
				return new Date('1995-12-17T03:24:00Z');
			},
			next_signin = function() {
				return new Duration('1w2d6h');
			},
			manager = function() {
				return new Record('user', 'joanna');
			},
			identifier = function() {
				return new Uuid('03412258-988f-47cd-82db-549902cdaffe');
			}
		;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: article:test,
				created_at: d'1995-12-17T03:24:00Z',
				next_signin: 1w2d6h,
				manager: user:joanna,
				identifier: u'03412258-988f-47cd-82db-549902cdaffe',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_module_os() -> Result<(), Error> {
	let sql = "
		CREATE platform:test SET version = function() {
			const { platform } = await import('os');
			return platform();
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	Ok(())
}

#[tokio::test]
async fn script_query_from_script_select() -> Result<(), Error> {
	let sql = r#"
		CREATE test SET name = "a", number = 0;
		CREATE test SET name = "b", number = 1;
		CREATE test SET name = "c", number = 2;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// direct query
	dbs.execute(sql, &ses, None).await?;
	let sql = r#"
		RETURN function(){
			return await surrealdb.query(`SELECT number FROM test WHERE name = $name`,{
				name: "b"
			})
		}
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				number: 1
			}
		]",
	);
	assert_eq!(tmp, val);

	// indirect query
	let sql = r#"
		RETURN function(){
			let query = new surrealdb.Query(`SELECT number FROM test WHERE name = $name`);
			query.bind("name","c")
			return await surrealdb.query(query);
		}
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				number: 2
			}
		]",
	);
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn script_query_from_script() -> Result<(), Error> {
	let sql = r#"
		RETURN function() {
			return await surrealdb.query(`CREATE ONLY article:test SET name = "The daily news", issue_number = 3`)
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"{
				id: article:test,
				name: "The daily news",
				issue_number: 3.0
		}"#,
	);
	assert_eq!(tmp, val);

	let sql = r#"
		SELECT * FROM article
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[{
				id: article:test,
				name: "The daily news",
				issue_number: 3.0
		}]"#,
	);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn script_value_function_params() -> Result<(), Error> {
	let sql = r#"
		LET $test = CREATE ONLY article:test SET name = "The daily news", issue_number = 3;
		RETURN function() {
			return await surrealdb.value(`$test.name`)
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	let tmp = res.remove(1).result?;
	let val = Value::parse(r#""The daily news""#);
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn script_value_function_inline_values() -> Result<(), Error> {
	let sql = r#"
		RETURN function() {
			if(await surrealdb.value(`3`) !== 3){
				throw new Error(1)
			}
			if(await surrealdb.value(`"some string"`) !== "some string"){
				throw new Error(2)
			}
			if(await surrealdb.value(`<future>{ math::floor(13.746189) }`) !== 13){
				throw new Error(3)
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	res.remove(0).result?;
	Ok(())
}

#[tokio::test]
async fn script_function_number_conversion_test() -> Result<(), Error> {
	let sql = r#"
		RETURN function() {
			if(await surrealdb.value(`2147483647`) !== 2147483647){
				throw new Error(1)
			}
			if(await surrealdb.value(`9007199254740991`) !== 9007199254740991){
				throw new Error(2)
			}
			if(await surrealdb.value(`9007199254740992`) !== 9007199254740992n){
				throw new Error(3)
			}
			if(await surrealdb.value(`-9007199254740992`) !== -9007199254740992){
				throw new Error(4)
			}
			if(await surrealdb.value(`-9007199254740993`) !== -9007199254740993n){
				throw new Error(5)
			}
			return {
				a:  9007199254740991,
				b: -9007199254740992,
				c: 100000000000000000n,
				d: 9223372036854775808n
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Object(res) = res.remove(0).result? else {
		panic!("not an object")
	};
	assert_eq!(res.get("a").unwrap(), &Value::Number(Number::Float(9007199254740991f64)));
	assert_eq!(res.get("b").unwrap(), &Value::Number(Number::Float(-9007199254740992f64)));
	assert_eq!(res.get("c").unwrap(), &Value::Number(Number::Int(100000000000000000i64)));
	assert_eq!(
		res.get("d").unwrap(),
		&Value::Number(Number::Decimal(Decimal::from(9223372036854775808u128)))
	);

	Ok(())
}
