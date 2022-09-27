#![cfg(feature = "scripting")]

mod parse;
use parse::Parse;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[tokio::test]
async fn script_function_simple() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET scores = function() {
			return [6.6, 8.4, 7.3].map(v => v * 10);
		};
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[{ id: person:test, scores: [66, 84, 73] }]");
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: article:test,
				created_at: '1995-12-17T03:24:00Z',
				next_signin: 1w2d6h,
				manager: user:joanna,
				identifier: '03412258-988f-47cd-82db-549902cdaffe',
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
			const { release } = await import('os');
			return release();
		};
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	Ok(())
}
