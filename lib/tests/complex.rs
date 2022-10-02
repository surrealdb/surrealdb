mod parse;

use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

async fn run_queries(
	sql: &str,
) -> Result<impl Iterator<Item = Result<Value, Error>> + ExactSizeIterator + 'static, Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	dbs.execute(&sql, &ses, None, false).await.map(|v| v.into_iter().map(|res| res.result))
}

#[tokio::test]
async fn self_referential_field() -> Result<(), Error> {
	let mut res = run_queries(
		"
		CREATE pet:dog SET tail = <future> { tail };
	",
	)
	.await?;

	assert_eq!(res.len(), 1);

	let result = res.next().unwrap();
	assert!(matches!(result, Err(Error::ComputationDepthExceeded)), "got: {:?}", result);

	Ok(())
}

#[tokio::test]
async fn cyclic_fields() -> Result<(), Error> {
	let mut res = run_queries(
		"
		CREATE recycle SET consume = <future> { produce }, produce = <future> { consume };
	",
	)
	.await?;

	assert_eq!(res.len(), 1);

	let result = res.next().unwrap();
	assert!(matches!(result, Err(Error::ComputationDepthExceeded)), "got: {:?}", result);

	Ok(())
}

#[tokio::test]
#[cfg(not(debug_assertions))]
async fn cyclic_records() -> Result<(), Error> {
	let mut res = run_queries(
		"
		CREATE thing:one SET friend = <future> { thing:two.friend };
		CREATE thing:two SET friend = <future> { thing:one.friend };
	",
	)
	.await?;

	assert_eq!(res.len(), 2);

	let tmp = res.next().unwrap();
	assert!(tmp.is_ok());

	let result = res.next().unwrap();
	assert!(matches!(result, Err(Error::ComputationDepthExceeded)));

	Ok(())
}

#[tokio::test]
async fn ok_cast_chain() -> Result<(), Error> {
	let mut res = run_queries(
		"
		SELECT * FROM <int><int><int><int><int><int><int><int><int><int><int><int><int>5;
	",
	)
	.await?;

	assert_eq!(res.len(), 1);

	let result = res.next().unwrap();
	assert_eq!(result.unwrap(), Value::from(vec![Value::from(5)]));

	Ok(())
}

#[tokio::test]
#[cfg(not(debug_assertions))]
async fn excessive_cast_chain() -> Result<(), Error> {
	let mut res = run_queries("
		SELECT * FROM <int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int><int>5;
	").await?;

	assert_eq!(res.len(), 1);

	let result = res.next().unwrap();
	assert!(matches!(result, Err(Error::ComputationDepthExceeded)), "got: {:?}", result);

	Ok(())
}
