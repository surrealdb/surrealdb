mod parse;

use std::future::Future;
use std::thread::Builder;
use surrealdb::sql::Value;
use surrealdb::Datastore;
use surrealdb::Error;
use surrealdb::Session;

#[test]
fn self_referential_field() -> Result<(), Error> {
	with_enough_stack(async {
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
	})
}

#[test]
fn cyclic_fields() -> Result<(), Error> {
	with_enough_stack(async {
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
	})
}

#[test]
fn cyclic_records() -> Result<(), Error> {
	with_enough_stack(async {
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
	})
}

#[test]
fn ok_future_graph_subquery_recursion_depth() -> Result<(), Error> {
	with_enough_stack(async {
		let mut res = run_queries(
			r#"
			CREATE thing:three SET fut = <future> { friends[0].fut }, friends = [thing:four, thing:two];
			CREATE thing:four SET fut = <future> { (friend) }, friend = <future> { 42 };
			CREATE thing:two SET fut = <future> { friend }, friend = <future> { thing:three.fut };

			CREATE thing:one SET foo = "bar";
			RELATE thing:one->friend->thing:two SET timestamp = time::now();

			CREATE thing:zero SET foo = "baz";
			RELATE thing:zero->enemy->thing:one SET timestamp = time::now();

			SELECT * FROM (SELECT * FROM (SELECT ->enemy->thing->friend->thing.fut as fut FROM thing:zero));
		"#,
		)
		.await?;

		assert_eq!(res.len(), 8);

		for i in 0..7 {
			let tmp = res.next().unwrap();
			assert!(tmp.is_ok(), "{} resulted in {:?}", i, tmp);
		}

		let result = res.next().unwrap();
		assert_eq!(result.unwrap(), Value::from(vec![Value::from(vec![Value::from(42)])]));

		Ok(())
	})
}

#[test]
fn ok_cast_chain_depth() -> Result<(), Error> {
	with_enough_stack(async {
		let mut res = run_queries(&cast_chain(10)).await?;

		assert_eq!(res.len(), 1);

		let result = res.next().unwrap();
		assert_eq!(result.unwrap(), Value::from(vec![Value::from(5)]));

		Ok(())
	})
}

#[test]
fn excessive_cast_chain_depth() -> Result<(), Error> {
	with_enough_stack(async {
		let mut res = run_queries(&cast_chain(35)).await?;

		assert_eq!(res.len(), 1);

		let result = res.next().unwrap();
		assert!(matches!(result, Err(Error::ComputationDepthExceeded)), "got: {:?}", result);

		Ok(())
	})
}

async fn run_queries(
	sql: &str,
) -> Result<impl Iterator<Item = Result<Value, Error>> + ExactSizeIterator + 'static, Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	dbs.execute(&sql, &ses, None, false).await.map(|v| v.into_iter().map(|res| res.result))
}

fn with_enough_stack(
	fut: impl Future<Output = Result<(), Error>> + Send + 'static,
) -> Result<(), Error> {
	#[allow(unused_mut)]
	let mut builder = Builder::new();

	#[cfg(debug_assertions)]
	{
		builder = builder.stack_size(8_000_000);
	}

	builder
		.spawn(|| {
			let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
			runtime.block_on(fut)
		})
		.unwrap()
		.join()
		.unwrap()
}

fn cast_chain(n: usize) -> String {
	let mut sql = String::from("SELECT * FROM ");
	for _ in 0..n {
		sql.push_str("<int>");
	}
	sql.push_str("5;");
	sql
}
