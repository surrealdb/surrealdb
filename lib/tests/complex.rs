#![cfg(not(target_arch = "wasm32"))]

mod parse;
use parse::Parse;
use std::future::Future;
use std::thread::Builder;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[test]
fn self_referential_field() -> Result<(), Error> {
	// Ensure a good stack size for tests
	with_enough_stack(async {
		let mut res = run_queries(
			"
			CREATE pet:dog SET tail = <future> { tail };
			",
		)
		.await?;
		//
		assert_eq!(res.len(), 1);
		//
		let tmp = res.next().unwrap();
		assert!(matches!(tmp, Err(Error::ComputationDepthExceeded)));
		//
		Ok(())
	})
}

#[test]
fn cyclic_fields() -> Result<(), Error> {
	// Ensure a good stack size for tests
	with_enough_stack(async {
		let mut res = run_queries(
			"
			CREATE recycle SET consume = <future> { produce }, produce = <future> { consume };
			",
		)
		.await?;
		//
		assert_eq!(res.len(), 1);
		//
		let tmp = res.next().unwrap();
		assert!(matches!(tmp, Err(Error::ComputationDepthExceeded)));
		//
		Ok(())
	})
}

#[test]
fn cyclic_records() -> Result<(), Error> {
	// Ensure a good stack size for tests
	with_enough_stack(async {
		let mut res = run_queries(
			"
			CREATE thing:one SET friend = <future> { thing:two.friend };
			CREATE thing:two SET friend = <future> { thing:one.friend };
			",
		)
		.await?;
		//
		assert_eq!(res.len(), 2);
		//
		let tmp = res.next().unwrap();
		assert!(tmp.is_ok());
		//
		let tmp = res.next().unwrap();
		assert!(matches!(tmp, Err(Error::ComputationDepthExceeded)));
		//
		Ok(())
	})
}

#[test]
fn ok_future_graph_subquery_recursion_depth() -> Result<(), Error> {
	// Ensure a good stack size for tests
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
		//
		assert_eq!(res.len(), 8);
		//
		for i in 0..7 {
			let tmp = res.next().unwrap();
			assert!(tmp.is_ok(), "Statement {} resulted in {:?}", i, tmp);
		}
		//
		let tmp = res.next().unwrap()?;
		let val = Value::parse("[ [42] ]");
		assert_eq!(tmp, val);
		//
		Ok(())
	})
}

#[test]
fn ok_graph_traversal_depth() -> Result<(), Error> {
	// Build the SQL traversal query
	fn graph_traversal(n: usize) -> String {
		let mut ret = String::from("CREATE node:0;\n");
		for i in 1..=n {
			let prev = i - 1;
			ret.push_str(&format!("CREATE node:{i};\n"));
			ret.push_str(&format!("RELATE node:{prev}->edge{i}->node:{i};\n"));
		}
		ret.push_str("SELECT ");
		for i in 1..=n {
			ret.push_str(&format!("->edge{i}->node"));
		}
		ret.push_str(" AS res FROM node:0;\n");
		ret
	}
	// Test different traveral depths
	for n in 1..=40 {
		// Ensure a good stack size for tests
		with_enough_stack(async move {
			// Run the graph traversal queries
			let mut res = run_queries(&graph_traversal(n)).await?;
			// Remove the last result
			let tmp = res.next_back().unwrap();
			// Check all other queries
			assert!(res.all(|r| r.is_ok()));
			//
			match tmp {
				Ok(res) => {
					let val = Value::parse(&format!(
						"[
							{{
								res: [node:{n}],
							}}
						]"
					));
					assert_eq!(res, val);
				}
				Err(res) => {
					assert!(matches!(res, Error::ComputationDepthExceeded));
					assert!(n > 10, "Max traversals: {}", n - 1);
				}
			}

			Ok(())
		})
		.unwrap();
	}

	Ok(())
}

#[test]
fn ok_cast_chain_depth() -> Result<(), Error> {
	// Ensure a good stack size for tests
	with_enough_stack(async {
		// Run a chasting query which succeeds
		let mut res = run_queries(&cast_chain(10)).await?;
		//
		assert_eq!(res.len(), 1);
		//
		let tmp = res.next().unwrap()?;
		let val = Value::from(vec![Value::from(5)]);
		assert_eq!(tmp, val);
		//
		Ok(())
	})
}

#[test]
fn excessive_cast_chain_depth() -> Result<(), Error> {
	// Ensure a good stack size for tests
	with_enough_stack(async {
		// Run a casting query which will fail
		let mut res = run_queries(&cast_chain(35)).await?;
		//
		assert_eq!(res.len(), 1);
		//
		let tmp = res.next().unwrap();
		assert!(matches!(tmp, Err(Error::ComputationDepthExceeded)));
		//
		Ok(())
	})
}

async fn run_queries(
	sql: &str,
) -> Result<
	impl Iterator<Item = Result<Value, Error>> + ExactSizeIterator + DoubleEndedIterator + 'static,
	Error,
> {
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
		builder = builder.stack_size(16_000_000);
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
