use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::id::Id;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::statements::select::SelectStatement;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use async_recursion::async_recursion;
use futures::future::try_join_all;

impl Value {
	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub async fn get(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
	) -> Result<Self, Error> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Future(v) => {
					// Check how many path parts are remaining
					match path.len() {
						// No further embedded fields, so just return this
						0 => Ok(Value::Future(v.clone())),
						//
						_ => v.compute(ctx, opt, txn, None).await?.get(ctx, opt, txn, path).await,
					}
				}
				// Current path part is an object
				Value::Object(v) => match p {
					// If requesting an `id` field, check if it is a complex Record ID
					Part::Field(f) if f.is_id() && path.len() > 1 => match v.get(f as &str) {
						Some(Value::Thing(Thing {
							id: Id::Object(v),
							..
						})) => Value::Object(v.clone()).get(ctx, opt, txn, path.next()).await,
						Some(Value::Thing(Thing {
							id: Id::Array(v),
							..
						})) => Value::Array(v.clone()).get(ctx, opt, txn, path.next()).await,
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Graph(_) => match v.rid() {
						Some(v) => Value::Thing(v).get(ctx, opt, txn, path).await,
						None => Ok(Value::None),
					},
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Value::None),
					},
					Part::All => self.get(ctx, opt, txn, path.next()).await,
					_ => Ok(Value::None),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, path));
						try_join_all(futs).await.map(Into::into)
					}
					Part::First => match v.first() {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Last => match v.last() {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Where(w) => {
						let path = path.next();
						let mut a = Vec::new();
						for v in v.iter() {
							if w.compute(ctx, opt, txn, Some(v)).await?.is_truthy() {
								a.push(v.get(ctx, opt, txn, path).await?)
							}
						}
						Ok(a.into())
					}
					_ => {
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, path));
						try_join_all(futs).await.map(Into::into)
					}
				},
				// Current path part is a thing
				Value::Thing(v) => {
					// Clone the thing
					let val = v.clone();
					// Check how many path parts are remaining
					match path.len() {
						// No remote embedded fields, so just return this
						0 => Ok(Value::Thing(val)),
						// Remote embedded field, so fetch the thing
						_ => match p {
							// This is a graph traversal expression
							Part::Graph(g) => {
								let stm = SelectStatement {
									expr: Fields(vec![Field::All]),
									what: Values(vec![Value::from(Edges {
										from: val,
										dir: g.dir.clone(),
										what: g.what.clone(),
									})]),
									cond: g.cond.clone(),
									..SelectStatement::default()
								};
								match path.len() {
									1 => stm
										.compute(ctx, opt, txn, None)
										.await?
										.all()
										.get(ctx, opt, txn, ID.as_ref())
										.await?
										.flatten()
										.ok(),
									_ => stm
										.compute(ctx, opt, txn, None)
										.await?
										.all()
										.get(ctx, opt, txn, path.next())
										.await?
										.flatten()
										.ok(),
								}
							}
							// This is a remote field expression
							_ => {
								let stm = SelectStatement {
									expr: Fields(vec![Field::All]),
									what: Values(vec![Value::from(val)]),
									..SelectStatement::default()
								};
								stm.compute(ctx, opt, txn, None)
									.await?
									.first()
									.get(ctx, opt, txn, path)
									.await
							}
						},
					}
				}
				// Ignore everything else
				_ => Ok(Value::None),
			},
			// No more parts so get the value
			None => Ok(self.clone()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::id::Id;
	use crate::sql::idiom::Idiom;
	use crate::sql::test::Parse;
	use crate::sql::thing::Thing;

	#[tokio::test]
	async fn get_none() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: Id::from("tobie")
			})
		);
	}

	#[tokio::test]
	async fn get_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(Thing {
				tb: String::from("test"),
				id: Id::from("jaime")
			})
		);
	}

	#[tokio::test]
	async fn get_array_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![36]));
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}
}
