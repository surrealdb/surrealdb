use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::field::{Field, Fields};
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::statements::select::SelectStatement;
use crate::sql::value::{Value, Values};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::sync::Arc;

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
				Value::Object(v) => match p {
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
						try_join_all(futs).await.map(|v| v.into())
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
						try_join_all(futs).await.map(|v| v.into())
					}
				},
				// Current path part is a thing
				Value::Thing(v) => match path.len() {
					// No remote embedded fields, so just return this
					0 => Ok(Value::Thing(v.clone())),
					// Remote embedded field, so fetch the thing
					_ => {
						let stm = SelectStatement {
							expr: Fields(vec![Field::All]),
							what: Values(vec![Value::Thing(v.clone())]),
							..SelectStatement::default()
						};
						Arc::new(stm)
							.compute(ctx, opt, txn, None)
							.await?
							.first()
							.get(ctx, opt, txn, path)
							.await
					}
				},
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
