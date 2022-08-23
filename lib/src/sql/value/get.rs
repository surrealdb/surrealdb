use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::paths::ID;
use crate::sql::statements::select::SelectStatement;
use crate::sql::value::{Value, Values};
use async_recursion::async_recursion;
use futures::future::try_join_all;
use std::borrow::Cow;

impl Value {
	#[cfg_attr(feature = "parallel", async_recursion)]
	#[cfg_attr(not(feature = "parallel"), async_recursion(?Send))]
	pub async fn get<'a>(
		&'a self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
	) -> Result<Cow<'a, Self>, Error> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Graph(_) => match v.rid() {
						Some(v) => {
							let x = Value::Thing(v);
							let x = x.get(ctx, opt, txn, path).await;
							// Without this manual cloning, it ends up with "cannot return value referencing temporary value"
							x.map(|x| Cow::from(x.into_owned()))
						}
						None => Ok(Cow::from(&Value::None)),
					},
					Part::Field(f) => match v.get(f as &str) {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Cow::from(&Value::None)),
					},
					Part::All => self.get(ctx, opt, txn, path.next()).await,
					Part::Any => self.get(ctx, opt, txn, path.next()).await,
					_ => Ok(Cow::from(&Value::None)),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, path));
						let x: Result<Vec<Value>, Error> = try_join_all(futs).await.map(|x| {
							let z = x.into_iter();
							let z = z.map(|y| y.into_owned());
							z.collect()
						});
						let x: Result<Value, Error> = x.map(Into::into);
						x.map(Cow::from)
					}
					Part::Any => {
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, path));
						let x: Result<Vec<Value>, Error> = try_join_all(futs).await.map(|x| {
							let z = x.into_iter().map(|y| y.into_owned());
							z.collect()
						});
						let x: Result<Value, Error> = x.map(Into::into);
						x.map(Cow::from)
					}
					Part::First => match v.first() {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Cow::from(&Value::None)),
					},
					Part::Last => match v.last() {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Cow::from(&Value::None)),
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v.get(ctx, opt, txn, path.next()).await,
						None => Ok(Cow::from(&Value::None)),
					},
					Part::Where(w) => {
						let path = path.next();
						let mut a = Vec::new();
						for v in v.iter() {
							if w.compute(ctx, opt, txn, Some(v)).await?.is_truthy() {
								a.push(v.get(ctx, opt, txn, path).await?.into_owned())
							}
						}
						Ok(Cow::Owned(a.into()))
					}
					_ => {
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, path));
						let x = try_join_all(futs).await;
						let x: Result<Vec<Value>, Error> = x.map(|x| {
							let x = x.into_iter().map(|y| y.into_owned()).collect();
							x
						});
						let x: Result<Value, Error> = x.map(Into::into);
						x.map(Cow::from)
					}
				},
				// Current path part is a thing
				Value::Thing(v) => {
					// Clone the thing
					let val = v.clone();
					// Check how many path parts are remaining
					match path.len() {
						// No remote embedded fields, so just return this
						0 => Ok(Cow::from(Value::Thing(val))),
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
									1 => {
										let x = stm.compute(ctx, opt, txn, None).await?.all();
										let x = x.get(ctx, opt, txn, ID.as_ref()).await?;
										let x = x
											// without .into_owned() it end up with
											//  move occurs because value has type `sql::value::value::Value`, which does not implement the `Copy` trait
											.into_owned()
											.flatten()
											.ok()
											.map(Cow::from);
										x
									}
									_ => {
										let x = stm.compute(ctx, opt, txn, None).await?.all();
										let x = x.get(ctx, opt, txn, path.next()).await?;
										let x = x.into_owned().flatten().ok().map(Cow::from);
										// We can't use x.clone() as Error doesn't derive Clone(and it can't due to various underlying serde-related errors don't derive Clone)
										x.map(|x| Cow::from(x.into_owned()))
									}
								}
							}
							// This is a remote field expression
							_ => {
								let stm = SelectStatement {
									expr: Fields(vec![Field::All]),
									what: Values(vec![Value::from(val)]),
									..SelectStatement::default()
								};
								let x = stm.compute(ctx, opt, txn, None).await?;
								let x = x.first();
								let x = x.get(ctx, opt, txn, path).await;
								let x = match x {
									Ok(x) => Ok(Cow::from(x.into_owned())),
									Err(x) => Err(x),
								};
								x
							}
						},
					}
				}
				// Ignore everything else
				_ => match p {
					Part::Any => match path.len() {
						1 => Ok(Cow::from(self)),
						_ => Ok(Cow::from(&Value::None)),
					},
					_ => Ok(Cow::from(&Value::None)),
				},
			},
			// No more parts so get the value
			None => Ok(Cow::from(self)),
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
		assert_eq!(res.into_owned(), val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res.into_owned(), Value::from(123));
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res.into_owned(),
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
		assert_eq!(res.into_owned(), Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res.into_owned(),
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
		assert_eq!(res.into_owned(), Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res.into_owned(), Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res.into_owned(), Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res.into_owned(), Value::from(vec![36]));
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(
			res.into_owned(),
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}
}
