use crate::cnf::MAX_COMPUTATION_DEPTH;
use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
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

impl Value {
	/// Asynchronous method for getting a local or remote field from a `Value`
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn get(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&'async_recursion CursorDoc<'_>>,
		path: &[Part],
	) -> Result<Self, Error> {
		// Limit recursion depth.
		if path.len() > (*MAX_COMPUTATION_DEPTH).into() {
			return Err(Error::ComputationDepthExceeded);
		}
		match path.first() {
			// Get the current value at the path
			Some(p) => match self {
				// Current value at path is a geometry
				Value::Geometry(v) => match p {
					// If this is the 'type' field then continue
					Part::Field(f) if f.is_type() => {
						Value::from(v.as_type()).get(ctx, opt, txn, doc, path.next()).await
					}
					// If this is the 'coordinates' field then continue
					Part::Field(f) if f.is_coordinates() && v.is_geometry() => {
						v.as_coordinates().get(ctx, opt, txn, doc, path.next()).await
					}
					// If this is the 'geometries' field then continue
					Part::Field(f) if f.is_geometries() && v.is_collection() => {
						v.as_coordinates().get(ctx, opt, txn, doc, path.next()).await
					}
					// Otherwise return none
					_ => Ok(Value::None),
				},
				// Current value at path is a future
				Value::Future(v) => {
					// Check how many path parts are remaining
					match path.len() {
						// No further embedded fields, so just return this
						0 => Ok(Value::Future(v.clone())),
						// Process the future and fetch the embedded field
						_ => {
							// Ensure the future is processed
							let fut = &opt.new_with_futures(true);
							// Get the future return value
							let val = v.compute(ctx, fut, txn, doc).await?;
							// Fetch the embedded field
							val.get(ctx, opt, txn, doc, path).await
						}
					}
				}
				// Current value at path is an object
				Value::Object(v) => match p {
					// If requesting an `id` field, check if it is a complex Record ID
					Part::Field(f) if f.is_id() && path.len() > 1 => match v.get(f.as_str()) {
						Some(Value::Thing(Thing {
							id: Id::Object(v),
							..
						})) => Value::Object(v.clone()).get(ctx, opt, txn, doc, path.next()).await,
						Some(Value::Thing(Thing {
							id: Id::Array(v),
							..
						})) => Value::Array(v.clone()).get(ctx, opt, txn, doc, path.next()).await,
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Graph(_) => match v.rid() {
						Some(v) => Value::Thing(v).get(ctx, opt, txn, doc, path).await,
						None => Ok(Value::None),
					},
					Part::Field(f) => match v.get(f.as_str()) {
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.get(&i.to_string()) {
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Value(x) => match x.compute(ctx, opt, txn, doc).await? {
						Value::Strand(f) => match v.get(f.as_str()) {
							Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
							None => Ok(Value::None),
						},
						_ => Ok(Value::None),
					},
					Part::All => self.get(ctx, opt, txn, doc, path.next()).await,
					_ => Ok(Value::None),
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					// Current path is an `*` part
					Part::All | Part::Flatten => {
						let path = path.next();
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, doc, path));
						try_join_all_buffered(futs).await.map(Into::into)
					}
					Part::First => match v.first() {
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Last => match v.last() {
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
						None => Ok(Value::None),
					},
					Part::Where(w) => {
						let mut a = Vec::new();
						for v in v.iter() {
							let cur = v.into();
							if w.compute(ctx, opt, txn, Some(&cur)).await?.is_truthy() {
								a.push(v.clone());
							}
						}
						Value::from(a).get(ctx, opt, txn, doc, path.next()).await
					}
					Part::Value(x) => match x.compute(ctx, opt, txn, doc).await? {
						Value::Number(i) => match v.get(i.to_usize()) {
							Some(v) => v.get(ctx, opt, txn, doc, path.next()).await,
							None => Ok(Value::None),
						},
						_ => Ok(Value::None),
					},
					_ => {
						let futs = v.iter().map(|v| v.get(ctx, opt, txn, doc, path));
						try_join_all_buffered(futs).await.map(Into::into)
					}
				},
				// Current value at path is an edges
				Value::Edges(v) => {
					// Clone the thing
					let val = v.clone();
					// Check how many path parts are remaining
					match path.len() {
						// No remote embedded fields, so just return this
						0 => Ok(Value::Edges(val)),
						// Remote embedded field, so fetch the thing
						_ => {
							let stm = SelectStatement {
								expr: Fields(vec![Field::All], false),
								what: Values(vec![Value::from(val)]),
								..SelectStatement::default()
							};
							stm.compute(ctx, opt, txn, None)
								.await?
								.first()
								.get(ctx, opt, txn, None, path)
								.await
						}
					}
				}
				// Current value at path is a thing
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
									expr: Fields(vec![Field::All], false),
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
										.get(ctx, opt, txn, None, ID.as_ref())
										.await?
										.flatten()
										.ok(),
									_ => stm
										.compute(ctx, opt, txn, None)
										.await?
										.all()
										.get(ctx, opt, txn, None, path.next())
										.await?
										.flatten()
										.ok(),
								}
							}
							// This is a remote field expression
							_ => {
								let stm = SelectStatement {
									expr: Fields(vec![Field::All], false),
									what: Values(vec![Value::from(val)]),
									..SelectStatement::default()
								};
								stm.compute(ctx, opt, txn, None)
									.await?
									.first()
									.get(ctx, opt, txn, None, path)
									.await
							}
						},
					}
				}
				v => {
					if matches!(p, Part::Flatten) {
						v.get(ctx, opt, txn, None, path.next()).await
					} else {
						// Ignore everything else
						Ok(Value::None)
					}
				}
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
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn get_none() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_basic_deep_ok() {
		let (ctx, opt, txn) = mock().await;
		let depth = 20;
		let idi = Idiom::parse(&format!("{}something", "test.".repeat(depth)));
		let val = Value::parse(&format!(
			"{} {{ other: null, something: 123 {} }}",
			"{ test: ".repeat(depth),
			"}".repeat(depth)
		));
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_basic_deep_ko() {
		let (ctx, opt, txn) = mock().await;
		let depth = 2000;
		let idi = Idiom::parse(&format!("{}something", "test.".repeat(depth)));
		let val = Value::parse("{}"); // A deep enough object cannot be parsed.
		let err = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap_err();
		assert!(
			matches!(err, Error::ComputationDepthExceeded),
			"expected computation depth exceeded, got {:?}",
			err
		);
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
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
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
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
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(res, Value::from(vec![36]));
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}

	#[tokio::test]
	async fn get_array_where_fields_array_index() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(map! {
				"age".to_string() => Value::from(34),
			})
		);
	}

	#[tokio::test]
	async fn get_future_embedded_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: <future> { { something: [{ age: 34 }, { age: 36 }] } } }");
		let res = val.get(&ctx, &opt, &txn, None, &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}

	#[tokio::test]
	async fn get_future_embedded_field_with_reference() {
		let (ctx, opt, txn) = mock().await;
		let doc = Value::parse("{ name: 'Tobie', something: [{ age: 34 }, { age: 36 }] }");
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: <future> { { something: something } } }");
		let cur = (&doc).into();
		let res = val.get(&ctx, &opt, &txn, Some(&cur), &idi).await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}
}
