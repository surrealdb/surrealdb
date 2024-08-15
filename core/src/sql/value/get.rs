use std::collections::BTreeMap;

use crate::cnf::MAX_COMPUTATION_DEPTH;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::fnc::idiom;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::id::Id;
use crate::sql::part::Part;
use crate::sql::part::{Next, NextMethod};
use crate::sql::paths::ID;
use crate::sql::statements::select::SelectStatement;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use crate::sql::Function;
use reblessive::tree::Stk;

impl Value {
	/// Asynchronous method for getting a local or remote field from a `Value`
	///
	/// Was marked recursive
	pub(crate) async fn get(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		path: &[Part],
	) -> Result<Self, Error> {
		// Limit recursion depth.
		if path.len() > (*MAX_COMPUTATION_DEPTH).try_into().unwrap_or(usize::MAX) {
			return Err(Error::ComputationDepthExceeded);
		}
		match path.first() {
			// Get the current value at the path
			Some(p) => match self {
				// Current value at path is a geometry
				Value::Geometry(v) => match p {
					// If this is the 'type' field then continue
					Part::Field(f) if f.is_type() => {
						let v = Value::from(v.as_type());
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					// If this is the 'coordinates' field then continue
					Part::Field(f) if f.is_coordinates() && v.is_geometry() => {
						let v = v.as_coordinates();
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					// If this is the 'geometries' field then continue
					Part::Field(f) if f.is_geometries() && v.is_collection() => {
						let v = v.as_coordinates();
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Destructure(_) => {
						let obj = Value::Object(v.as_object());
						stk.run(|stk| obj.get(stk, ctx, opt, doc, path)).await
					}
					Part::Method(name, args) => {
						let v = stk
							.run(|stk| {
								idiom(stk, ctx, opt, doc, v.clone().into(), name, args.clone())
							})
							.await?;
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
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
							let val = v.compute(stk, ctx, fut, doc).await?;
							// Fetch the embedded field
							stk.run(|stk| val.get(stk, ctx, opt, doc, path)).await
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
						})) => {
							let v = Value::Object(v.clone());
							stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
						}
						Some(Value::Thing(Thing {
							id: Id::Array(v),
							..
						})) => {
							let v = Value::Array(v.clone());
							stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
						}
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Graph(_) => match v.rid() {
						Some(v) => {
							let v = Value::Thing(v);
							stk.run(|stk| v.get(stk, ctx, opt, doc, path)).await
						}
						None => Ok(Value::None),
					},
					Part::Field(f) => match v.get(f.as_str()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.get(&i.to_string()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Value(x) => match stk.run(|stk| x.compute(stk, ctx, opt, doc)).await? {
						Value::Strand(f) => match v.get(f.as_str()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						_ => Ok(Value::None),
					},
					Part::All => stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await,
					Part::Destructure(p) => {
						let mut obj = BTreeMap::<String, Value>::new();
						for p in p.iter() {
							let path = p.path();
							let v = stk
								.run(|stk| self.get(stk, ctx, opt, doc, path.as_slice()))
								.await?;
							obj.insert(p.field().to_raw(), v);
						}

						let obj = Value::from(obj);
						stk.run(|stk| obj.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Method(name, args) => {
						let res = stk
							.run(|stk| {
								idiom(stk, ctx, opt, doc, v.clone().into(), name, args.clone())
							})
							.await;
						let res = match &res {
							Ok(_) => res,
							Err(Error::InvalidFunction {
								..
							}) => match v.get(name) {
								Some(v) => {
									let fnc = Function::Anonymous(v.clone(), args.clone());
									match stk.run(|stk| fnc.compute(stk, ctx, opt, doc)).await {
										Ok(v) => Ok(v),
										Err(Error::InvalidFunction {
											..
										}) => res,
										e => e,
									}
								}
								None => res,
							},
							_ => res,
						}?;

						stk.run(|stk| res.get(stk, ctx, opt, doc, path.next())).await
					}
					_ => Ok(Value::None),
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					// Current path is an `*` part
					Part::All | Part::Flatten => {
						let path = path.next();
						stk.scope(|scope| {
							let futs =
								v.iter().map(|v| scope.run(|stk| v.get(stk, ctx, opt, doc, path)));
							try_join_all_buffered(futs)
						})
						.await
						.map(Into::into)
					}
					Part::First => match v.first() {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Last => match v.last() {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => Ok(Value::None),
					},
					Part::Where(w) => {
						let mut a = Vec::new();
						for v in v.iter() {
							let cur = v.clone().into();
							if stk
								.run(|stk| w.compute(stk, ctx, opt, Some(&cur)))
								.await?
								.is_truthy()
							{
								a.push(v.clone());
							}
						}
						let v = Value::from(a);
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Value(x) => match stk.run(|stk| x.compute(stk, ctx, opt, doc)).await? {
						Value::Number(i) => match v.get(i.to_usize()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						_ => Ok(Value::None),
					},
					Part::Method(name, args) => {
						let v = stk
							.run(|stk| {
								idiom(stk, ctx, opt, doc, v.clone().into(), name, args.clone())
							})
							.await?;
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					_ => stk
						.scope(|scope| {
							let futs =
								v.iter().map(|v| scope.run(|stk| v.get(stk, ctx, opt, doc, path)));
							try_join_all_buffered(futs)
						})
						.await
						.map(Into::into),
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
							let v = stk.run(|stk| stm.compute(stk, ctx, opt, None)).await?.first();
							stk.run(|stk| v.get(stk, ctx, opt, None, path)).await
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
									1 => {
										let v = stk
											.run(|stk| stm.compute(stk, ctx, opt, None))
											.await?
											.all();
										stk.run(|stk| v.get(stk, ctx, opt, None, ID.as_ref()))
											.await?
											.flatten()
											.ok()
									}
									_ => {
										let v = stk
											.run(|stk| stm.compute(stk, ctx, opt, None))
											.await?
											.all();
										let res = stk
											.run(|stk| v.get(stk, ctx, opt, None, path.next()))
											.await?;
										// We only want to flatten the results if the next part
										// is a graph part. Reason being that if we flatten fields,
										// the results of those fields (which could be arrays) will
										// be merged into each other. So [1, 2, 3], [4, 5, 6] would
										// become [1, 2, 3, 4, 5, 6]. This slice access won't panic
										// as we have already checked the length of the path.
										Ok(if let Part::Graph(_) = path[1] {
											res.flatten()
										} else {
											res
										})
									}
								}
							}
							Part::Method(name, args) => {
								let v = stk
									.run(|stk| {
										idiom(
											stk,
											ctx,
											opt,
											doc,
											v.clone().into(),
											name,
											args.clone(),
										)
									})
									.await?;
								stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
							}
							// This is a remote field expression
							_ => {
								let stm = SelectStatement {
									expr: Fields(vec![Field::All], false),
									what: Values(vec![Value::from(val)]),
									..SelectStatement::default()
								};
								let v =
									stk.run(|stk| stm.compute(stk, ctx, opt, None)).await?.first();
								stk.run(|stk| v.get(stk, ctx, opt, None, path)).await
							}
						},
					}
				}
				v => {
					match p {
						Part::Flatten => {
							stk.run(|stk| v.get(stk, ctx, opt, None, path.next())).await
						}
						Part::Method(name, args) => {
							let v = stk
								.run(|stk| idiom(stk, ctx, opt, doc, v.clone(), name, args.clone()))
								.await?;
							stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
						}
						// Only continue processing the path from the point that it contains a method
						_ => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next_method()))
								.await
						}
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
		let (ctx, opt) = mock().await;
		let idi = Idiom::default();
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something");
		let val = Value::parse("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_basic_deep_ok() {
		let (ctx, opt) = mock().await;
		let depth = 20;
		let idi = Idiom::parse(&format!("{}something", "test.".repeat(depth)));
		let val = Value::parse(&format!(
			"{} {{ other: null, something: 123 {} }}",
			"{ test: ".repeat(depth),
			"}".repeat(depth)
		));
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_basic_deep_ko() {
		let (ctx, opt) = mock().await;
		let depth = 2000;
		let idi = Idiom::parse(&format!("{}something", "test.".repeat(depth)));
		let val = Value::parse("{}"); // A deep enough object cannot be parsed.
		let mut stack = reblessive::tree::TreeStack::new();
		let err =
			stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap_err();
		assert!(
			matches!(err, Error::ComputationDepthExceeded),
			"expected computation depth exceeded, got {:?}",
			err
		);
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.other");
		let val = Value::parse("{ test: { other: test:tobie, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
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
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let val = Value::parse("{ test: { something: [test:tobie, test:jaime] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
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
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_fields_flat() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(vec![34, 36]));
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(vec![36]));
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}

	#[tokio::test]
	async fn get_array_where_fields_array_index() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0]");
		let val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(map! {
				"age".to_string() => Value::from(34),
			})
		);
	}

	#[tokio::test]
	async fn get_future_embedded_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: <future> { { something: [{ age: 34 }, { age: 36 }] } } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}

	#[tokio::test]
	async fn get_future_embedded_field_with_reference() {
		let (ctx, opt) = mock().await;
		let doc = Value::parse("{ name: 'Tobie', something: [{ age: 34 }, { age: 36 }] }");
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let val = Value::parse("{ test: <future> { { something: something } } }");
		let cur = doc.into();
		let mut stack = reblessive::tree::TreeStack::new();
		let res =
			stack.enter(|stk| val.get(stk, &ctx, &opt, Some(&cur), &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(vec![Value::from(map! {
				"age".to_string() => Value::from(36),
			})])
		);
	}
}
