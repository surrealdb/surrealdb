use std::collections::BTreeMap;
use std::ops::Deref;

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
use crate::sql::part::{FindRecursionPlan, Next, NextMethod, SplitByRepeatRecurse};
use crate::sql::part::{Part, Skip};
use crate::sql::paths::ID;
use crate::sql::statements::select::SelectStatement;
use crate::sql::thing::Thing;
use crate::sql::value::{Value, Values};
use crate::sql::Function;
use reblessive::tree::Stk;

use super::idiom_recursion::{compute_idiom_recursion, Recursion};

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
			// The knowledge of the current value is not relevant to Part::Recurse
			Some(Part::Recurse(recurse, inner_path)) => {
				// Find the path to recurse and what path to process after the recursion is finished
				let (path, after) = match inner_path {
					Some(p) => (p.0.as_slice(), path.next().to_vec()),
					_ => (path.next(), vec![]),
				};

				// We first try to split out a root-level repeat-recurse symbol
				// By doing so, we can eliminate un-needed recursion, as we can
				// simply loop.
				let (path, plan, after) = match path.split_by_repeat_recurse() {
					Some((path, local_after)) => (path, None, [local_after, &after].concat()),

					// If we do not find a root-level repeat-recurse symbol, we
					// can scan for a nested one. We only ever allow for a single
					// repeat recurse symbol, hence the separate check.
					_ => match path.find_recursion_plan() {
						Some((path, plan, local_after)) => {
							(path, Some(plan), [local_after, &after].concat())
						}
						_ => (path, None, after),
					},
				};

				// Collect the min & max for the recursion context
				let (min, max) = recurse.to_owned().try_into()?;
				// Construct the recursion context
				let rec = Recursion {
					min,
					max,
					iterated: 0,
					current: self,
					path,
					plan: plan.as_ref(),
				};

				// Compute the recursion
				let v = compute_idiom_recursion(stk, ctx, opt, doc, rec).await?;

				// If we have a leftover path, process it
				if !after.is_empty() {
					stk.run(|stk| v.get(stk, ctx, opt, doc, after.as_slice())).await
				} else {
					Ok(v)
				}
			}
			// We only support repeat recurse symbol in certain scenarios, to
			// ensure we can process them efficiently. When encountering a
			// recursion part, it will find the repeat recurse part and handle
			// it. If we find one in any unsupported scenario, we throw an error.
			Some(Part::RepeatRecurse) => Err(Error::UnsupportedRepeatRecurse),
			Some(Part::Doc) => {
				// Try to obtain a Record ID from the document, otherwise we'll operate on NONE
				let v = match doc {
					Some(doc) => match &doc.rid {
						Some(id) => Value::Thing(id.deref().to_owned()),
						_ => Value::None,
					},
					None => Value::None,
				};

				stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
			}
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
					Part::Optional => {
						stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
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
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Graph(_) => match v.rid() {
						Some(v) => {
							let v = Value::Thing(v);
							stk.run(|stk| v.get(stk, ctx, opt, doc, path)).await
						}
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Field(f) => match v.get(f.as_str()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Index(i) => match v.get(&i.to_string()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Value(x) => match stk.run(|stk| x.compute(stk, ctx, opt, doc)).await? {
						Value::Strand(f) => match v.get(f.as_str()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						Value::Thing(t) => match v.get(&t.to_raw()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						_ => stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await,
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
					Part::Optional => {
						stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
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
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Last => match v.last() {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
					},
					Part::Index(i) => match v.get(i.to_usize()) {
						Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						None => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await
						}
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
						Value::Range(r) => {
							if let Some(range) = r.slice(v.as_slice()) {
								let path = path.next();
								stk.scope(|scope| {
									let futs = range
										.iter()
										.map(|v| scope.run(|stk| v.get(stk, ctx, opt, doc, path)));
									try_join_all_buffered(futs)
								})
								.await
								.map(Into::into)
							} else {
								Ok(Value::None)
							}
						}
						_ => stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await,
					},
					Part::Method(name, args) => {
						let v = stk
							.run(|stk| {
								idiom(stk, ctx, opt, doc, v.clone().into(), name, args.clone())
							})
							.await?;
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Optional => {
						stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
					}
					_ => {
						let len = match path.get(1) {
							// Say that we have a path like `[a:1].out.*`, then `.*`
							// references `out` and not the resulting array of `[a:1].out`
							Some(Part::All) => 2,
							_ => 1,
						};

						let mapped = stk
							.scope(|scope| {
								let futs = v.iter().map(|v| {
									scope.run(|stk| v.get(stk, ctx, opt, doc, &path[0..len]))
								});
								try_join_all_buffered(futs)
							})
							.await
							.map(Value::from)?;

						// If we are chaining graph parts, we need to make sure to flatten the result
						let mapped = match (path.first(), path.get(1)) {
							(Some(Part::Graph(_)), Some(Part::Graph(_))) => mapped.flatten(),
							(Some(Part::Graph(_)), Some(Part::Where(_))) => mapped.flatten(),
							_ => mapped,
						};

						stk.run(|stk| mapped.get(stk, ctx, opt, doc, path.skip(len))).await
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
							let v = stk.run(|stk| stm.compute(stk, ctx, opt, None)).await?.all();
							stk.run(|stk| v.get(stk, ctx, opt, None, path)).await?.flatten().ok()
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
										// is a graph or where part. Reason being that if we flatten
										// fields, the results of those fields (which could be arrays)
										// will be merged into each other. So [1, 2, 3], [4, 5, 6] would
										// become [1, 2, 3, 4, 5, 6]. This slice access won't panic
										// as we have already checked the length of the path.
										Ok(match path[1] {
											Part::Graph(_) | Part::Where(_) => res.flatten(),
											_ => res,
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
							Part::Optional => {
								stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
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
						Part::Optional => match &self {
							Value::None => Ok(Value::None),
							v => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
						},
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

	#[tokio::test]
	async fn get_object_with_thing_based_key() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test[city:london]");
		let val =
			Value::parse("{ test: { 'city:london': true, other: test:tobie, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(true));
	}
}
