use std::collections::BTreeMap;
use std::ops::Deref;

use futures::future::try_join_all;
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::expr::field::Fields;
use crate::expr::idiom::recursion::{Recursion, compute_idiom_recursion};
use crate::expr::part::{FindRecursionPlan, Next, NextMethod, Part, SplitByRepeatRecurse};
use crate::expr::statements::select::SelectStatement;
use crate::expr::{ControlFlow, Expr, FlowResult, FlowResultExt as _, Idiom, Literal, Lookup};
use crate::fnc::idiom;
use crate::val::{RecordIdKey, Value};

macro_rules! fallback_function {
	(if $first:expr => InvalidFunction($e:ident) then $second:expr) => {
		match $first {
			Err(e) if matches!(e.downcast_ref(), Some(Error::InvalidFunction { .. })) => {
				let $e = e;
				$second
			}
			x => x,
		}
	};
}

impl Value {
	/// Asynchronous method for getting a local or remote field from a `Value`
	///
	/// Was marked recursive
	pub(crate) async fn get(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
		path: &[Part],
	) -> FlowResult<Self> {
		// Limit recursion depth.
		if path.len() > ctx.config().limits.max_computation_depth as usize {
			return Err(ControlFlow::from(anyhow::Error::new(Error::ComputationDepthExceeded)));
		}

		let Some(first) = path.first() else {
			return Ok(self.clone());
		};

		match first {
			// The knowledge of the current value is not relevant to Part::Recurse
			Part::Recurse(recurse, inner_path, instruction) => {
				// Find the path to recurse and what path to process after the recursion is
				// finished
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
					_ => {
						// If the user already specified a recursion instruction,
						// we will not process any recursion plans.
						if instruction.is_some() {
							match path.find_recursion_plan() {
								Some(_) => {
									return Err(ControlFlow::Err(anyhow::Error::new(
										Error::RecursionInstructionPlanConflict,
									)));
								}
								_ => (path, None, after),
							}
						} else {
							match path.find_recursion_plan() {
								Some((path, plan, local_after)) => {
									(path, Some(plan), [local_after, &after].concat())
								}
								_ => (path, None, after),
							}
						}
					}
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
					instruction: instruction.as_ref(),
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
			Part::RepeatRecurse => {
				Err(ControlFlow::Err(anyhow::Error::new(Error::UnsupportedRepeatRecurse)))
			}
			Part::Doc => {
				// Try to obtain a Record ID from the document, otherwise we'll operate on NONE
				let v = match doc {
					Some(doc) => match &doc.rid {
						Some(id) => Value::RecordId(id.deref().to_owned()),
						_ => Value::None,
					},
					None => Value::None,
				};

				stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
			}
			// Get the current value at the path
			p => match self {
				// Current value at path is a geometry
				Value::Geometry(v) => match p {
					// If this is the 'type' field then continue
					Part::Field(f) if f == "type" => {
						let v = Value::from(v.as_type());
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					// If this is the 'coordinates' field then continue
					Part::Field(f) if f == "coordinates" && v.is_geometry() => {
						let v = v.as_coordinates();
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					// If this is the 'geometries' field then continue
					Part::Field(f) if f == "geometries" && v.is_collection() => {
						let v = v.as_coordinates();
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Destructure(_) => {
						let obj = Value::Object(v.as_object());
						stk.run(|stk| obj.get(stk, ctx, opt, doc, path)).await
					}
					Part::Method(name, args) => {
						let a = stk
							.scope(|scope| {
								try_join_all(
									args.iter()
										.map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
								)
							})
							.await?;
						let v = stk
							.run(|stk| idiom(stk, ctx, opt, doc, v.clone().into(), name, a))
							.await?;
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Optional => {
						stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
					}
					// Otherwise return none
					_ => Ok(Value::None),
				},
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Lookup(_) => match v.rid() {
						Some(v) => {
							let v = Value::RecordId(v);
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
					Part::Value(x) => match stk
						.run(|stk| x.compute(stk, ctx, opt, doc))
						.await
						.catch_return()?
					{
						Value::Number(n) => match v.get(&n.to_sql()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => {
								stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next()))
									.await
							}
						},
						Value::String(f) => match v.get(f.as_str()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						Value::RecordId(t) => match v.get(&t.to_sql()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						_ => stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await,
					},
					Part::All => stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await,
					Part::Destructure(p) => {
						let cur_doc = CursorDoc::from(self.clone());
						let mut obj = BTreeMap::<String, Value>::new();
						for p in p.iter() {
							let idiom = p.idiom();
							obj.insert(
								p.field().to_owned(),
								stk.run(|stk| idiom.compute(stk, ctx, opt, Some(&cur_doc))).await?,
							);
						}

						let obj = Value::from(obj);
						stk.run(|stk| obj.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Method(name, args) => {
						let args = stk
							.scope(|scope| {
								try_join_all(
									args.iter()
										.map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
								)
							})
							.await?;

						let res = stk
							.run(|stk| {
								idiom(stk, ctx, opt, doc, v.clone().into(), name, args.clone())
							})
							.await;

						let res = fallback_function! {
							if res => InvalidFunction(e) then {
								if let Some(Value::Closure(x)) = v.get(name) {
									x.invoke(stk, ctx, opt, doc, args).await
								} else {
									Err(e)
								}
							}
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
					Part::All => {
						stk.scope(|scope| {
							let futs = v.iter().map(|v| {
								scope.run(|stk| {
									let path = if v.is_record() {
										path
									} else {
										// .* applies to the elements of the array it was applied
										// to, not recursively if one of the values is an
										// array, we skip the .* part, as it implied that
										// the user collected all values of the nested array. See
										// `array_range.surql`.
										path.next()
									};

									v.get(stk, ctx, opt, doc, path)
								})
							});
							try_join_all_buffered(futs)
						})
						.await
						.map(Into::into)
					}
					Part::Flatten => {
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
					Part::Where(w) => {
						let mut a = Vec::new();
						for v in v.iter() {
							let cur = v.clone().into();
							if stk
								.run(|stk| w.compute(stk, ctx, opt, Some(&cur)))
								.await
								.catch_return()?
								.is_truthy()
							{
								a.push(v.clone());
							}
						}
						let v = Value::from(a);
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Value(x) => match stk
						.run(|stk| x.compute(stk, ctx, opt, doc))
						.await
						.catch_return()?
					{
						// TODO: Remove to_usize()
						Value::Number(i) => match v.get(i.to_usize()) {
							Some(v) => stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await,
							None => Ok(Value::None),
						},
						Value::Range(r) => {
							let v = r
								.coerce_to_typed::<i64>()
								.map_err(Error::from)
								.map_err(anyhow::Error::new)
								.map_err(ControlFlow::Err)?
								.slice(v.as_slice())
								.map(|v| Value::from(v.to_vec()))
								.unwrap_or_default();

							stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
						}
						_ => stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next())).await,
					},
					Part::Method(name, args) => {
						let a = stk
							.scope(|scope| {
								try_join_all(
									args.iter()
										.map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
								)
							})
							.await?;
						let v = stk
							.run(|stk| idiom(stk, ctx, opt, doc, v.clone().into(), name, a))
							.await?;
						stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
					}
					Part::Optional => {
						stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
					}
					_ => {
						let res = stk
							.scope(|scope| {
								let futs = v
									.iter()
									.map(|v| scope.run(|stk| v.get(stk, ctx, opt, doc, path)));
								try_join_all_buffered(futs)
							})
							.await
							.map(Value::from)?;

						Ok(res)
					}
				},
				// Current value at path is a record
				Value::RecordId(v) => {
					// Clone the record
					let val = v.clone();
					// Check how many path parts are remaining
					if path.is_empty() {
						return Ok(Value::RecordId(val));
					}

					match p {
						// This is a graph traversal expression
						Part::Lookup(g) => {
							let last_part = path.len() == 1;
							let fields = g.expr.clone().unwrap_or(Fields::value_id());
							let what = Expr::Idiom(Idiom(vec![
								Part::Start(Expr::Literal(Literal::RecordId(val.into_literal()))),
								Part::Lookup(Lookup {
									what: g.what.clone(),
									kind: g.kind.clone(),
									..Default::default()
								}),
							]));

							let stm = SelectStatement {
								fields,
								what: vec![what],
								cond: g.cond.clone(),
								limit: g.limit.clone(),
								order: g.order.clone(),
								split: g.split.clone(),
								group: g.group.clone(),
								start: g.start.clone(),
								omit: vec![],
								only: false,
								with: None,
								fetch: None,
								version: Expr::Literal(Literal::None),
								timeout: Expr::Literal(Literal::None),
								explain: None,
								tempfiles: false,
							};

							let res = stk.run(|stk| stm.compute(stk, ctx, opt, None)).await?.all();

							if last_part {
								Ok(res)
							} else {
								let res = stk
									.run(|stk| res.get(stk, ctx, opt, None, path.next()))
									.await?;

								match path.get(1) {
									Some(Part::Lookup(_)) => Ok(res.flatten()),
									Some(Part::Where(_)) => Ok(res.flatten()),
									_ => Ok(res),
								}
							}
						}
						Part::Method(name, args) => {
							let a = stk
								.scope(|scope| {
									try_join_all(
										args.iter().map(|v| {
											scope.run(|stk| v.compute(stk, ctx, opt, doc))
										}),
									)
								})
								.await?;

							let res = stk
								.run(|stk| {
									idiom(stk, ctx, opt, doc, v.clone().into(), name, a.clone())
								})
								.await?;

							stk.run(|stk| res.get(stk, ctx, opt, doc, path.next())).await
						}
						Part::Optional => {
							stk.run(|stk| self.get(stk, ctx, opt, doc, path.next())).await
						}
						// This is a remote field expression with one exception
						// If the RecordId is array-based, and we try to access an index,
						// then we return that index of the RecordId's array.
						p => {
							// Discover what the path is that we need to continue with
							let next = match (p, &val.key) {
								// If the computed value is a number, and the RecordIdKey is an
								// array, then we return the value at the index of the
								// array. Otherwise, we return the computed value and the
								// next path part.
								(Part::Value(x), RecordIdKey::Array(arr)) => {
									match stk
										.run(|stk| x.compute(stk, ctx, opt, doc))
										.await
										.catch_return()?
									{
										Value::Number(n) => {
											return match arr.get(n.to_usize()) {
												Some(v) => {
													stk.run(|stk| {
														v.get(stk, ctx, opt, doc, path.next())
													})
													.await
												}
												None => Ok(Value::None),
											};
										}
										x => &[&[Part::Value(x.into_literal())], path.next()]
											.concat(),
									}
								}

								// No special case, fetch the document and continue processing the
								// path
								_ => path,
							};

							// Fetch the record id's contents
							let v = val
								.select_document(stk, ctx, opt, doc)
								.await?
								.map(Value::Object)
								.unwrap_or(Value::None);

							// Continue processing the path on the now fetched record
							stk.run(|stk| v.get(stk, ctx, opt, None, next)).await
						}
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
							let a = stk
								.scope(|scope| {
									try_join_all(
										args.iter().map(|v| {
											scope.run(|stk| v.compute(stk, ctx, opt, doc))
										}),
									)
								})
								.await?;
							let v = stk
								.run(|stk| idiom(stk, ctx, opt, doc, v.clone(), name, a))
								.await?;
							stk.run(|stk| v.get(stk, ctx, opt, doc, path.next())).await
						}
						// Only continue processing the path from the point that it contains a
						// method
						_ => {
							stk.run(|stk| Value::None.get(stk, ctx, opt, doc, path.next_method()))
								.await
						}
					}
				}
			},
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::sql::idiom::Idiom as SqlIdiom;
	use crate::syn;
	use crate::val::RecordId;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn get_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = SqlIdiom::default().into();
		let val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn get_basic() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let val: Value = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(123));
	}

	#[tokio::test]
	async fn get_basic_deep_ok() {
		let (ctx, opt) = mock().await;
		let depth = 20;
		let idi: Idiom = syn::idiom(&format!("{}something", "test.".repeat(depth))).unwrap().into();
		let val: Value = parse_val!(&format!(
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
		let idi: Idiom = syn::idiom(&format!("{}something", "test.".repeat(depth))).unwrap().into();
		let val: Value = parse_val!("{}"); // A deep enough object cannot be parsed.
		let mut stack = reblessive::tree::TreeStack::new();
		let err = stack
			.enter(|stk| val.get(stk, &ctx, &opt, None, &idi))
			.finish()
			.await
			.catch_return()
			.unwrap_err();

		assert!(
			matches!(err.downcast_ref(), Some(Error::ComputationDepthExceeded)),
			"expected computation depth exceeded, got {:?}",
			err
		);
	}

	#[tokio::test]
	async fn get_thing() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.other").unwrap().into();
		let val: Value = parse_val!("{ test: { other: test:tobie, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(RecordId {
				table: "test".into(),
				key: RecordIdKey::String("tobie".to_owned())
			})
		);
	}

	#[tokio::test]
	async fn get_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [123, 456, 789] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(456));
	}

	#[tokio::test]
	async fn get_array_thing() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [test:tobie, test:jaime] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(
			res,
			Value::from(RecordId {
				table: "test".into(),
				key: RecordIdKey::String("jaime".to_owned())
			})
		);
	}

	#[tokio::test]
	async fn get_array_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(36));
	}

	#[tokio::test]
	async fn get_array_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}

	#[tokio::test]
	async fn get_array_fields_flat() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, [Value::from(34i64), Value::from(36i64)].into_iter().collect::<Value>());
	}

	#[tokio::test]
	async fn get_array_where_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35].age").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, [Value::from(36i64)].into_iter().collect::<Value>());
	}

	#[tokio::test]
	async fn get_array_where_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35]").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
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
		let idi: Idiom = syn::idiom("test.something[WHERE age > 30][0]").unwrap().into();
		let val: Value = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
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
	async fn get_object_with_thing_based_key() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test[city:london]").unwrap().into();
		let val: Value =
			parse_val!("{ test: { 'city:london': true, other: test:tobie, something: 123 } }");
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.get(stk, &ctx, &opt, None, &idi)).finish().await.unwrap();
		assert_eq!(res, Value::from(true));
	}
}
