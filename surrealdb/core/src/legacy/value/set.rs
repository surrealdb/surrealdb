use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_collections::Entry;
use surrealdb_types::ToSql;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exe::{FlowResultExt as _, try_join_all_buffered};
use crate::expr::part::Part;
use crate::expr::{Expr, Literal};
use crate::val::{Object, Strand, Value};

/// Asynchronous method for setting a field on a `Value`
pub(crate) async fn value_set(
	this: &mut Value,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	path: &[Part],
	val: Value,
) -> Result<()> {
	if path.is_empty() {
		*this = val;
		return Ok(());
	}

	let mut iter = path.iter();
	let mut place = this;
	let mut prev = path;

	// Index forward trying to find the location where to insert the value
	// Whenever we hit an existing path in the value we update place to point to the
	// new value. If we hit a dead end, we then assign the into that dead end. If
	// any path is not yet matched we use that to create an object to assign.
	while let Some(p) = iter.next() {
		match place {
			Value::RecordId(_) | Value::Null | Value::None => {
				// any index is guaranteed to fail so just assign to this place.
				return value_assign(stk, ctx, opt, place, val, prev).await;
			}
			_ => {}
		}

		match p {
			Part::Lookup(lookup) => {
				match place {
					Value::Object(obj) => match obj.entry(lookup.to_sql()) {
						Entry::Vacant(x) => {
							let v = x.insert(Value::None);
							return value_assign(stk, ctx, opt, v, val, iter.as_slice()).await;
						}
						Entry::Occupied(x) => {
							place = x.into_mut();
						}
					},
					Value::Array(arr) => {
						// Apply to all entries of the array
						stk.scope(|scope| {
							let futs = arr.iter_mut().map(|v| {
								scope.run(|stk| {
									crate::legacy::value_set(v, stk, ctx, opt, prev, val.clone())
								})
							});
							try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
						})
						.await?;
						return Ok(());
					}
					_ => return Ok(()),
				};
			}
			Part::Field(f) => {
				match place {
					Value::Object(obj) => match obj.entry(f.as_str().to_owned()) {
						Entry::Vacant(x) => {
							let v = x.insert(Value::None);
							return value_assign(stk, ctx, opt, v, val, iter.as_slice()).await;
						}
						Entry::Occupied(x) => {
							place = x.into_mut();
						}
					},
					Value::Array(arr) => {
						// Apply to all entries of the array
						stk.scope(|scope| {
							let futs = arr.iter_mut().map(|v| {
								scope.run(|stk| {
									crate::legacy::value_set(v, stk, ctx, opt, prev, val.clone())
								})
							});
							try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
						})
						.await?;
						return Ok(());
					}
					_ => return Ok(()),
				};
			}
			Part::Value(x) => {
				let v = stk
					.run(|stk| crate::legacy::expr_compute(x, stk, ctx, opt, None))
					.await
					.catch_return()?;

				match place {
					Value::Object(obj) => {
						let v = match v {
							Value::String(x) => x.as_str().to_owned(),
							x => x.to_sql(),
						};

						match obj.entry(v) {
							Entry::Vacant(x) => {
								let v = x.insert(Value::None);
								return value_assign(stk, ctx, opt, v, val, iter.as_slice()).await;
							}
							Entry::Occupied(x) => {
								place = x.into_mut();
							}
						}
					}
					Value::Array(arr) => match v {
						Value::Range(x) => {
							if let Some(v) = x.coerce_to_typed::<i64>()?.slice_mut(arr) {
								let path = iter.as_slice();
								stk.scope(|scope| {
									let futs = v.iter_mut().map(|v| {
										scope.run(|stk| {
											crate::legacy::value_set(
												v,
												stk,
												ctx,
												opt,
												path,
												val.clone(),
											)
										})
									});
									try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
								})
								.await?;
								return Ok(());
							} else {
								return Ok(());
							}
						}
						Value::Number(i) => {
							let Some(idx) = i.as_array_index() else {
								return Ok(());
							};
							if let Some(v) = arr.get_mut(idx) {
								place = v;
							} else {
								return Ok(());
							}
						}
						_ => return Ok(()),
					},
					_ => return Ok(()),
				}
			}
			Part::First => match place {
				Value::Array(arr) => {
					let Some(x) = arr.first_mut() else {
						return Ok(());
					};
					place = x;
				}
				Value::Set(set) => {
					let Some(x) = set.first_mut() else {
						return Ok(());
					};
					place = x;
				}
				_ => return Ok(()),
			},
			Part::Last => match place {
				Value::Array(arr) => {
					let Some(x) = arr.last_mut() else {
						return Ok(());
					};
					place = x;
				}
				Value::Set(set) => {
					let Some(x) = set.last_mut() else {
						return Ok(());
					};
					place = x;
				}
				_ => return Ok(()),
			},
			Part::All => {
				let path = iter.as_slice();
				match place {
					Value::Array(v) => {
						stk.scope(|scope| {
							let futs = v.iter_mut().map(|v| {
								scope.run(|stk| {
									crate::legacy::value_set(v, stk, ctx, opt, path, val.clone())
								})
							});
							try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
						})
						.await?;
					}
					Value::Object(v) => {
						stk.scope(|scope| {
							let futs = v.iter_mut().map(|(_, v)| {
								scope.run(|stk| {
									crate::legacy::value_set(v, stk, ctx, opt, path, val.clone())
								})
							});
							try_join_all_buffered(futs, ctx.config.max_concurrent_tasks)
						})
						.await?;
					}
					_ => (),
				};

				return Ok(());
			}
			Part::Where(w) => {
				let Value::Array(arr) = place else {
					return Ok(());
				};
				if iter.as_slice().first().and_then(|x| x.as_old_index()).is_some() {
					let mut a = Vec::new();
					let mut p = Vec::new();
					// Store the elements and positions to update
					for (i, o) in arr.iter_mut().enumerate() {
						let cur = o.clone().into();
						if stk
							.run(|stk| crate::legacy::expr_compute(w, stk, ctx, opt, Some(&cur)))
							.await
							.catch_return()?
							.is_truthy()
						{
							a.push(o.clone());
							p.push(i);
						}
					}
					// Convert the matched elements array to a value
					let mut a = Value::from(a);
					// Set the new value on the matches elements
					stk.run(|stk| {
						crate::legacy::value_set(
							&mut a,
							stk,
							ctx,
							opt,
							iter.as_slice(),
							val.clone(),
						)
					})
					.await?;
					// Push the new values into the original array
					for (i, p) in p.into_iter().enumerate() {
						arr[p] = a.pick(&[Part::Value(Expr::Literal(Literal::Integer(i as i64)))]);
					}
					return Ok(());
				} else {
					for v in arr.iter_mut() {
						let cur = v.clone().into();
						if stk
							.run(|stk| crate::legacy::expr_compute(w, stk, ctx, opt, Some(&cur)))
							.await
							.catch_return()?
							.is_truthy()
						{
							stk.run(|stk| {
								crate::legacy::value_set(
									v,
									stk,
									ctx,
									opt,
									iter.as_slice(),
									val.clone(),
								)
							})
							.await?;
						}
					}
					return Ok(());
				}
			}
			_ => return Ok(()),
		}
		prev = iter.as_slice();
	}

	*place = val;
	Ok(())
}

pub(crate) async fn value_assign(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	place: &mut Value,
	mut val: Value,
	path: &[Part],
) -> Result<()> {
	for p in path.iter().rev() {
		let name: Strand = match p {
			Part::Lookup(x) => x.to_sql().into(),
			Part::Field(f) => f.as_str().into(),
			Part::Value(x) => {
				let v = stk
					.run(|stk| crate::legacy::expr_compute(x, stk, ctx, opt, None))
					.await
					.catch_return()?;
				match v {
					Value::String(x) => x,
					Value::RecordId(x) => x.to_sql().into(),
					Value::Number(x) => x.to_sql().into(),
					Value::Range(x) => x.to_sql().into(),
					_ => return Ok(()),
				}
			}
			x => {
				if let Some(idx) = x.as_old_index() {
					idx.to_string().into()
				} else {
					return Ok(());
				}
			}
		};
		let mut object = Object::default();
		object.insert(name, val);
		val = object.into();
	}

	*place = val;
	Ok(())
}

#[cfg(test)]
mod tests {

	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::syn;
	use crate::val::Value;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn set_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = Default::default();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("999");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_empty() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = Value::None;
		let res = parse_val!("{ test: 999 }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_blank() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = Value::None;
		let res = parse_val!("{ test: { something: 999 } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_reset() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: 999 }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_basic() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 999 } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_allow() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.allow").unwrap().into();
		let mut val = parse_val!("{ test: { other: null } }");
		let res = parse_val!("{ test: { other: null, something: { allow: 999 } } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_wrong() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.wrong").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_other() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.other.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: { something: 999 }, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [123, 456, 789] } }");
		let res = parse_val!("{ test: { something: [123, 999, 789] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(999))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields_flat() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35].age").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 34 }, 21] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 30][0]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [21, { age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 30][0].age").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 21 }, { age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(21)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.other['inner']").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!(
			"{ test: { other: { inner: true }, something: [{ age: 34 }, { age: 36 }] } }"
		);
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(true))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field_in_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.other['inner']").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!(
			"{ test: { something: [{ age: 34, other: { inner: true } }, { age: 36, other: { inner: true } }] } }"
		);
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(true))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field_in_array_with_thing() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.other[city:london]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!(
			"{ test: { something: [{ age: 34, other: { 'city:london': true } }, { age: 36, other: { 'city:london': true } }] } }"
		);
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| {
				crate::legacy::value_set(&mut val, stk, &ctx, &opt, &idi, Value::from(true))
			})
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
