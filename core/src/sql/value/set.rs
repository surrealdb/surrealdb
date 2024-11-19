use std::collections::btree_map::Entry;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::sql::part::Part;
use crate::sql::value::Value;
use crate::sql::Object;
use reblessive::tree::Stk;

impl Value {
	/// Asynchronous method for setting a field on a `Value`
	///
	/// Was marked recursive
	pub(crate) async fn set(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		if path.is_empty() {
			*self = val;
			return Ok(());
		}

		let mut iter = path.iter();
		let mut place = self;
		let mut prev = path;

		// Index forward trying to find the location where to insert the value
		// Whenever we hit an existing path in the value we update place to point to the new value.
		// If we hit a dead end, we then assign the into that dead end. If any path is not yet
		// matched we use that to create an object to assign.
		while let Some(p) = iter.next() {
			match place {
				Value::Thing(_) | Value::Null | Value::None => {
					// any index is guaranteed to fail so just assign to this place.
					return Self::assign(stk, ctx, opt, place, val, prev).await;
				}
				_ => {}
			}

			match p {
				Part::Graph(g) => {
					match place {
						Value::Object(obj) => match obj.entry(g.to_raw()) {
							Entry::Vacant(x) => {
								let v = x.insert(Value::None);
								return Self::assign(stk, ctx, opt, v, val, iter.as_slice()).await;
							}
							Entry::Occupied(x) => {
								place = x.into_mut();
							}
						},
						Value::Array(arr) => {
							// Apply to all entries of the array
							stk.scope(|scope| {
								let futs = arr.iter_mut().map(|v| {
									scope.run(|stk| v.set(stk, ctx, opt, prev, val.clone()))
								});
								try_join_all_buffered(futs)
							})
							.await?;
							return Ok(());
						}
						_ => return Ok(()),
					};
				}
				Part::Field(f) => {
					match place {
						Value::Object(obj) => match obj.entry(f.0.clone()) {
							Entry::Vacant(x) => {
								let v = x.insert(Value::None);
								return Self::assign(stk, ctx, opt, v, val, iter.as_slice()).await;
							}
							Entry::Occupied(x) => {
								place = x.into_mut();
							}
						},
						Value::Array(arr) => {
							// Apply to all entries of the array
							stk.scope(|scope| {
								let futs = arr.iter_mut().map(|v| {
									scope.run(|stk| v.set(stk, ctx, opt, prev, val.clone()))
								});
								try_join_all_buffered(futs)
							})
							.await?;
							return Ok(());
						}
						_ => return Ok(()),
					};
				}
				Part::Index(f) => match place {
					Value::Object(obj) => match obj.entry(f.to_string()) {
						Entry::Vacant(x) => {
							let v = x.insert(Value::None);
							return Self::assign(stk, ctx, opt, v, val, iter.as_slice()).await;
						}
						Entry::Occupied(x) => {
							place = x.into_mut();
						}
					},
					Value::Array(arr) => {
						if let Some(x) = arr.get_mut(f.to_usize()) {
							place = x
						} else {
							return Ok(());
						}
					}
					_ => return Ok(()),
				},
				Part::Value(x) => {
					let v = stk.run(|stk| x.compute(stk, ctx, opt, None)).await?;

					match place {
						Value::Object(obj) => {
							let v = match v {
								Value::Strand(x) => x.0.clone(),
								x => x.to_string(),
							};

							match obj.entry(v) {
								Entry::Vacant(x) => {
									let v = x.insert(Value::None);
									return Self::assign(stk, ctx, opt, v, val, iter.as_slice())
										.await;
								}
								Entry::Occupied(x) => {
									place = x.into_mut();
								}
							}
						}
						Value::Array(arr) => match v {
							Value::Range(x) => {
								if let Some(v) = x.slice_mut(arr) {
									let path = iter.as_slice();
									stk.scope(|scope| {
										let futs = v.iter_mut().map(|v| {
											scope.run(|stk| v.set(stk, ctx, opt, path, val.clone()))
										});
										try_join_all_buffered(futs)
									})
									.await?;
									return Ok(());
								} else {
									return Ok(());
								}
							}
							Value::Number(i) => {
								if let Some(v) = arr.get_mut(i.to_usize()) {
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
				Part::First => {
					let Value::Array(arr) = place else {
						return Ok(());
					};
					let Some(x) = arr.first_mut() else {
						return Ok(());
					};
					place = x
				}
				Part::Last => {
					let Value::Array(arr) = place else {
						return Ok(());
					};
					let Some(x) = arr.last_mut() else {
						return Ok(());
					};
					place = x
				}
				Part::All => {
					let path = iter.as_slice();
					match place {
						Value::Array(v) => {
							stk.scope(|scope| {
								let futs = v.iter_mut()
									.map(|v| scope.run(|stk| v.set(stk, ctx, opt, path, val.clone())));
								try_join_all_buffered(futs)
							})
							.await?;
						},
						Value::Object(v) => {
							stk.scope(|scope| {
								let futs = v.iter_mut()
									.map(|(_, v)| scope.run(|stk| v.set(stk, ctx, opt, path, val.clone())));
								try_join_all_buffered(futs)
							})
							.await?;
						},
						_ => ()
					};

					return Ok(());
				}
				Part::Where(w) => {
					let Value::Array(arr) = place else {
						return Ok(());
					};
					if let Some(Part::Index(_)) = iter.as_slice().first() {
						let mut a = Vec::new();
						let mut p = Vec::new();
						// Store the elements and positions to update
						for (i, o) in arr.iter_mut().enumerate() {
							let cur = o.clone().into();
							if w.compute(stk, ctx, opt, Some(&cur)).await?.is_truthy() {
								a.push(o.clone());
								p.push(i);
							}
						}
						// Convert the matched elements array to a value
						let mut a = Value::from(a);
						// Set the new value on the matches elements
						stk.run(|stk| a.set(stk, ctx, opt, iter.as_slice(), val.clone())).await?;
						// Push the new values into the original array
						for (i, p) in p.into_iter().enumerate() {
							arr[p] = a.pick(&[Part::Index(i.into())]);
						}
						return Ok(());
					} else {
						for v in arr.iter_mut() {
							let cur = v.clone().into();
							if w.compute(stk, ctx, opt, Some(&cur)).await?.is_truthy() {
								stk.run(|stk| v.set(stk, ctx, opt, iter.as_slice(), val.clone()))
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

	async fn assign(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		place: &mut Value,
		mut val: Value,
		path: &[Part],
	) -> Result<(), Error> {
		for p in path.iter().rev() {
			let name = match p {
				Part::Graph(x) => x.to_raw(),
				Part::Field(f) => f.0.clone(),
				Part::Index(i) => i.to_string(),
				Part::Value(x) => {
					let v = stk.run(|stk| x.compute(stk, ctx, opt, None)).await?;
					match v {
						Value::Strand(x) => x.0,
						Value::Thing(x) => x.to_raw(),
						Value::Number(x) => x.to_string(),
						Value::Range(x) => x.to_string(),
						_ => return Ok(()),
					}
				}
				_ => return Ok(()),
			};
			let mut object = Object::default();
			object.insert(name, val);
			val = object.into();
		}

		*place = val;
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::idiom::Idiom;
	use crate::syn::Parse;

	#[tokio::test]
	async fn set_none() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::default();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("999");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_empty() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::None;
		let res = Value::parse("{ test: 999 }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_blank() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::None;
		let res = Value::parse("{ test: { something: 999 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_reset() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: 999 }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_basic() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 999 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_allow() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.allow");
		let mut val = Value::parse("{ test: { other: null } }");
		let res = Value::parse("{ test: { other: null, something: { allow: 999 } } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_wrong() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_other() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: { something: 999 }, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 999, 789] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(999))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields_flat() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, 21] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [21, { age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(21))).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.other['inner']");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse(
			"{ test: { other: { inner: true }, something: [{ age: 34 }, { age: 36 }] } }",
		);
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(true)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field_in_array() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.other['inner']");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34, other: { inner: true } }, { age: 36, other: { inner: true } }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(true)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_object_with_new_nested_array_access_field_in_array_with_thing() {
		let (ctx, opt) = mock().await;
		let idi = Idiom::parse("test.something.other[city:london]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34, other: { 'city:london': true } }, { age: 36, other: { 'city:london': true } }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack
			.enter(|stk| val.set(stk, &ctx, &opt, &idi, Value::from(true)))
			.finish()
			.await
			.unwrap();
		assert_eq!(res, val);
	}
}
