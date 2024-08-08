use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;
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
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Graph(g) => match v.get_mut(g.to_raw().as_str()) {
						Some(v) if v.is_some() => {
							stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await
						}
						_ => {
							let mut obj = Value::base();
							stk.run(|stk| obj.set(stk, ctx, opt, path.next(), val)).await?;
							v.insert(g.to_raw(), obj);
							Ok(())
						}
					},
					Part::Field(f) => match v.get_mut(f.as_str()) {
						Some(v) if v.is_some() => {
							stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await
						}
						_ => {
							let mut obj = Value::base();
							stk.run(|stk| obj.set(stk, ctx, opt, path.next(), val)).await?;
							v.insert(f.to_raw(), obj);
							Ok(())
						}
					},
					Part::Index(i) => match v.get_mut(&i.to_string()) {
						Some(v) if v.is_some() => {
							stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await
						}
						_ => {
							let mut obj = Value::base();
							stk.run(|stk| obj.set(stk, ctx, opt, path.next(), val)).await?;
							v.insert(i.to_string(), obj);
							Ok(())
						}
					},
					Part::Value(x) => match stk.run(|stk| x.compute(stk, ctx, opt, None)).await? {
						Value::Strand(f) => match v.get_mut(f.as_str()) {
							Some(v) if v.is_some() => {
								stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await
							}
							_ => {
								let mut obj = Value::base();
								stk.run(|stk| obj.set(stk, ctx, opt, path.next(), val)).await?;
								v.insert(f.to_string(), obj);
								Ok(())
							}
						},
						_ => Ok(()),
					},
					_ => Ok(()),
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();

						stk.scope(|scope| {
							let futs = v
								.iter_mut()
								.map(|v| scope.run(|stk| v.set(stk, ctx, opt, path, val.clone())));
							try_join_all_buffered(futs)
						})
						.await?;
						Ok(())
					}
					Part::First => match v.first_mut() {
						Some(v) => stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await,
						None => Ok(()),
					},
					Part::Last => match v.last_mut() {
						Some(v) => stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.get_mut(i.to_usize()) {
						Some(v) => stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await,
						None => Ok(()),
					},
					Part::Where(w) => match path.next().first() {
						Some(Part::Index(_)) => {
							let mut a = Vec::new();
							let mut p = Vec::new();
							// Store the elements and positions to update
							for (i, o) in v.iter_mut().enumerate() {
								let cur = o.clone().into();
								if w.compute(stk, ctx, opt, Some(&cur)).await?.is_truthy() {
									a.push(o.clone());
									p.push(i);
								}
							}
							// Convert the matched elements array to a value
							let mut a = Value::from(a);
							// Set the new value on the matches elements
							stk.run(|stk| a.set(stk, ctx, opt, path.next(), val.clone())).await?;
							// Push the new values into the original array
							for (i, p) in p.into_iter().enumerate() {
								v[p] = a.pick(&[Part::Index(i.into())]);
							}
							Ok(())
						}
						_ => {
							let path = path.next();
							for v in v.iter_mut() {
								let cur = v.clone().into();
								if w.compute(stk, ctx, opt, Some(&cur)).await?.is_truthy() {
									stk.run(|stk| v.set(stk, ctx, opt, path, val.clone())).await?;
								}
							}
							Ok(())
						}
					},
					Part::Value(x) => match x.compute(stk, ctx, opt, None).await? {
						Value::Number(i) => match v.get_mut(i.to_usize()) {
							Some(v) => stk.run(|stk| v.set(stk, ctx, opt, path.next(), val)).await,
							None => Ok(()),
						},
						_ => Ok(()),
					},
					_ => {
						stk.scope(|scope| {
							let futs = v
								.iter_mut()
								.map(|v| scope.run(|stk| v.set(stk, ctx, opt, path, val.clone())));
							try_join_all_buffered(futs)
						})
						.await?;

						Ok(())
					}
				},
				// Current value at path is empty
				Value::Null => {
					*self = Value::base();
					stk.run(|stk| self.set(stk, ctx, opt, path, val)).await
				}
				// Current value at path is empty
				Value::None => {
					*self = Value::base();
					stk.run(|stk| self.set(stk, ctx, opt, path, val)).await
				}
				// Ignore everything else
				_ => Ok(()),
			},
			// No more parts so set the value
			None => {
				*self = val;
				Ok(())
			}
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
}
