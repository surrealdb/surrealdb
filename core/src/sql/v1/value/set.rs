use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;
use async_recursion::async_recursion;

impl Value {
	/// Asynchronous method for setting a field on a `Value`
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn set(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
		val: Value,
	) -> Result<(), Error> {
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Graph(g) => match v.get_mut(g.to_raw().as_str()) {
						Some(v) if v.is_some() => v.set(ctx, opt, txn, path.next(), val).await,
						_ => {
							let mut obj = Value::base();
							obj.set(ctx, opt, txn, path.next(), val).await?;
							v.insert(g.to_raw(), obj);
							Ok(())
						}
					},
					Part::Field(f) => match v.get_mut(f.as_str()) {
						Some(v) if v.is_some() => v.set(ctx, opt, txn, path.next(), val).await,
						_ => {
							let mut obj = Value::base();
							obj.set(ctx, opt, txn, path.next(), val).await?;
							v.insert(f.to_raw(), obj);
							Ok(())
						}
					},
					Part::Index(i) => match v.get_mut(&i.to_string()) {
						Some(v) if v.is_some() => v.set(ctx, opt, txn, path.next(), val).await,
						_ => {
							let mut obj = Value::base();
							obj.set(ctx, opt, txn, path.next(), val).await?;
							v.insert(i.to_string(), obj);
							Ok(())
						}
					},
					Part::Value(x) => match x.compute(ctx, opt, txn, None).await? {
						Value::Strand(f) => match v.get_mut(f.as_str()) {
							Some(v) if v.is_some() => v.set(ctx, opt, txn, path.next(), val).await,
							_ => {
								let mut obj = Value::base();
								obj.set(ctx, opt, txn, path.next(), val).await?;
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
						let futs = v.iter_mut().map(|v| v.set(ctx, opt, txn, path, val.clone()));
						try_join_all_buffered(futs).await?;
						Ok(())
					}
					Part::First => match v.first_mut() {
						Some(v) => v.set(ctx, opt, txn, path.next(), val).await,
						None => Ok(()),
					},
					Part::Last => match v.last_mut() {
						Some(v) => v.set(ctx, opt, txn, path.next(), val).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.get_mut(i.to_usize()) {
						Some(v) => v.set(ctx, opt, txn, path.next(), val).await,
						None => Ok(()),
					},
					Part::Where(w) => match path.next().first() {
						Some(Part::Index(_)) => {
							let mut a = Vec::new();
							let mut p = Vec::new();
							// Store the elements and positions to update
							for (i, o) in v.iter_mut().enumerate() {
								let cur = o.into();
								if w.compute(ctx, opt, txn, Some(&cur)).await?.is_truthy() {
									a.push(o.clone());
									p.push(i);
								}
							}
							// Convert the matched elements array to a value
							let mut a = Value::from(a);
							// Set the new value on the matches elements
							a.set(ctx, opt, txn, path.next(), val.clone()).await?;
							// Push the new values into the original array
							for (i, p) in p.into_iter().enumerate() {
								v[p] = a.pick(&[Part::Index(i.into())]);
							}
							Ok(())
						}
						_ => {
							let path = path.next();
							for v in v.iter_mut() {
								let cur = v.into();
								if w.compute(ctx, opt, txn, Some(&cur)).await?.is_truthy() {
									v.set(ctx, opt, txn, path, val.clone()).await?;
								}
							}
							Ok(())
						}
					},
					Part::Value(x) => match x.compute(ctx, opt, txn, None).await? {
						Value::Number(i) => match v.get_mut(i.to_usize()) {
							Some(v) => v.set(ctx, opt, txn, path.next(), val).await,
							None => Ok(()),
						},
						_ => Ok(()),
					},
					_ => {
						let futs = v.iter_mut().map(|v| v.set(ctx, opt, txn, path, val.clone()));
						try_join_all_buffered(futs).await?;
						Ok(())
					}
				},
				// Current value at path is empty
				Value::Null => {
					*self = Value::base();
					self.set(ctx, opt, txn, path, val).await
				}
				// Current value at path is empty
				Value::None => {
					*self = Value::base();
					self.set(ctx, opt, txn, path, val).await
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
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::default();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("999");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_empty() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::None;
		let res = Value::parse("{ test: 999 }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_blank() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::None;
		let res = Value::parse("{ test: { something: 999 } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_reset() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: 999 }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 999 } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_allow() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.allow");
		let mut val = Value::parse("{ test: { other: null } }");
		let res = Value::parse("{ test: { other: null, something: { allow: 999 } } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_wrong() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_other() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: { something: 999 }, something: 123 } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 999, 789] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(999)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, 21] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0]");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [21, { age: 36 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn set_array_where_fields_array_index_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 36 }] } }");
		val.set(&ctx, &opt, &txn, &idi, Value::from(21)).await.unwrap();
		assert_eq!(res, val);
	}
}
