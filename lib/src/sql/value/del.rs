use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::sql::array::Abolish;
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::value::Value;
use async_recursion::async_recursion;
use std::collections::HashSet;

impl Value {
	/// Asynchronous method for deleting a field from a `Value`
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn del(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
	) -> Result<(), Error> {
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::Field(f) => match path.len() {
						1 => {
							v.remove(f.as_str());
							Ok(())
						}
						_ => match v.get_mut(f.as_str()) {
							Some(v) if v.is_some() => v.del(ctx, opt, txn, path.next()).await,
							_ => Ok(()),
						},
					},
					Part::Index(i) => match path.len() {
						1 => {
							v.remove(&i.to_string());
							Ok(())
						}
						_ => match v.get_mut(&i.to_string()) {
							Some(v) if v.is_some() => v.del(ctx, opt, txn, path.next()).await,
							_ => Ok(()),
						},
					},
					Part::Value(x) => match x.compute(ctx, opt, txn, None).await? {
						Value::Strand(f) => match path.len() {
							1 => {
								v.remove(f.as_str());
								Ok(())
							}
							_ => match v.get_mut(f.as_str()) {
								Some(v) if v.is_some() => v.del(ctx, opt, txn, path.next()).await,
								_ => Ok(()),
							},
						},
						_ => Ok(()),
					},
					_ => Ok(()),
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					Part::All => match path.len() {
						1 => {
							v.clear();
							Ok(())
						}
						_ => {
							let path = path.next();
							let futs = v.iter_mut().map(|v| v.del(ctx, opt, txn, path));
							try_join_all_buffered(futs).await?;
							Ok(())
						}
					},
					Part::First => match path.len() {
						1 => {
							if !v.is_empty() {
								let i = 0;
								v.remove(i);
							}
							Ok(())
						}
						_ => match v.first_mut() {
							Some(v) => v.del(ctx, opt, txn, path.next()).await,
							None => Ok(()),
						},
					},
					Part::Last => match path.len() {
						1 => {
							if !v.is_empty() {
								let i = v.len() - 1;
								v.remove(i);
							}
							Ok(())
						}
						_ => match v.last_mut() {
							Some(v) => v.del(ctx, opt, txn, path.next()).await,
							None => Ok(()),
						},
					},
					Part::Index(i) => match path.len() {
						1 => {
							if v.len() > i.to_usize() {
								v.remove(i.to_usize());
							}
							Ok(())
						}
						_ => match v.get_mut(i.to_usize()) {
							Some(v) => v.del(ctx, opt, txn, path.next()).await,
							None => Ok(()),
						},
					},
					Part::Where(w) => match path.len() {
						1 => {
							// TODO: If further optimization is desired, push indices to a vec,
							// iterate in reverse, and call swap_remove
							let mut m = HashSet::new();
							for (i, v) in v.iter().enumerate() {
								let cur = v.into();
								if w.compute(ctx, opt, txn, Some(&cur)).await?.is_truthy() {
									m.insert(i);
								};
							}
							v.abolish(|i| m.contains(&i));
							Ok(())
						}
						_ => match path.next().first() {
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
								a.del(ctx, opt, txn, path.next()).await?;
								// Push the new values into the original array
								for (i, p) in p.into_iter().enumerate().rev() {
									match a.pick(&[Part::Index(i.into())]) {
										Value::None => {
											v.remove(i);
										}
										x => v[p] = x,
									}
								}
								Ok(())
							}
							_ => {
								let path = path.next();
								for v in v.iter_mut() {
									let cur = v.into();
									if w.compute(ctx, opt, txn, Some(&cur)).await?.is_truthy() {
										v.del(ctx, opt, txn, path).await?;
									}
								}
								Ok(())
							}
						},
					},
					Part::Value(x) => match x.compute(ctx, opt, txn, None).await? {
						Value::Number(i) => match path.len() {
							1 => {
								if v.len() > i.to_usize() {
									v.remove(i.to_usize());
								}
								Ok(())
							}
							_ => match v.get_mut(i.to_usize()) {
								Some(v) => v.del(ctx, opt, txn, path.next()).await,
								None => Ok(()),
							},
						},
						_ => Ok(()),
					},
					_ => {
						let futs = v.iter_mut().map(|v| v.del(ctx, opt, txn, path));
						try_join_all_buffered(futs).await?;
						Ok(())
					}
				},
				// Ignore everything else
				_ => Ok(()),
			},
			// We are done
			None => Ok(()),
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
	async fn del_none() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::default();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_reset() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_basic() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_wrong() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_other() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 789] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields_flat() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something.age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_field() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35].age");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_fields() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 35]");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'A', age: 34 }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_fields_array_index() {
		let (ctx, opt, txn) = mock().await;
		let idi = Idiom::parse("test.something[WHERE age > 30][0]");
		let mut val = Value::parse(
			"{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }",
		);
		let res = Value::parse("{ test: { something: [{ name: 'B', age: 36 }] } }");
		val.del(&ctx, &opt, &txn, &idi).await.unwrap();
		assert_eq!(res, val);
	}
}
