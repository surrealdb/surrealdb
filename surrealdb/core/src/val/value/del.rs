use anyhow::{Result, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::expr::part::{DestructurePart, Next, Part};
use crate::expr::{Expr, FlowResultExt as _, Literal};
use crate::val::Value;

impl Value {
	/// Asynchronous method for deleting a field from a `Value`
	///
	/// Was marked recursive
	///
	/// TODO: Document exact behavior with respect to this.
	pub(crate) async fn del(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		path: &[Part],
	) -> Result<()> {
		match path.first() {
			// Get the current value at path
			Some(p) => match self {
				// Current value at path is an object
				Value::Object(v) => match p {
					Part::All => match path.len() {
						1 => {
							v.clear();
							Ok(())
						}
						_ => {
							let path = path.next();
							for v in v.values_mut() {
								stk.run(|stk| v.del(stk, ctx, opt, path)).await?;
							}
							Ok(())
						}
					},
					Part::Field(f) => match path.len() {
						1 => {
							v.remove(&**f);
							Ok(())
						}
						_ => match v.get_mut(&**f) {
							Some(v) if !v.is_nullish() => {
								stk.run(|stk| v.del(stk, ctx, opt, path.next())).await
							}
							_ => Ok(()),
						},
					},
					Part::Value(x) => {
						match stk.run(|stk| x.compute(stk, ctx, opt, None)).await.catch_return()? {
							Value::Number(n) => match path.len() {
								1 => {
									v.remove(&n.to_sql());
									Ok(())
								}
								_ => match v.get_mut(&n.to_sql()) {
									Some(v) if !v.is_nullish() => {
										stk.run(|stk| v.del(stk, ctx, opt, path.next())).await
									}
									_ => Ok(()),
								},
							},
							Value::String(f) => match path.len() {
								1 => {
									v.remove(f.as_str());
									Ok(())
								}
								_ => match v.get_mut(f.as_str()) {
									Some(v) if !v.is_nullish() => {
										stk.run(|stk| v.del(stk, ctx, opt, path.next())).await
									}
									_ => Ok(()),
								},
							},
							Value::RecordId(t) => match path.len() {
								1 => {
									v.remove(&t.to_sql());
									Ok(())
								}
								_ => match v.get_mut(&t.to_sql()) {
									Some(v) if !v.is_nullish() => {
										stk.run(|stk| v.del(stk, ctx, opt, path.next())).await
									}
									_ => Ok(()),
								},
							},
							_ => Ok(()),
						}
					}
					Part::Destructure(parts) => {
						for part in parts {
							ensure!(
								!matches!(part, DestructurePart::Aliased(_, _)),
								Error::UnsupportedDestructure {
									variant: "An aliased".into(),
								}
							);

							let path = [part.path().as_slice(), path.next()].concat();
							stk.run(|stk| self.del(stk, ctx, opt, &path)).await?;
						}

						Ok(())
					}
					_ => Ok(()),
				},
				// Current value at path is an array
				Value::Array(v) => match p {
					Part::All => {
						if path.len() == 1 {
							v.clear();
							Ok(())
						} else {
							let path = path.next();
							stk.scope(|scope| {
								let futs = v
									.iter_mut()
									.map(|v| scope.run(|stk| v.del(stk, ctx, opt, path)));
								try_join_all_buffered(futs)
							})
							.await?;
							Ok(())
						}
					}
					Part::First => {
						if path.len() == 1 {
							if !v.is_empty() {
								let i = 0;
								v.remove(i);
							}
							Ok(())
						} else {
							match v.first_mut() {
								Some(v) => stk.run(|stk| v.del(stk, ctx, opt, path.next())).await,
								None => Ok(()),
							}
						}
					}
					Part::Last => {
						if path.len() == 1 {
							if !v.is_empty() {
								let i = v.len() - 1;
								v.remove(i);
							}
							Ok(())
						} else {
							match v.last_mut() {
								Some(v) => stk.run(|stk| v.del(stk, ctx, opt, path.next())).await,
								None => Ok(()),
							}
						}
					}
					Part::Where(w) => {
						if path.len() == 1 {
							let mut new_res = Vec::new();
							for v in v.0.iter() {
								let cur = v.clone().into();
								if !stk
									.run(|stk| w.compute(stk, ctx, opt, Some(&cur)))
									.await
									.catch_return()?
									.is_truthy()
								{
									new_res.push(cur.doc.into_owned())
								};
							}
							v.0 = new_res;
							Ok(())
						} else if let Some(Part::Value(_)) = path.get(1) {
							//TODO: Figure out if the behavior here is different with this
							//special case then without. I think this can be simplified.
							let mut true_values = Vec::new();
							let mut true_indecies = Vec::new();
							// Store the elements and positions to update
							for (i, o) in v.iter_mut().enumerate() {
								let cur = o.clone().into();
								if stk
									.run(|stk| w.compute(stk, ctx, opt, Some(&cur)))
									.await
									.catch_return()?
									.is_truthy()
								{
									true_values.push(o.clone());
									true_indecies.push(i);
								}
							}
							// Convert the matched elements array to a value
							let mut a = Value::from(true_values);
							// Set the new value on the matches elements
							stk.run(|stk| a.del(stk, ctx, opt, path.next())).await?;
							// Push the new values into the original array
							for (i, p) in true_indecies.into_iter().enumerate().rev() {
								match a.pick(&[Part::Value(Expr::Literal(Literal::Integer(
									// This technically can overflow but it is very unlikely.
									i as i64,
								)))]) {
									Value::None => {
										v.remove(i);
									}
									x => v[p] = x,
								}
							}
							Ok(())
						} else {
							let path = path.next();
							for v in v.iter_mut() {
								let cur = v.clone().into();
								if stk
									.run(|stk| w.compute(stk, ctx, opt, Some(&cur)))
									.await
									.catch_return()?
									.is_truthy()
								{
									stk.run(|stk| v.del(stk, ctx, opt, path)).await?;
								}
							}
							Ok(())
						}
					}
					Part::Value(x) => {
						if let Value::Number(i) =
							stk.run(|stk| x.compute(stk, ctx, opt, None)).await.catch_return()?
						{
							if path.len() == 1 {
								if v.len() > i.to_usize() {
									v.remove(i.to_usize());
								}
								Ok(())
							} else {
								match v.get_mut(i.to_usize()) {
									Some(v) => {
										stk.run(|stk| v.del(stk, ctx, opt, path.next())).await
									}
									None => Ok(()),
								}
							}
						} else {
							Ok(())
						}
					}
					_ => {
						stk.scope(|scope| {
							let futs =
								v.iter_mut().map(|v| scope.run(|stk| v.del(stk, ctx, opt, path)));
							try_join_all_buffered(futs)
						})
						.await?;
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
	use crate::dbs::test::mock;
	use crate::expr::idiom::Idiom;
	use crate::syn;

	macro_rules! parse_val {
		($input:expr) => {
			crate::val::convert_public_value_to_internal(syn::value($input).unwrap())
		};
	}

	#[tokio::test]
	async fn del_none() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = Default::default();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_reset() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_basic() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_wrong() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.wrong").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_other() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.other.something").unwrap().into();
		let mut val = parse_val!("{ test: { other: null, something: 123 } }");
		let res = parse_val!("{ test: { other: null, something: 123 } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1]").unwrap().into();
		let mut val = parse_val!("{ test: { something: [123, 456, 789] } }");
		let res = parse_val!("{ test: { something: [123, 789] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[1].age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[*].age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_fields_flat() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something.age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A' }, { name: 'B' }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_field() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35].age").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B' }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_fields() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 35]").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'A', age: 34 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_array_where_fields_array_index() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test.something[WHERE age > 30][0]").unwrap().into();
		let mut val =
			parse_val!("{ test: { something: [{ name: 'A', age: 34 }, { name: 'B', age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ name: 'B', age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn del_object_with_thing_based_key() {
		let (ctx, opt) = mock().await;
		let idi: Idiom = syn::idiom("test[city:london]").unwrap().into();
		let mut val =
			parse_val!("{ test: { 'city:london': true, something: [{ age: 34 }, { age: 36 }] } }");
		let res = parse_val!("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let mut stack = reblessive::TreeStack::new();
		stack.enter(|stk| val.del(stk, &ctx, &opt, &idi)).finish().await.unwrap();
		assert_eq!(res, val);
	}
}
