use anyhow::Result;
use futures::future::try_join_all;
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
//use crate::expr::edges::Edges;
use crate::expr::field::{Field, Fields};
use crate::expr::part::Next;
use crate::expr::statements::select::SelectStatement;
use crate::expr::{Expr, FlowResultExt as _, Idiom, Literal, Lookup, Part};
use crate::val::Value;

impl Value {
	pub(crate) async fn fetch(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		path: &[Part],
	) -> Result<()> {
		let mut this = self;
		let mut iter = path.iter();
		let mut prev = path;

		// Loop over the path.
		// If the we just need to select a sub section of a value we update this to
		// point to the new subsection of the value. Otherwise we call into fetch
		// again and then immediately return.
		// If we encounter a idiom application which does not make sense, like `(1).foo`
		// just return Ok(())
		while let Some(p) = iter.next() {
			match p {
				Part::Lookup(g) => match this {
					Value::Object(o) => {
						let Some(v) = o.rid() else {
							return Ok(());
						};

						let mut v = Value::RecordId(v);
						return stk.run(|stk| v.fetch(stk, ctx, opt, iter.as_slice())).await;
					}
					Value::RecordId(x) => {
						let what = Expr::Idiom(Idiom(vec![
							Part::Start(Expr::Literal(Literal::RecordId(x.clone().into_literal()))),
							Part::Lookup(Lookup {
								what: g.what.clone(),
								kind: g.kind.clone(),
								..Default::default()
							}),
						]));

						let stm = SelectStatement {
							expr: g.expr.clone().unwrap_or(Fields::all()),
							what: vec![what],
							cond: g.cond.clone(),
							limit: g.limit.clone(),
							order: g.order.clone(),
							split: g.split.clone(),
							group: g.group.clone(),
							start: g.start.clone(),
							..SelectStatement::default()
						};
						*this = stm
							.compute(stk, ctx, opt, None)
							.await?
							.all()
							.get(stk, ctx, opt, None, path.next())
							.await
							.catch_return()?
							.flatten();
						return Ok(());
					}
					Value::Array(x) => {
						// apply this path to every entry of the array.
						stk.scope(|scope| {
							let futs =
								x.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, prev)));
							try_join_all(futs)
						})
						.await?;
						return Ok(());
					}
					// break her to be comp
					_ => return Ok(()),
				},
				Part::Field(f) => match this {
					Value::Object(o) => {
						let Some(x) = o.get_mut(f.as_str()) else {
							return Ok(());
						};
						this = x;
					}
					Value::Array(x) => {
						// apply this path to every entry of the array.
						stk.scope(|scope| {
							let futs =
								x.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, prev)));
							try_join_all(futs)
						})
						.await?;
						return Ok(());
					}
					_ => break,
				},
				Part::Value(v) => {
					let v = stk.run(|stk| v.compute(stk, ctx, opt, None)).await.catch_return()?;
					match this {
						Value::Object(obj) => {
							let Some(x) = obj.get_mut(v.coerce_to::<String>()?.as_str()) else {
								return Ok(());
							};
							this = x;
						}
						Value::Array(array) => {
							if let Value::Range(x) = v {
								let Some(range) = x.coerce_to_typed::<i64>()?.slice(array) else {
									return Ok(());
								};
								let mut range = Value::Array(range.to_vec().into());
								return stk
									.run(|stk| range.fetch(stk, ctx, opt, iter.as_slice()))
									.await;
							}
							let idx = v.coerce_to::<i64>()?;
							// TODO: i64 here is truncated, should probably be handled differently.
							let Some(x) = array.get_mut(idx as usize) else {
								return Ok(());
							};
							this = x;
						}
						_ => return Ok(()),
					}
				}
				Part::Destructure(p) => match this {
					Value::Array(x) => {
						// apply this path to every entry of the array.
						stk.scope(|scope| {
							let futs =
								x.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, prev)));
							try_join_all(futs)
						})
						.await?;
					}
					Value::Object(_) => {
						for p in p.iter() {
							let mut destructure_path = p.path();
							destructure_path.extend_from_slice(path);
							stk.run(|stk| this.fetch(stk, ctx, opt, &destructure_path)).await?;
						}
						return Ok(());
					}
					_ => return Ok(()),
				},
				Part::All => match this {
					Value::Object(x) => {
						let next_path = iter.as_slice();
						// no need to spawn all those futures if their is no more paths to
						// calculate
						if next_path.is_empty() {
							break;
						}

						stk.scope(|scope| {
							let futs = x
								.iter_mut()
								.map(|(_, v)| scope.run(|stk| v.fetch(stk, ctx, opt, next_path)));
							try_join_all(futs)
						})
						.await?;
						return Ok(());
					}
					Value::Array(x) => {
						let next_path = iter.as_slice();
						// no need to spawn all those futures if their is no more paths to
						// calculate
						if next_path.is_empty() {
							break;
						}

						stk.scope(|scope| {
							let futs = x
								.iter_mut()
								.map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, next_path)));
							try_join_all(futs)
						})
						.await?;
						return Ok(());
					}
					_ => break,
				},
				Part::First => match this {
					Value::Array(x) => {
						let Some(x) = x.first_mut() else {
							return Ok(());
						};
						this = x;
					}
					_ => return Ok(()),
				},
				Part::Last => match this {
					Value::Array(x) => {
						let Some(x) = x.last_mut() else {
							return Ok(());
						};
						this = x;
					}
					_ => return Ok(()),
				},
				Part::Where(w) => match this {
					Value::Array(x) => {
						for v in x.iter_mut() {
							let doc = v.clone().into();
							if stk
								.run(|stk| w.compute(stk, ctx, opt, Some(&doc)))
								.await
								.catch_return()?
								.is_truthy()
							{
								stk.run(|stk| v.fetch(stk, ctx, opt, iter.as_slice())).await?;
							}
						}
					}
					_ => return Ok(()),
				},
				_ => break,
			}
			prev = iter.as_slice();
		}

		// If the final value is on of following types we still need to compute it.
		match this {
			Value::Array(v) => {
				stk.scope(|scope| {
					let futs = v.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, path)));
					try_join_all(futs)
				})
				.await?;
				Ok(())
			}
			Value::RecordId(v) => {
				// Clone the thing
				let val = v.clone();
				// Fetch the remote embedded record
				let stm = SelectStatement {
					expr: Fields::Select(vec![Field::All]),
					what: vec![Expr::Literal(Literal::RecordId(val.into_literal()))],
					..SelectStatement::default()
				};
				*this = stm.compute(stk, ctx, opt, None).await?.first();
				Ok(())
			}
			_ => Ok(()),
		}
	}
}
