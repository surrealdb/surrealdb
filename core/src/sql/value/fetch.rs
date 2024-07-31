use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::statements::select::SelectStatement;
use crate::sql::value::{Value, Values};
use futures::future::try_join_all;
use reblessive::tree::Stk;

impl Value {
	/// Was marked recursive
	pub(crate) async fn fetch(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		path: &[Part],
	) -> Result<(), Error> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Graph(_) => match v.rid() {
						Some(v) => {
							let mut v = Value::Thing(v);
							stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await
						}
						None => Ok(()),
					},
					Part::Field(f) => match v.get_mut(f as &str) {
						Some(v) => stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.get_mut(&i.to_string()) {
						Some(v) => stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await,
						None => Ok(()),
					},
					Part::All => stk.run(|stk| self.fetch(stk, ctx, opt, path.next())).await,
					Part::Destructure(p) => {
						for p in p.iter() {
							let path = [&p.path().as_slice(), path].concat();
							stk.run(|stk| self.fetch(stk, ctx, opt, &path)).await?;
						}

						Ok(())
					}
					_ => Ok(()),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();
						stk.scope(|scope| {
							let futs =
								v.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, path)));
							try_join_all(futs)
						})
						.await?;
						Ok(())
					}
					Part::First => match v.first_mut() {
						Some(v) => stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await,
						None => Ok(()),
					},
					Part::Last => match v.last_mut() {
						Some(v) => stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.get_mut(i.to_usize()) {
						Some(v) => stk.run(|stk| v.fetch(stk, ctx, opt, path.next())).await,
						None => Ok(()),
					},
					Part::Where(w) => {
						let path = path.next();
						for v in v.iter_mut() {
							let cur = v.into();
							if w.compute(stk, ctx, opt, Some(&cur)).await?.is_truthy() {
								stk.run(|stk| v.fetch(stk, ctx, opt, path)).await?;
							}
						}
						Ok(())
					}
					_ => {
						stk.scope(|scope| {
							let futs =
								v.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, path)));
							try_join_all(futs)
						})
						.await?;
						Ok(())
					}
				},
				// Current path part is a thing
				Value::Thing(v) => {
					// Clone the thing
					let val = v.clone();
					// Fetch the remote embedded record
					match p {
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
							*self = stm
								.compute(stk, ctx, opt, None)
								.await?
								.all()
								.get(stk, ctx, opt, None, path.next())
								.await?
								.flatten()
								.ok()?;
							Ok(())
						}
						// This is a remote field expression
						_ => {
							let stm = SelectStatement {
								expr: Fields(vec![Field::All], false),
								what: Values(vec![Value::from(val)]),
								..SelectStatement::default()
							};
							*self = stm.compute(stk, ctx, opt, None).await?.first();
							Ok(())
						}
					}
				}
				// Ignore everything else
				_ => Ok(()),
			},
			// No more parts so get the value
			None => match self {
				// Current path part is an array
				Value::Array(v) => {
					stk.scope(|scope| {
						let futs =
							v.iter_mut().map(|v| scope.run(|stk| v.fetch(stk, ctx, opt, path)));
						try_join_all(futs)
					})
					.await?;
					Ok(())
				}
				// Current path part is a thing
				Value::Thing(v) => {
					// Clone the thing
					let val = v.clone();
					// Fetch the remote embedded record
					let stm = SelectStatement {
						expr: Fields(vec![Field::All], false),
						what: Values(vec![Value::from(val)]),
						..SelectStatement::default()
					};
					*self = stm.compute(stk, ctx, opt, None).await?.first();
					Ok(())
				}
				// Ignore everything else
				_ => Ok(()),
			},
		}
	}
}
