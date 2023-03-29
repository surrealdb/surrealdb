use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::edges::Edges;
use crate::sql::field::{Field, Fields};
use crate::sql::part::Next;
use crate::sql::part::Part;
use crate::sql::statements::select::SelectStatement;
use crate::sql::value::{Value, Values};
use async_recursion::async_recursion;
use futures::future::try_join_all;

impl Value {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub async fn fetch(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		path: &[Part],
	) -> Result<(), Error> {
		match path.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Graph(_) => match v.rid() {
						Some(v) => Value::Thing(v).fetch(ctx, opt, txn, path.next()).await,
						None => Ok(()),
					},
					Part::Field(f) => match v.get_mut(f as &str) {
						Some(v) => v.fetch(ctx, opt, txn, path.next()).await,
						None => Ok(()),
					},
					Part::All => self.fetch(ctx, opt, txn, path.next()).await,
					_ => Ok(()),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => {
						let path = path.next();
						let futs = v.iter_mut().map(|v| v.fetch(ctx, opt, txn, path));
						try_join_all(futs).await?;
						Ok(())
					}
					Part::First => match v.first_mut() {
						Some(v) => v.fetch(ctx, opt, txn, path.next()).await,
						None => Ok(()),
					},
					Part::Last => match v.last_mut() {
						Some(v) => v.fetch(ctx, opt, txn, path.next()).await,
						None => Ok(()),
					},
					Part::Index(i) => match v.get_mut(i.to_usize()) {
						Some(v) => v.fetch(ctx, opt, txn, path.next()).await,
						None => Ok(()),
					},
					Part::Where(w) => {
						let path = path.next();
						for v in v.iter_mut() {
							if w.compute(ctx, opt, txn, Some(v)).await?.is_truthy() {
								v.fetch(ctx, opt, txn, path).await?;
							}
						}
						Ok(())
					}
					_ => {
						let futs = v.iter_mut().map(|v| v.fetch(ctx, opt, txn, path));
						try_join_all(futs).await?;
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
								.compute(ctx, opt, txn, None)
								.await?
								.all()
								.get(ctx, opt, txn, path.next())
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
							*self = stm.compute(ctx, opt, txn, None).await?.first();
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
					let futs = v.iter_mut().map(|v| v.fetch(ctx, opt, txn, path));
					try_join_all(futs).await?;
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
					*self = stm.compute(ctx, opt, txn, None).await?.first();
					Ok(())
				}
				// Ignore everything else
				_ => Ok(()),
			},
		}
	}
}
