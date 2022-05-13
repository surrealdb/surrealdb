use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::field::Field;
use crate::sql::idiom::Idiom;
use crate::sql::output::Output;
use crate::sql::permission::Permission;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn pluck(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.futures(true);
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(Error::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => Ok(self.initial.diff(&self.current, Idiom::default()).into()),
				Output::After => self.current.compute(ctx, opt, txn, Some(&self.current)).await,
				Output::Before => self.initial.compute(ctx, opt, txn, Some(&self.initial)).await,
				Output::Fields(v) => {
					let mut out = match v.all() {
						true => self.current.compute(ctx, opt, txn, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in v.other() {
						match v {
							Field::All => (),
							Field::Alone(v) => {
								let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
								out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
							}
							Field::Alias(v, i) => {
								let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
								out.set(ctx, opt, txn, i, x).await?;
							}
						}
					}
					Ok(out)
				}
			},
			None => match stm {
				Statement::Select(s) => {
					let mut out = match s.expr.all() {
						true => self.current.compute(ctx, opt, txn, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in s.expr.other() {
						match v {
							Field::All => (),
							Field::Alone(v) => match v {
								Value::Function(f) if s.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
							},
							Field::Alias(v, i) => match v {
								Value::Function(f) if s.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, i, x).await?;
								}
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, i, x).await?;
								}
							},
						}
					}
					Ok(out)
				}
				Statement::Create(_) => {
					self.current.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Update(_) => {
					self.current.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Relate(_) => {
					self.current.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Insert(_) => {
					self.current.compute(ctx, opt, txn, Some(&self.current)).await
				}
				_ => Err(Error::Ignore),
			},
		}?;
		// Check if this record exists
		if self.id.is_some() {
			// Loop through all field statements
			for fd in self.fd(opt, txn).await?.iter() {
				// Loop over each field in document
				for k in out.each(&fd.name).iter() {
					// Process field permissions
					match &fd.permissions.select {
						Permission::Full => (),
						Permission::None => out.del(ctx, opt, txn, k).await?,
						Permission::Specific(e) => {
							// Get the current value
							let val = self.current.pick(k);
							// Configure the context
							let mut ctx = Context::new(ctx);
							ctx.add_value("value".into(), val);
							let ctx = ctx.freeze();
							// Process the PERMISSION clause
							if !e.compute(&ctx, opt, txn, Some(&self.current)).await?.is_truthy() {
								out.del(&ctx, opt, txn, k).await?
							}
						}
					}
				}
			}
		}
		// Output result
		Ok(out)
	}
}
