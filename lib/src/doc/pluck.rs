use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::field::Field;
use crate::sql::idiom::Idiom;
use crate::sql::output::Output;
use crate::sql::part::Part;
use crate::sql::permission::Permission;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn pluck(
		&self,
		ctx: &Context<'_>,
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
							Field::Alone(v) => match v {
								// This expression is a multi-output graph traversal
								Value::Idiom(v) if v.is_multi_yield() => {
									// Store the different output yields here
									let mut res: Vec<(&[Part], Value)> = Vec::new();
									// Split the expression by each output alias
									for v in v.split_inclusive(Idiom::split_multi_yield) {
										// Use the last fetched value for each fetch
										let x = match res.last() {
											Some((_, r)) => r,
											None => self.current.as_ref(),
										};
										// Continue fetching the next idiom part
										let x = x
											.get(ctx, opt, txn, v)
											.await?
											.compute(ctx, opt, txn, Some(&self.current))
											.await?;
										// Add the result to the temporary store
										res.push((v, x));
									}
									// Assign each fetched yield to the output
									for (p, x) in res {
										match p.last().unwrap().alias() {
											// This is an alias expression part
											Some(i) => out.set(ctx, opt, txn, i, x).await?,
											// This is the end of the expression
											None => out.set(ctx, opt, txn, v, x).await?,
										}
									}
								}
								// This expression is a normal field expression
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?
								}
							},
							Field::Alias(v, i) => match v {
								// This expression is a multi-output graph traversal
								Value::Idiom(v) if v.is_multi_yield() => {
									// Store the different output yields here
									let mut res: Vec<(&[Part], Value)> = Vec::new();
									// Split the expression by each output alias
									for v in v.split_inclusive(Idiom::split_multi_yield) {
										// Use the last fetched value for each fetch
										let x = match res.last() {
											Some((_, r)) => r,
											None => self.current.as_ref(),
										};
										// Continue fetching the next idiom part
										let x = x
											.get(ctx, opt, txn, v)
											.await?
											.compute(ctx, opt, txn, Some(&self.current))
											.await?;
										// Add the result to the temporary store
										res.push((v, x));
									}
									// Assign each fetched yield to the output
									for (p, x) in res {
										match p.last().unwrap().alias() {
											// This is an alias expression part
											Some(i) => out.set(ctx, opt, txn, i, x).await?,
											// This is the end of the expression
											None => out.set(ctx, opt, txn, i, x).await?,
										}
									}
								}
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, i, x).await?
								}
							},
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
								// This expression is a grouped aggregate function
								Value::Function(f) if s.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										// If no function arguments, then compute the result
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										// If arguments, then pass the first value through
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
								// This expression is a multi-output graph traversal
								Value::Idiom(v) if v.is_multi_yield() => {
									// Store the different output yields here
									let mut res: Vec<(&[Part], Value)> = Vec::new();
									// Split the expression by each output alias
									for v in v.split_inclusive(Idiom::split_multi_yield) {
										// Use the last fetched value for each fetch
										let x = match res.last() {
											Some((_, r)) => r,
											None => self.current.as_ref(),
										};
										// Continue fetching the next idiom part
										let x = x
											.get(ctx, opt, txn, v)
											.await?
											.compute(ctx, opt, txn, Some(&self.current))
											.await?;
										// Add the result to the temporary store
										res.push((v, x));
									}
									// Assign each fetched yield to the output
									for (p, x) in res {
										match p.last().unwrap().alias() {
											// This is an alias expression part
											Some(i) => out.set(ctx, opt, txn, i, x).await?,
											// This is the end of the expression
											None => out.set(ctx, opt, txn, v, x).await?,
										}
									}
								}
								// This expression is a normal field expression
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
							},
							Field::Alias(v, i) => match v {
								// This expression is a grouped aggregate function
								Value::Function(f) if s.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										// If no function arguments, then compute the result
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										// If arguments, then pass the first value through
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, i, x).await?;
								}
								// This expression is a multi-output graph traversal
								Value::Idiom(v) if v.is_multi_yield() => {
									// Store the different output yields here
									let mut res: Vec<(&[Part], Value)> = Vec::new();
									// Split the expression by each output alias
									for v in v.split_inclusive(Idiom::split_multi_yield) {
										// Use the last fetched value for each fetch
										let x = match res.last() {
											Some((_, r)) => r,
											None => self.current.as_ref(),
										};
										// Continue fetching the next idiom part
										let x = x
											.get(ctx, opt, txn, v)
											.await?
											.compute(ctx, opt, txn, Some(&self.current))
											.await?;
										// Add the result to the temporary store
										res.push((v, x));
									}
									// Assign each fetched yield to the output
									for (p, x) in res {
										match p.last().unwrap().alias() {
											// This is an alias expression part
											Some(i) => out.set(ctx, opt, txn, i, x).await?,
											// This is the end of the expression
											None => out.set(ctx, opt, txn, i, x).await?,
										}
									}
								}
								// This expression is a normal field expression
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
							ctx.add_value("value".into(), &val);
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
