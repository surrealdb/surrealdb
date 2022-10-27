use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use crate::sql::output::Output;
use crate::sql::paths::META;
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
				Output::Fields(v) => v.compute(ctx, opt, txn, Some(&self.current), false).await,
			},
			None => match stm {
				Statement::Select(s) => {
					s.expr.compute(ctx, opt, txn, Some(&self.current), s.group.is_some()).await
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
					// Check for a PERMISSIONS clause
					if opt.perms && opt.auth.perms() {
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
								if !e
									.compute(&ctx, opt, txn, Some(&self.current))
									.await?
									.is_truthy()
								{
									out.del(&ctx, opt, txn, k).await?
								}
							}
						}
					}
				}
			}
		}
		// Remove metadata fields on output
		out.del(ctx, opt, txn, &*META).await?;
		// Output result
		Ok(out)
	}
}
