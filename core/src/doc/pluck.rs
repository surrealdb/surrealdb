use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
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
		let opt = &opt.new_with_futures(true);
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(Error::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => {
					// Output a DIFF of any changes applied to the document
					Ok(self.initial.doc.diff(self.current.doc.as_ref(), Idiom::default()).into())
				}
				Output::After => {
					// Output the full document after all changes were applied
					self.current.doc.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Output::Before => {
					// Output the full document before any changes were applied
					self.initial.doc.compute(ctx, opt, txn, Some(&self.initial)).await
				}
				Output::Fields(v) => {
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("after", self.current.doc.as_ref());
					ctx.add_value("before", self.initial.doc.as_ref());
					// Output the specified fields
					v.compute(&ctx, opt, txn, Some(&self.current), false).await
				}
			},
			None => match stm {
				Statement::Live(s) => match s.expr.len() {
					0 => Ok(self.initial.doc.diff(&self.current.doc, Idiom::default()).into()),
					_ => s.expr.compute(ctx, opt, txn, Some(&self.current), false).await,
				},
				Statement::Select(s) => {
					s.expr.compute(ctx, opt, txn, Some(&self.current), s.group.is_some()).await
				}
				Statement::Create(_) => {
					self.current.doc.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Update(_) => {
					self.current.doc.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Relate(_) => {
					self.current.doc.compute(ctx, opt, txn, Some(&self.current)).await
				}
				Statement::Insert(_) => {
					self.current.doc.compute(ctx, opt, txn, Some(&self.current)).await
				}
				_ => Err(Error::Ignore),
			},
		}?;
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(Action::View) {
				// Loop through all field statements
				for fd in self.fd(opt, txn).await?.iter() {
					// Loop over each field in document
					for k in out.each(&fd.name).iter() {
						// Process the field permissions
						match &fd.permissions.select {
							Permission::Full => (),
							Permission::None => out.del(ctx, opt, txn, k).await?,
							Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.new_with_perms(false);
								// Get the current value
								let val = self.current.doc.pick(k);
								// Configure the context
								let mut ctx = Context::new(ctx);
								ctx.add_value("value", &val);
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
		// Remove any omitted fields from output
		if let Some(v) = stm.omit() {
			for v in v.iter() {
				out.del(ctx, opt, txn, v).await?;
			}
		}
		// Remove metadata fields on output
		out.cut(&*META);
		// Output result
		Ok(out)
	}
}
