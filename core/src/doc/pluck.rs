use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::iam::Action;
use crate::sql::idiom::Idiom;
use crate::sql::output::Output;
use crate::sql::paths::META;
use crate::sql::permission::Permission;
use crate::sql::value::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

impl Document {
	/// Evaluates a doc that has been modified so that it can be further computed into a result Value
	/// This includes some permissions handling, output format handling (as specified in statement),
	/// field handling (like params, links etc).
	pub async fn pluck(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(Action::View)? {
				// Get the table for this document
				let table = self.tb(ctx, opt).await?;
				// Get the permissions for this table
				let perms = &table.permissions.select;
				// Process the table permissions
				match perms {
					Permission::None => return Err(Error::Ignore),
					Permission::Full => (),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Process the PERMISSION clause
						if !e
							.compute(
								stk,
								ctx,
								opt,
								Some(match stm.is_delete() {
									true => &self.initial,
									false => &self.current,
								}),
							)
							.await?
							.is_truthy()
						{
							return Err(Error::Ignore);
						}
					}
				}
			}
		}
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(Error::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the initial permitted
					self.process_permitted_initial(stk, ctx, opt).await?;
					// Output a DIFF of any changes applied to the document
					Ok(self
						.initial_permitted
						.doc
						.as_ref()
						.diff(self.current_permitted.doc.as_ref(), Idiom::default())
						.into())
				}
				Output::After => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Output the full document after all changes were applied
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				Output::Before => {
					// Process the initial permitted
					self.process_permitted_initial(stk, ctx, opt).await?;
					// Output the full document before any changes were applied
					self.initial_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.initial_permitted))
						.await
				}
				Output::Fields(v) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the initial permitted
					self.process_permitted_initial(stk, ctx, opt).await?;
					// Configure the context
					let mut ctx = MutableContext::new(ctx);
					ctx.add_value("after", self.current_permitted.doc.as_arc());
					ctx.add_value("before", self.initial_permitted.doc.as_arc());
					let ctx = ctx.freeze();
					// Output the specified fields
					v.compute(stk, &ctx, opt, Some(&self.current_permitted), false).await
				}
			},
			None => match stm {
				Statement::Live(s) => match s.expr.len() {
					0 => Ok(self
						.initial
						.doc
						.as_ref()
						.diff(self.current.doc.as_ref(), Idiom::default())
						.into()),
					_ => s.expr.compute(stk, ctx, opt, Some(&self.current), false).await,
				},
				Statement::Select(s) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					s.expr
						.compute(stk, ctx, opt, Some(&self.current_permitted), s.group.is_some())
						.await
				}
				Statement::Create(_) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the document output
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				Statement::Upsert(_) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the document output
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				Statement::Update(_) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the document output
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				Statement::Relate(_) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the document output
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				Statement::Insert(_) => {
					// Process the current permitted
					self.process_permitted_current(stk, ctx, opt).await?;
					// Process the document output
					self.current_permitted
						.doc
						.as_ref()
						.compute(stk, ctx, opt, Some(&self.current_permitted))
						.await
				}
				_ => Err(Error::Ignore),
			},
		}?;
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(Action::View)? {
				// Loop through all field statements
				for fd in self.fd(ctx, opt).await?.iter() {
					// Loop over each field in document
					for k in out.each(&fd.name).iter() {
						// Process the field permissions
						match &fd.permissions.select {
							Permission::Full => (),
							Permission::None => out.del(stk, ctx, opt, k).await?,
							Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.new_with_perms(false);
								// Get the current value
								let val = Arc::new(self.current.doc.as_ref().pick(k));
								// Configure the context
								let mut ctx = MutableContext::new(ctx);
								ctx.add_value("value", val);
								let ctx = ctx.freeze();
								// Process the PERMISSION clause
								if !e
									.compute(stk, &ctx, opt, Some(&self.current))
									.await?
									.is_truthy()
								{
									out.del(stk, &ctx, opt, k).await?
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
				out.del(stk, ctx, opt, v).await?;
			}
		}
		// Remove metadata fields on output
		out.cut(&*META);
		// Output result
		Ok(out)
	}
}
