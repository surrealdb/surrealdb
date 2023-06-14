use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
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
				Output::After => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					self.current.compute(&ctx, opt).await
				}
				Output::Before => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.initial);
					self.initial.compute(&ctx, opt).await
				}
				Output::Fields(v) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					v.compute(&ctx, opt, false).await
				}
			},
			None => match stm {
				Statement::Live(s) => match s.expr.len() {
					0 => Ok(self.initial.diff(&self.current, Idiom::default()).into()),
					_ => {
						let mut ctx = Context::new(ctx);
						ctx.add_cursor_doc(&self.current);
						s.expr.compute(&ctx, opt, false).await
					}
				},
				Statement::Select(s) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					s.expr.compute(&ctx, opt, s.group.is_some()).await
				}
				Statement::Create(_) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					self.current.compute(&ctx, opt).await
				}
				Statement::Update(_) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					self.current.compute(&ctx, opt).await
				}
				Statement::Relate(_) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					self.current.compute(&ctx, opt).await
				}
				Statement::Insert(_) => {
					let mut ctx = Context::new(ctx);
					ctx.add_cursor_doc(&self.current);
					self.current.compute(&ctx, opt).await
				}
				_ => Err(Error::Ignore),
			},
		}?;
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.perms && opt.auth.perms() {
				// Clone transaction
				let txn = ctx.clone_transaction()?;
				// Loop through all field statements
				for fd in self.fd(opt, &txn).await?.iter() {
					// Loop over each field in document
					for k in out.each(&fd.name).iter() {
						// Process the field permissions
						match &fd.permissions.select {
							Permission::Full => (),
							Permission::None => out.del(ctx, opt, k).await?,
							Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.perms(false);
								// Get the current value
								let val = self.current.pick(k);
								// Configure the context
								let mut ctx = Context::new(ctx);
								ctx.add_value("value", &val);
								ctx.add_cursor_doc(&self.current);
								// Process the PERMISSION clause
								if !e.compute(&ctx, opt).await?.is_truthy() {
									out.del(&ctx, opt, k).await?
								}
							}
						}
					}
				}
			}
		}
		// Remove metadata fields on output
		out.del(ctx, opt, &*META).await?;
		// Output result
		Ok(out)
	}
}
