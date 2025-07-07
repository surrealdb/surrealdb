use crate::ctx::{Context, MutableContext};
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::expr::idiom::Idiom;
use crate::expr::output::Output;
use crate::expr::paths::META;
use crate::expr::permission::Permission;
use crate::expr::{FlowResultExt as _, Operation};
use crate::iam::Action;
use crate::val::Value;
use reblessive::tree::Stk;
use std::sync::Arc;

use super::IgnoreError;

impl Document {
	/// Evaluates a doc that has been modified so that it can be further computed into a result Value
	/// This includes some permissions handling, output format handling (as specified in statement),
	/// field handling (like params, links etc).
	pub(super) async fn pluck(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Check if we can view the output
		self.check_permissions_view(stk, ctx, opt, stm).await?;
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(IgnoreError::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => {
					// Process the permitted documents
					let (initial, current) = if self.reduced(stk, ctx, opt, Both).await? {
						(&self.initial_reduced, &self.current_reduced)
					} else {
						(&self.initial, &self.current)
					};
					// Output a DIFF of any changes applied to the document
					let ops = initial.doc.as_ref().diff(current.doc.as_ref(), Idiom::default());
					Ok(Operation::operations_to_value(ops))
				}
				Output::After => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Current).await? {
						Ok(self.current_reduced.doc.as_ref().to_owned())
					} else {
						Ok(self.current.doc.as_ref().to_owned())
					}
				}
				Output::Before => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Initial).await? {
						Ok(self.initial_reduced.doc.as_ref().to_owned())
					} else {
						Ok(self.initial.doc.as_ref().to_owned())
					}
				}
				Output::Fields(v) => {
					// Process the permitted documents
					let (initial, current) = if self.reduced(stk, ctx, opt, Both).await? {
						(&mut self.initial_reduced, &mut self.current_reduced)
					} else {
						(&mut self.initial, &mut self.current)
					};
					// Configure the context
					let mut ctx = MutableContext::new(ctx);
					ctx.add_value("after", current.doc.as_arc());
					ctx.add_value("before", initial.doc.as_arc());
					let ctx = ctx.freeze();
					// Output the specified fields
					v.compute(stk, &ctx, opt, Some(current), false).await.map_err(IgnoreError::from)
				}
			},
			None => match stm {
				Statement::Live(s) => {
					// There was a if here which tested if the live statement had no selectors,
					// which seems like it should never happen so I removed it.
					/*
					if s.expr.is_empty() {
						// Process the permitted documents
						let (initial, current) = if self.reduced(stk, ctx, opt, Both).await? {
							(&self.initial_reduced, &self.current_reduced)
						} else {
							(&self.initial, &self.current)
						};
						// Output a DIFF of any changes applied to the document
						let ops = initial.doc.as_ref().diff(current.doc.as_ref(), Idiom::default());
						Ok(Operation::operations_to_value(ops))
					} else {
					*/
					// Process the permitted documents
					let current = if self.reduced(stk, ctx, opt, Current).await? {
						&self.current_reduced
					} else {
						&self.current
					};
					// Process the LIVE SELECT statement fields
					s.expr
						.compute(stk, ctx, opt, Some(current), false)
						.await
						.map_err(IgnoreError::from)
				}
				Statement::Select(s) => {
					// Process the permitted documents
					let current = if self.reduced(stk, ctx, opt, Current).await? {
						&self.current_reduced
					} else {
						&self.current
					};
					// Process the SELECT statement fields
					s.expr
						.compute(stk, ctx, opt, Some(current), s.group.is_some())
						.await
						.map_err(IgnoreError::from)
				}
				Statement::Create(_)
				| Statement::Upsert(_)
				| Statement::Update(_)
				| Statement::Relate(_)
				| Statement::Insert(_) => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Current).await? {
						Ok(self.current_reduced.doc.as_ref().to_owned())
					} else {
						Ok(self.current.doc.as_ref().to_owned())
					}
				}
				_ => Err(IgnoreError::Ignore),
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
									.await
									.catch_return()?
									.is_truthy()
								{
									out.cut(k);
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
