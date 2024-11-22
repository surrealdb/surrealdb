use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::doc::Permitted::*;
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
		// Check if we can view the output
		self.check_permissions_view(stk, ctx, opt, stm).await?;
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(Error::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => {
					// Process the permitted documents
					let (initial, current) = match self.reduced(stk, ctx, opt, Both).await? {
						true => (&self.initial_reduced, &self.current_reduced),
						false => (&self.initial, &self.current),
					};
					// Output a DIFF of any changes applied to the document
					Ok(initial.doc.as_ref().diff(current.doc.as_ref(), Idiom::default()).into())
				}
				Output::After => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// Output the full document before any changes were applied
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
				}
				Output::Before => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Initial).await? {
						// This is an already processed reduced document
						true => Ok(self.initial_reduced.doc.as_ref().to_owned()),
						// Output the full document before any changes were applied
						false => {
							self.initial
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.initial))
								.await
						}
					}
				}
				Output::Fields(v) => {
					// Process the permitted documents
					let (initial, current) = match self.reduced(stk, ctx, opt, Both).await? {
						true => (&mut self.initial_reduced, &mut self.current_reduced),
						false => (&mut self.initial, &mut self.current),
					};
					// Configure the context
					let mut ctx = MutableContext::new(ctx);
					ctx.add_value("after", current.doc.as_arc());
					ctx.add_value("before", initial.doc.as_arc());
					let ctx = ctx.freeze();
					// Output the specified fields
					v.compute(stk, &ctx, opt, Some(current), false).await
				}
			},
			None => match stm {
				Statement::Live(s) => match s.expr.len() {
					0 => {
						// Process the permitted documents
						let (initial, current) = match self.reduced(stk, ctx, opt, Both).await? {
							true => (&self.initial_reduced, &self.current_reduced),
							false => (&self.initial, &self.current),
						};
						// Output a DIFF of any changes applied to the document
						Ok(initial.doc.as_ref().diff(current.doc.as_ref(), Idiom::default()).into())
					}
					_ => {
						// Process the permitted documents
						let current = match self.reduced(stk, ctx, opt, Current).await? {
							true => &self.current_reduced,
							false => &self.current,
						};
						// Process the LIVE SELECT statement fields
						s.expr.compute(stk, ctx, opt, Some(current), false).await
					}
				},
				Statement::Select(s) => {
					// Process the permitted documents
					let current = match self.reduced(stk, ctx, opt, Current).await? {
						true => &self.current_reduced,
						false => &self.current,
					};
					// Process the SELECT statement fields
					s.expr.compute(stk, ctx, opt, Some(current), s.group.is_some()).await
				}
				Statement::Create(_) => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// This is a full document, so process it
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
				}
				Statement::Upsert(_) => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// This is a full document, so process it
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
				}
				Statement::Update(_) => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// This is a full document, so process it
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
				}
				Statement::Relate(_) => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// This is a full document, so process it
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
				}
				Statement::Insert(_) => {
					// Process the permitted documents
					match self.reduced(stk, ctx, opt, Current).await? {
						// This is an already processed reduced document
						true => Ok(self.current_reduced.doc.as_ref().to_owned()),
						// This is a full document, so process it
						false => {
							self.current
								.doc
								.as_ref()
								.compute(stk, ctx, opt, Some(&self.current))
								.await
						}
					}
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
