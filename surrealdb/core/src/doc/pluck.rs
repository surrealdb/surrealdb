use std::sync::Arc;

use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::doc::compute::DocKind;
use crate::expr::output::Output;
use crate::expr::{FlowResultExt as _, Idiom, Operation, SelectStatement};
use crate::iam::{Action, AuthLimit};
use crate::idx::planner::RecordStrategy;
use crate::val::Value;

impl Document {
	/// Evaluates a doc that has been modified so that it can be further
	/// computed into a result Value This includes some permissions handling,
	/// output format handling (as specified in statement), field handling
	/// (like params, links etc).
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::pluck_generic", skip_all)
	)]
	pub(super) async fn pluck_generic(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<Value, IgnoreError> {
		// Check if we can view the output
		self.check_output_permissions(stk, ctx, opt, stm).await?;
		// Process the desired output
		let mut out = match stm.output() {
			Some(v) => match v {
				Output::None => Err(IgnoreError::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => {
					// Process the permitted documents
					let (initial, current) = if self.reduced(stk, ctx, opt, Both).await? {
						// Compute the computed fields
						self.computed_fields(stk, ctx, opt, DocKind::InitialReduced).await?;
						self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
						(&mut self.initial_reduced, &mut self.current_reduced)
					} else {
						// Compute the computed fields
						self.computed_fields(stk, ctx, opt, DocKind::Initial).await?;
						self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
						(&mut self.initial, &mut self.current)
					};
					// Output a DIFF of any changes applied to the document
					let ops = initial.doc.as_ref().diff(current.doc.as_ref());
					Ok(Operation::operations_to_value(ops))
				}
				Output::After => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Current).await? {
						self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
						Ok(self.current_reduced.doc.as_ref().to_owned())
					} else {
						self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
						Ok(self.current.doc.as_ref().to_owned())
					}
				}
				Output::Before => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Initial).await? {
						self.computed_fields(stk, ctx, opt, DocKind::InitialReduced).await?;
						Ok(self.initial_reduced.doc.as_ref().to_owned())
					} else {
						self.computed_fields(stk, ctx, opt, DocKind::Initial).await?;
						Ok(self.initial.doc.as_ref().to_owned())
					}
				}
				Output::Fields(v) => {
					// Process the permitted documents
					let (initial, current) = if self.reduced(stk, ctx, opt, Both).await? {
						self.computed_fields(stk, ctx, opt, DocKind::InitialReduced).await?;
						self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
						(&mut self.initial_reduced, &mut self.current_reduced)
					} else {
						self.computed_fields(stk, ctx, opt, DocKind::Initial).await?;
						self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
						(&mut self.initial, &mut self.current)
					};
					// Configure the context
					let mut ctx = Context::new(ctx);
					ctx.add_value("after", current.doc.as_arc());
					ctx.add_value("before", initial.doc.as_arc());
					let ctx = ctx.freeze();
					// Output the specified fields
					v.compute(stk, &ctx, opt, Some(current)).await.map_err(IgnoreError::from)
				}
			},
			None => match stm {
				Statement::Live(_) => Err(IgnoreError::Error(anyhow::anyhow!(
					".lives() uses .lq_pluck(), not .pluck()"
				))),
				Statement::Select {
					stmt,
					..
				} => {
					// FAST PATH: For COUNT operations, skip all field computation and permissions
					// COUNT operations create synthetic documents with only the count value
					if matches!(self.record_strategy, RecordStrategy::Count) {
						Ok(self.current.doc.data.as_ref().clone())
					} else {
						// Process the permitted documents
						let current = if self.reduced(stk, ctx, opt, Current).await? {
							self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
							&self.current_reduced
						} else {
							self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
							&self.current
						};

						if stmt.group.is_some() {
							// Field computation with groups is defered to collection.
							Ok(current.doc.data.as_ref().clone())
						} else {
							// Process the SELECT statement fields
							stmt.fields
								.compute(stk, ctx, opt, Some(current))
								.await
								.map_err(IgnoreError::from)
						}
					}
				}
				Statement::Create(_)
				| Statement::Upsert(_)
				| Statement::Update(_)
				| Statement::Relate(_)
				| Statement::Insert(_) => {
					// Process the permitted documents
					if self.reduced(stk, ctx, opt, Current).await? {
						self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
						Ok(self.current_reduced.doc.as_ref().to_owned())
					} else {
						self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
						Ok(self.current.doc.as_ref().to_owned())
					}
				}
				_ => Err(IgnoreError::Ignore),
			},
		}?;
		// Check if this record exists
		// Skip field permissions for COUNT operations - they only need table-level permissions
		if self.id.is_some() && !matches!(self.record_strategy, RecordStrategy::Count) {
			// Should we run permissions checks?
			if opt.check_perms(Action::View)? {
				let table_fields = self.doc_ctx.fd()?;

				// Loop through all field statements
				for fd in table_fields.iter() {
					// Limit auth
					let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
					// Loop over each field in document
					for k in out.each(&fd.name).iter() {
						// Process the field permissions
						match &fd.select_permission {
							catalog::Permission::Full => (),
							catalog::Permission::None => out.del(stk, ctx, &opt, k).await?,
							catalog::Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.new_with_perms(false);
								// Get the current value
								let val = Arc::new(self.current.doc.as_ref().pick(k));
								// Configure the context
								let mut ctx = Context::new(ctx);
								ctx.add_value("value", val);
								let ctx = ctx.freeze();
								// Process the PERMISSION clause
								if !stk
									.run(|stk| e.compute(stk, &ctx, opt, Some(&self.current)))
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

		// Output result
		Ok(out)
	}

	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::pluck_select", skip_all)
	)]
	pub(super) async fn pluck_select(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stmt: &SelectStatement,
		omit: &[Idiom],
	) -> Result<Value, IgnoreError> {
		// Process the desired output
		let mut out = {
			// FAST PATH: For COUNT operations, skip all field computation and permissions
			// COUNT operations create synthetic documents with only the count value
			if matches!(self.record_strategy, RecordStrategy::Count) {
				Ok(self.current.doc.data.as_ref().clone())
			} else {
				// Process the permitted documents
				let current = if self.reduced(stk, ctx, opt, Current).await? {
					self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
					&self.current_reduced
				} else {
					self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
					&self.current
				};

				if stmt.group.is_some() {
					// Field computation with groups is deferred to collection.
					Ok(current.doc.data.as_ref().clone())
				} else {
					// Process the SELECT statement fields
					stmt.fields
						.compute(stk, ctx, opt, Some(current))
						.await
						.map_err(IgnoreError::from)
				}
			}
		}?;

		// Only check field permissions if we have a record ID (and thus a table context)
		// Skip field permissions for COUNT operations - they only need table-level permissions
		if self.id.is_some() && !matches!(self.record_strategy, RecordStrategy::Count) {
			let table_fields = self.doc_ctx.fd()?;
			// Should we run permissions checks?
			if opt.check_perms(Action::View)? {
				// Loop through all field statements
				for fd in table_fields.iter() {
					// Loop over each field in document
					for k in out.each(&fd.name).iter() {
						// Process the field permissions
						match &fd.select_permission {
							catalog::Permission::Full => (),
							catalog::Permission::None => out.del(stk, ctx, opt, k).await?,
							catalog::Permission::Specific(e) => {
								// Disable permissions
								let opt = &opt.new_with_perms(false);
								// Get the current value
								let val = Arc::new(self.current.doc.as_ref().pick(k));
								// Configure the context
								let mut ctx = Context::new(ctx);
								ctx.add_value("value", val);
								let ctx = ctx.freeze();
								// Process the PERMISSION clause
								if !stk
									.run(|stk| e.compute(stk, &ctx, opt, Some(&self.current)))
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
		for field in omit {
			out.del(stk, ctx, opt, field).await?;
		}
		// Output result
		Ok(out)
	}
}
