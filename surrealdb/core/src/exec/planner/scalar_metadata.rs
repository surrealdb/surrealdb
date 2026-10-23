//! Scalar requirements read from the logical expression.

use super::Planner;
use crate::err::Error;
use crate::exec::BoxFut;
use crate::exec::clause::ScalarMetadata;
use crate::expr::Function;
use crate::expr::logical::{LogicalExpr, LogicalOp, LogicalRecursePath};
use crate::val::Value;

impl Planner<'_> {
	/// The description of the scalar `expr`, with its folded constant; see [`ScalarMetadata`].
	pub(crate) async fn scalar_metadata(&self, expr: LogicalExpr) -> Result<ScalarMetadata, Error> {
		crate::exec::compiles::note();
		self.check_depth()?;
		self.validate_scalar(&expr).await?;
		let literal = super::static_value(&expr).or_else(|| self.constant_path_value(&expr));
		Ok(ScalarMetadata::new(expr, literal))
	}

	/// Validate the shapes whose compilation raises before clause evaluation.
	fn validate_scalar<'a>(&'a self, expr: &'a LogicalExpr) -> BoxFut<'a, Result<(), Error>> {
		Box::pin(async move {
			let mut pending = vec![expr];
			while let Some(expr) = pending.pop() {
				if super::static_value(expr).is_some() || self.constant_path_value(expr).is_some() {
					continue;
				}
				match expr {
					LogicalExpr::Prefix {
						expr,
						..
					} => {
						let mut depth = self.depth;
						let mut node = expr.as_ref();
						while let LogicalExpr::Prefix {
							expr,
							..
						} = node
						{
							depth += 1;
							if depth > self.ctx.config.exec.max_computation_depth {
								return Err(crate::exec::Error::ComputationDepthExceeded.into());
							}
							node = expr;
						}
					}
					LogicalExpr::Recurse(recurse) => {
						self.recurse_bounds(&recurse.recurse)?;
						if recurse.instruction.is_some()
							&& let Some(LogicalRecursePath::Trailing(body)) = &recurse.path
							&& super::contains_repeat_recurse(body)
						{
							return Err(crate::exec::Error::RecursionInstructionPlanConflict.into());
						}
					}
					LogicalExpr::Closure(closure) => {
						let args = closure.args.iter().map(|(param, _)| param.as_str().to_owned());
						let planner = if self.txn.is_some() && self.auth.is_some() {
							self.own_scope().shadowing(args)
						} else {
							Planner::new(&self.ctx, self.function_registry)
								.with_depth(self.depth)
								.with_cycle_guard(self.cycle_guard.clone())
								.shadowing(args)
						};
						planner.validate_scalar(&closure.body).await?;
						continue;
					}
					LogicalExpr::Subquery(op) => {
						if matches!(op.as_ref(), LogicalOp::Let(_)) {
							return Err(crate::exec::Error::InvalidStatement(
							"LET statements can only appear at the top level of a query or inside a block expression".to_owned(),
						).into());
						}
						// A `LET` no statement may hold is refused from the text
						// written, before binding folds an arm that never runs.
						// See `exec::logical::invalid_let`.
						if let Some(error) = crate::exec::logical::invalid_let(op) {
							return Err(error);
						}
						if self.txn.is_some() {
							if let Some(reason) = crate::exec::logical::unplannable_write(op) {
								return Err(crate::exec::Error::PlannerUnsupported {
									reason,
								}
								.into());
							}
							let unbound = matches!(
								op.as_ref(),
								LogicalOp::Sequence(_)
									| LogicalOp::Foreach(_) | LogicalOp::Let(_)
									| LogicalOp::Eval(_)
							)
							.then(|| op.as_ref().clone());
							let planner = if self.within.nested_late {
								self.late_binding()
							} else {
								let within = crate::exec::vm::catalog_within(
									self,
									op,
									&self.shadowed_params,
								)
								.await;
								self.shadowing(std::iter::empty()).reaching(within)
							};
							let bound = planner.bind(op.as_ref().clone()).await?;
							crate::exec::vm::plan_checked(&planner, &bound, unbound.as_ref())
								.await?;
						}
						continue;
					}
					LogicalExpr::Call(call) => match &call.receiver {
						Function::Normal(name)
							if self.function_registry().is_index_function(name) =>
						{
							self.index_function_context(name, &mut call.args.clone())?;
						}
						Function::Module(module, sub, _) => {
							self.resolve_module_writeable(module, sub.as_deref()).await?;
						}
						Function::Silo {
							org,
							pkg,
							major,
							minor,
							patch,
							sub,
						} => {
							self.resolve_silo_writeable(
								org,
								pkg,
								*major,
								*minor,
								*patch,
								sub.as_deref(),
							)
							.await?;
						}
						Function::Normal(_)
						| Function::Custom(_)
						| Function::Script(_)
						| Function::Model(_) => {}
					},
					LogicalExpr::Literal(_)
					| LogicalExpr::Array(_)
					| LogicalExpr::Set(_)
					| LogicalExpr::Object(_)
					| LogicalExpr::RecordId(_)
					| LogicalExpr::Param(_)
					| LogicalExpr::Table(_)
					| LogicalExpr::Mock(_)
					| LogicalExpr::Constant(_)
					| LogicalExpr::Field(_)
					| LogicalExpr::Doc
					| LogicalExpr::Current
					| LogicalExpr::Access {
						..
					}
					| LogicalExpr::Index {
						..
					}
					| LogicalExpr::First(_)
					| LogicalExpr::Last(_)
					| LogicalExpr::Each(_)
					| LogicalExpr::Flatten {
						..
					}
					| LogicalExpr::Filter {
						..
					}
					| LogicalExpr::Method {
						..
					}
					| LogicalExpr::Destructure {
						..
					}
					| LogicalExpr::Optional {
						..
					}
					| LogicalExpr::Lookup {
						..
					}
					| LogicalExpr::RepeatRecurse(_)
					| LogicalExpr::Broadcast {
						..
					}
					| LogicalExpr::SetField {
						..
					}
					| LogicalExpr::Postfix {
						..
					}
					| LogicalExpr::Invoke {
						..
					}
					| LogicalExpr::Binary {
						..
					}
					| LogicalExpr::If(_)
					| LogicalExpr::Throw(_)
					| LogicalExpr::Break
					| LogicalExpr::Continue => {}
				}
				let start = pending.len();
				expr.for_each_child(&mut |child| pending.push(child));
				pending[start..].reverse();
			}
			Ok(())
		})
	}

	fn constant_path_value(&self, expr: &LogicalExpr) -> Option<Value> {
		let (root, fields) = super::path::constant_access_chain(expr)?;
		let root = self.constant_root_value(root)?;
		super::path::fold_constant_field_chain(&root, &fields)
	}
}
