//! Source and lookup planning for the planner.
//!
//! Handles graph/reference lookups, index functions, range bounds, and order conversion.

use std::sync::Arc;

use super::Planner;
use super::util::extract_table_from_matches;
use crate::err::Error;
use crate::exec::Error as ExecError;
use crate::expr::logical::{LogicalExpr, LogicalLiteral};

// ============================================================================
// impl Planner — Source Planning
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Resolve the index context declared by a function without compiling argument evaluators.
	pub(super) fn index_function_context(
		&self,
		name: &str,
		args: &mut Vec<LogicalExpr>,
	) -> Result<crate::exec::function::IndexContext, Error> {
		use crate::exec::function::{IndexContext, IndexContextKind};
		let registry = self.function_registry();
		let func = registry.get_index_function(name).ok_or_else(|| ExecError::Query {
			message: format!("Index function '{}' not found in registry", name),
		})?;

		// Resolve the appropriate index context based on the function's declared kind
		let index_ctx = match func.index_context_kind() {
			IndexContextKind::FullText => {
				// FullText functions must declare which argument is the index ref
				let ref_idx = func.index_ref_arg_index().ok_or_else(|| ExecError::Query {
					message: format!(
						"Index function '{}': FullText functions must declare an index_ref_arg_index",
						name
					),
				})?;

				if ref_idx >= args.len() {
					return Err(ExecError::Query {
						message: format!(
							"Index function '{}' requires at least {} arguments",
							name,
							ref_idx + 1
						),
					}
					.into());
				}

				// Extract the match_ref argument at plan time (not passed at runtime)
				let arg = args.remove(ref_idx);
				let match_ref = if let LogicalExpr::Literal(lit) = &arg {
					if let LogicalLiteral::Integer(n) = lit {
						(0..=255).contains(n).then_some(*n as u8)
					} else if let LogicalLiteral::Float(n) = lit {
						((0.0..=255.0).contains(n) && n.fract() == 0.0).then_some(*n as u8)
					} else {
						None
					}
				} else {
					None
				};
				let Some(match_ref) = match_ref else {
					return Err(ExecError::Query {
						message: format!(
							"Index function '{name}': index_ref argument must be a literal integer in range 0..255"
						),
					}
					.into());
				};

				// Resolve the MatchContext from the MATCHES context
				let matches_ctx =
					self.matches_context.as_ref().ok_or_else(|| ExecError::Query {
						message: format!(
							"Index function '{}': no MATCHES clause found in WHERE condition",
							name
						),
					})?;

				let match_ctx = matches_ctx
					.resolve(match_ref, extract_table_from_matches(matches_ctx))
					.map_err(|e| ExecError::Query {
						message: format!("Index function '{}': {}", name, e),
					})?;

				IndexContext::FullText(match_ctx)
			}
			IndexContextKind::Knn => {
				// KNN functions read the KNN context from the planning context.
				// A declared ref argument is removed from the argument list and not used.
				if let Some(ref_idx) = func.index_ref_arg_index()
					&& ref_idx < args.len()
				{
					args.remove(ref_idx);
				}

				let knn_ctx = self.knn_context.as_ref().ok_or_else(|| ExecError::Query {
					message: format!(
						"Index function '{}': no KNN operator found in WHERE condition",
						name
					),
				})?;
				IndexContext::Knn(Arc::clone(knn_ctx))
			}
		};

		Ok(index_ctx)
	}
}
