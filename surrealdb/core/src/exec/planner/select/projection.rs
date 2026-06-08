//! SELECT projection planning.
//!
//! Two paths converge here:
//!
//! - **`plan_projections_fast`**: the default. Pre-classifies every SELECT field via
//!   [`classify_select_field`], detects shadowing against [`ExpressionRegistry`] entries without
//!   mutating it, and only then commits to building a `SelectProject` (with an optional upstream
//!   `Compute` operator for non-trivial expressions).
//! - **`plan_projections`** (the slow path): falls back to the full `Project` operator when the
//!   fast path declines (projection function, multi-part alias, multi-part output idiom,
//!   source-field shadowing).
//!
//! [`ProjectedField`] is the type the classification phase returns.
//! Splitting classification from registry mutation is what makes shadowing
//! detection precise: the simulated commit uses an [`ExpressionRegistry`]
//! clone to compute the actual internal names that would be assigned and
//! checks them against simple-source-field reads. A discarded fast-path
//! attempt therefore never leaks Project-point entries into the real
//! registry.

use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_types::ToSql;

use super::super::Planner;
use super::super::util::{derive_field_name, idiom_to_field_name, idiom_to_field_path};
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry};
use crate::exec::operators::{
	Compute, FieldSelection, Project, ProjectValue, Projection, SelectProject,
};
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::field::{Field, Fields};
use crate::expr::{Expr, Idiom};

/// Classification of a single SELECT field for the fast projection path.
///
/// Pre-classifying every field separately from registry mutation lets the
/// planner detect disqualifying conditions (projection functions, multi-part
/// aliases, multi-part output idioms, source-field shadowing by Compute
/// outputs) before committing any expressions to the [`ExpressionRegistry`].
/// If any field rejects the fast path, the SELECT falls back to
/// [`Planner::plan_projections`] with the registry unchanged by what would
/// have been the fast path's `Project`-point entries.
pub(super) enum ProjectedField {
	/// `Field::All` — wildcard, served by `Projection::All`.
	All,
	/// Direct read of a source field by name.
	Include(String),
	/// Read source field `from` and emit it under output name `to`.
	Rename {
		from: String,
		to: String,
	},
	/// Non-trivial expression that needs to be evaluated by `Compute`
	/// before `SelectProject` can read it. The registry registration is
	/// deferred until the commit phase.
	Compute {
		expr_key: String,
		physical: Arc<dyn crate::exec::PhysicalExpr>,
		output_name: String,
	},
	/// This field — and therefore the whole SELECT — must use the full
	/// `Project` operator. The string is a short reason for diagnostics.
	Fallback(&'static str),
}

impl<'ctx> Planner<'ctx> {
	/// Plan projections (SELECT fields or SELECT VALUE).
	///
	/// The slow path. Used when [`plan_projections_fast`] declines, when an
	/// OMIT expression is non-trivial, or when projections include features
	/// (projection functions, nested output paths, multi-part aliases) the
	/// fast path can't model.
	pub(crate) async fn plan_projections(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let omit_fields = if !omit.is_empty() {
					self.plan_omit(omit).await?
				} else {
					vec![]
				};
				let expr = self.physical_expr(selector.expr).await?;
				Ok(Arc::new(ProjectValue::new(input, expr, omit_fields)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					let omit_fields = if !omit.is_empty() {
						self.plan_omit(omit).await?
					} else {
						vec![]
					};
					return Ok(Arc::new(Project::new(input, vec![], omit_fields, true))
						as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				let mut field_selections = Vec::with_capacity(field_list.len());

				for field in field_list {
					if let Field::Single(selector) = field {
						let field_selection = if let Some(alias) = &selector.alias {
							// Build the output path directly from the alias
							// idiom so `AS foo.bar` produces a nested
							// `[foo, bar]` path, while `` AS `foo.bar` ``
							// (a single Part::Field containing a dot) stays
							// a flat key `"foo.bar"`.
							let output_path = idiom_to_field_path(alias);
							let expr = self.physical_expr(selector.expr).await?;
							FieldSelection::with_alias_path(output_path, expr)
						} else {
							let output_name_or_path = match &selector.expr {
								Expr::Idiom(idiom) => Ok(idiom_to_field_path(idiom)),
								_ => Err(derive_field_name(&selector.expr)),
							};
							let expr = self.physical_expr(selector.expr).await?;
							match output_name_or_path {
								Ok(output_path) => {
									FieldSelection::from_field_path(output_path, expr)
								}
								Err(output_name) => FieldSelection::new(output_name.as_str(), expr),
							}
						};

						field_selections.push(field_selection);
					}
				}

				let omit_fields = if !omit.is_empty() {
					self.plan_omit(omit).await?
				} else {
					vec![]
				};

				Ok(Arc::new(Project::new(input, field_selections, omit_fields, has_wildcard))
					as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan projections with the fast path: use `SelectProject` for simple field
	/// selection and `Compute` for complex expressions, avoiding the full
	/// IdiomExpr/PhysicalExpr/async evaluation chain in `Project`.
	///
	/// Falls back to [`plan_projections`] when projection functions, nested
	/// output paths, or shadowing make the fast path inapplicable.
	pub(crate) async fn plan_projections_fast(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
		registry: &mut ExpressionRegistry,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let omit_fields = if !omit.is_empty() {
					self.plan_omit(omit).await?
				} else {
					vec![]
				};
				// If the alias was registered for sort (Compute pre-evaluated
				// it), read the pre-computed field to avoid re-evaluating
				// non-deterministic expressions like rand() or time::now().
				// Skip when the alias is being OMITted — OMIT deletes the
				// pre-computed field before evaluation, so we must fall
				// through to re-evaluate the expression directly.
				// SELECT VALUE aliases are guaranteed single-part by the parser.
				if let Some(ref alias) = selector.alias
					&& alias.len() == 1
					&& let Some(crate::expr::part::Part::Field(name)) = alias.first()
					&& registry.contains_name(name)
					&& !omit_fields.iter().any(|f| {
						f.len() == 1
							&& matches!(f.first(), Some(crate::expr::part::Part::Field(n)) if n == name)
					}) {
					let idiom = Idiom(vec![crate::expr::part::Part::Field(name.clone())]);
					let expr = self.physical_expr(Expr::Idiom(idiom)).await?;
					return Ok(Arc::new(ProjectValue::new(input, expr, omit_fields))
						as Arc<dyn ExecOperator>);
				}
				let expr = self.physical_expr(selector.expr).await?;
				Ok(Arc::new(ProjectValue::new(input, expr, omit_fields)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(ref field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					if Self::has_complex_omit(&omit) {
						return self.plan_projections(fields, omit, input).await;
					}

					let mut projections = vec![Projection::All];
					for expr in &omit {
						if let Expr::Idiom(idiom) = expr {
							projections.push(Projection::Omit(idiom_to_field_name(idiom).into()));
						}
					}
					return Ok(Arc::new(SelectProject::new(
						input,
						projections,
						Arc::new(OperatorMetrics::new()),
					)) as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				// Bail out early if OMIT contains complex expressions (nested
				// paths, function calls, params) — the fast SelectProject path
				// can't handle them, and checking now avoids compiling physical
				// expressions we'd throw away.
				if Self::has_complex_omit(&omit) {
					return self.plan_projections(fields, omit, input).await;
				}

				// Pre-classify every field WITHOUT touching the registry. This
				// keeps the disqualifying checks (projection functions,
				// multi-part aliases, multi-part output idioms, shadowing)
				// separate from registry mutation. If any check rejects the
				// fast path, we hand off to `plan_projections` with the
				// registry untouched by what would have been the fast path's
				// `Project`-point entries.
				let mut classifications = Vec::with_capacity(field_list.len());
				for field in field_list {
					classifications.push(self.classify_select_field(field).await?);
				}

				// Any single field demanding the full Project operator forces
				// fallback for the entire SELECT.
				if let Some(reason) = classifications.iter().find_map(|c| match c {
					ProjectedField::Fallback(r) => Some(*r),
					_ => None,
				}) {
					tracing::debug!(
						reason = %reason,
						"SELECT projection fast path declined; using full Project operator",
					);
					return self.plan_projections(fields, omit, input).await;
				}

				// Source fields read by simple Include/Rename projections.
				// A Compute output that lands on one of these names would
				// overwrite the source value before SelectProject reads it.
				let simple_source_fields: HashSet<String> = classifications
					.iter()
					.filter_map(|c| match c {
						ProjectedField::Include(s) => Some(s.clone()),
						ProjectedField::Rename {
							from,
							..
						} => Some(from.clone()),
						_ => None,
					})
					.collect();

				// Simulate Project-point registrations so shadow detection uses
				// the actual internal names the registry would choose. This
				// matters when aliases collide: the later Compute field falls
				// back to a synthetic name such as `_e0`, which can still shadow
				// a simple source field selected by the same projection.
				let project_shadow = if simple_source_fields.is_empty() {
					false
				} else {
					let mut simulated_registry = registry.clone();
					classifications.iter().any(|c| match c {
						ProjectedField::Compute {
							expr_key,
							physical,
							output_name,
						} => {
							let internal_name = simulated_registry.register_physical(
								expr_key.clone(),
								Arc::clone(physical),
								ComputePoint::Project,
								Some(output_name.clone()),
							);
							simple_source_fields.contains(&internal_name)
						}
						_ => false,
					})
				};

				// Sort-point Compute entries are already registered by the
				// upstream `plan_sort_consolidated`. If any of their internal
				// names matches a simple source field, the Compute operator
				// (built once, used for both Sort and Project Compute points)
				// would clobber the source field before SelectProject reads it.
				let sort_shadow = !simple_source_fields.is_empty()
					&& registry
						.get_expressions_for_point(ComputePoint::Sort)
						.iter()
						.any(|(name, _)| simple_source_fields.contains(name));

				if project_shadow || sort_shadow {
					return self.plan_projections(fields, omit, input).await;
				}

				// All checks passed — commit to the fast path. From here on,
				// registry mutation is allowed.
				let mut projections = Vec::with_capacity(
					classifications.len() + omit.len() + usize::from(has_wildcard),
				);
				if has_wildcard {
					projections.push(Projection::All);
				}
				for classification in classifications {
					match classification {
						ProjectedField::All => {} // already handled via has_wildcard
						ProjectedField::Include(name) => {
							projections.push(Projection::Include(name.into()));
						}
						ProjectedField::Rename {
							from,
							to,
						} => {
							if from == to {
								projections.push(Projection::Include(to.into()));
							} else {
								projections.push(Projection::Rename {
									from: from.into(),
									to: to.into(),
								});
							}
						}
						ProjectedField::Compute {
							expr_key,
							physical,
							output_name,
						} => {
							Self::register_and_push_projection(
								&mut projections,
								registry,
								expr_key,
								physical,
								output_name,
							);
						}
						ProjectedField::Fallback(reason) => {
							// Filtered above; the type system can't see that.
							// `debug_assert!` catches future refactors that let
							// a Fallback slip past the up-front check.
							debug_assert!(
								false,
								"Fallback should have been filtered before commit phase: {reason}",
							);
							return self.plan_projections(fields, omit, input).await;
						}
					}
				}

				// Add OMIT projections (all simple / top-level)
				for expr in &omit {
					if let Expr::Idiom(idiom) = expr {
						projections.push(Projection::Omit(idiom_to_field_name(idiom).into()));
					}
				}

				// Create Compute operator if any complex expressions were registered
				let computed = if registry.has_expressions_for_point(ComputePoint::Project) {
					let compute_fields = registry
						.get_expressions_for_point(ComputePoint::Project)
						.into_iter()
						.map(|(name, expr)| (crate::val::Strand::new(name), expr))
						.collect();
					Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
				} else {
					input
				};

				Ok(Arc::new(SelectProject::new(
					computed,
					projections,
					Arc::new(OperatorMetrics::new()),
				)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Classify a single SELECT field for the fast projection path without
	/// touching the [`ExpressionRegistry`].
	///
	/// The returned [`ProjectedField`] tells the caller whether the field can
	/// be served by `SelectProject` (with optional `Compute` pre-evaluation),
	/// or whether the entire SELECT must fall back to the full `Project`
	/// operator. Registry mutation is deferred to a commit phase that runs
	/// only after every classification has succeeded and shadowing has been
	/// ruled out, so a discarded fast-path attempt leaves no leftover
	/// `Project`-point entries in the registry.
	async fn classify_select_field(&self, field: &Field) -> Result<ProjectedField, Error> {
		let selector = match field {
			Field::All => return Ok(ProjectedField::All),
			Field::Single(s) => s,
		};

		let physical = self.physical_expr(selector.expr.clone()).await?;

		// Projection functions produce dynamic field bindings and require
		// the full Project operator.
		if physical.is_projection_function() {
			return Ok(ProjectedField::Fallback("projection function"));
		}

		if let Some(alias) = &selector.alias {
			// Multi-part aliases (e.g. `AS status.events`) require nested
			// object construction. Single-part aliases whose identifier
			// contains a dot (e.g. `` `foo.bar` ``) stay on the fast path —
			// SelectProject renames them into a flat key.
			if alias.0.len() > 1 {
				return Ok(ProjectedField::Fallback("multi-part alias"));
			}

			let output_name = idiom_to_field_name(alias);
			if let Some(field_name) = physical.try_simple_field() {
				return Ok(ProjectedField::Rename {
					from: field_name.to_string(),
					to: output_name,
				});
			}
			Ok(ProjectedField::Compute {
				expr_key: selector.expr.to_sql(),
				physical,
				output_name,
			})
		} else {
			if let Some(field_name) = physical.try_simple_field() {
				return Ok(ProjectedField::Include(field_name.to_string()));
			}
			if let Expr::Idiom(idiom) = &selector.expr {
				let path = idiom_to_field_path(idiom);
				if path.len() > 1 {
					return Ok(ProjectedField::Fallback("multi-part output idiom"));
				}
				return Ok(ProjectedField::Compute {
					expr_key: selector.expr.to_sql(),
					physical,
					output_name: idiom_to_field_name(idiom),
				});
			}
			Ok(ProjectedField::Compute {
				expr_key: selector.expr.to_sql(),
				physical,
				output_name: derive_field_name(&selector.expr),
			})
		}
	}

	/// Register a complex expression in the `ExpressionRegistry` and push the
	/// corresponding `Include` or `Rename` projection.
	///
	/// Deduplicates the identical pattern that appeared three times in
	/// [`plan_projections_fast`] (aliased expr, unaliased idiom, unaliased
	/// non-idiom).
	fn register_and_push_projection(
		projections: &mut Vec<Projection>,
		registry: &mut ExpressionRegistry,
		expr_key: String,
		physical: Arc<dyn crate::exec::PhysicalExpr>,
		output_name: String,
	) {
		let internal_name = registry.register_physical(
			expr_key,
			physical,
			ComputePoint::Project,
			Some(output_name.clone()),
		);
		if internal_name == output_name {
			projections.push(Projection::Include(output_name.into()));
		} else {
			projections.push(Projection::Rename {
				from: internal_name.into(),
				to: output_name.into(),
			});
		}
	}

	/// Check whether any OMIT expression requires the full `Project` operator.
	///
	/// `SelectProject` only handles flat `Projection::Omit` with simple idioms.
	/// Nested paths like `opts.age`, function calls like `type::field(...)`, and
	/// parameters all require the full `Project` operator via [`plan_omit`].
	fn has_complex_omit(omit: &[Expr]) -> bool {
		omit.iter().any(|e| {
			if let Expr::Idiom(idiom) = e {
				idiom.len() > 1
			} else {
				true
			}
		})
	}

	/// Plan OMIT fields — convert expressions to idioms.
	pub(crate) async fn plan_omit(
		&self,
		omit: Vec<Expr>,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		let mut fields = Vec::with_capacity(omit.len());
		for expr in omit {
			let mut idioms = self.resolve_field_idioms(expr).await?;
			fields.append(&mut idioms);
		}
		Ok(fields)
	}
}
