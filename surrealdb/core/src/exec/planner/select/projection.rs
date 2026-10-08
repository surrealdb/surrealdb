//! SELECT projection planning.
//!
//! Two paths converge here:
//!
//! - **`plan_projections_fast`**: the default. Pre-classifies every SELECT field via
//!   [`classify_select_field`], detects shadowing against [`ExpressionRegistry`] entries without
//!   mutating it, and only then commits to building a `SelectProject` (with an optional upstream
//!   `Compute` operator for non-trivial expressions).
//! - **`plan_projections`** (the slow path): falls back to the full `Project` operator when the
//!   fast path declines (projection function, multi-part alias, multi-part output path,
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
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry};
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::logical::{
	BatchRetry, ColumnEffects, columns_may_run_column_major, is_effect_free,
	may_signal_control_flow,
};
use crate::exec::operators::{
	Compute, FieldSelection, OmitAction, OmitPath, OmitStep, Project, ProjectValue, Projection,
	SelectProject,
};
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::logical::{LogicalExpr, LogicalField, LogicalFields, OutputPath, OutputPathPart};

/// Classification of a single SELECT field for the fast projection path.
///
/// Pre-classifying every field separately from registry mutation lets the
/// planner detect disqualifying conditions (projection functions, multi-part
/// aliases, multi-part output paths, source-field shadowing by Compute
/// outputs) before committing any expressions to the [`ExpressionRegistry`].
/// If any field rejects the fast path, the SELECT falls back to
/// [`Planner::plan_projections`] with the registry unchanged by what would
/// have been the fast path's `Project`-point entries.
pub(super) enum ProjectedField {
	/// `LogicalField::All` — wildcard, served by `Projection::All`.
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
		/// What evaluating the expression as written does beyond producing
		/// its value; see [`Compute::column_major`].
		effects: ColumnEffects,
	},
	/// This field — and therefore the whole SELECT — must use the full
	/// `Project` operator. The string is a short reason for diagnostics.
	Fallback(&'static str),
}

/// Where a projected value lands, as the path the `Project` operator writes
/// to: one key per [`OutputPath`] part, a lookup key included.
pub(crate) fn output_field_path(path: &OutputPath) -> FieldPath {
	FieldPath(
		path.parts()
			.iter()
			.map(|part| match part {
				OutputPathPart::Field(name) | OutputPathPart::Lookup(name) => {
					FieldPathPart::Field(name.clone())
				}
			})
			.collect(),
	)
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
		fields: LogicalFields,
		omit: Vec<LogicalExpr>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			LogicalFields::Value {
				expr,
				alias: _,
			} => {
				let omit_fields = if !omit.is_empty() {
					self.plan_omit(omit).await?
				} else {
					vec![]
				};
				// `ProjectValue` holds one expression, so what this answers is
				// whether evaluating it again is free, not whether two columns
				// may be interleaved; see `ProjectValue::repeat_is_free`.
				let repeat_is_free = is_effect_free(&expr, self.function_registry())
					|| !may_signal_control_flow(&expr);
				let expr = self.physical_expr(expr).await?;
				let mut project = ProjectValue::new(input, expr, omit_fields);
				if repeat_is_free {
					project = project.with_repeat_is_free();
				}
				Ok(Arc::new(project) as Arc<dyn ExecOperator>)
			}

			LogicalFields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(LogicalField::All));

				if is_select_all {
					let omit_fields = if !omit.is_empty() {
						self.plan_omit(omit).await?
					} else {
						vec![]
					};
					return Ok(Arc::new(Project::new(input, vec![], omit_fields, true))
						as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, LogicalField::All));

				// Judged from the expressions as written, before they are
				// compiled away; see `Project::column_major`. A projection
				// passes a failed column on as the batch's, so it repeats
				// nothing.
				let column_effects: Vec<ColumnEffects> = field_list
					.iter()
					.map(|field| match field {
						LogicalField::All => ColumnEffects::INERT,
						LogicalField::Single {
							expr,
							..
						} => ColumnEffects::of(expr, self.function_registry()),
					})
					.collect();
				let column_major = columns_may_run_column_major(&column_effects, BatchRetry::None);

				let mut field_selections = Vec::with_capacity(field_list.len());

				for field in field_list {
					if let LogicalField::Single {
						expr,
						alias,
						..
					} = field
					{
						let field_selection = if let Some(alias) = &alias {
							// The alias is already the output path: `AS foo.bar`
							// is the nested `[foo, bar]`, while `` AS `foo.bar` ``
							// (one identifier containing a dot) is the flat key
							// `"foo.bar"`.
							let output_path = output_field_path(alias);
							let expr = self.physical_expr(expr).await?;
							FieldSelection::with_alias_path(output_path, expr)
						} else {
							// A path lands at the position its field reads and
							// lookups spell out; any other expression lands under
							// its derived name as one flat key.
							let output_path = expr.output_path();
							let is_path = expr.is_path();
							let expr = self.physical_expr(expr).await?;
							if is_path {
								FieldSelection::from_field_path(
									output_field_path(&output_path),
									expr,
								)
							} else {
								FieldSelection::new(&output_path.name(), expr)
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

				let mut project = Project::new(input, field_selections, omit_fields, has_wildcard);
				if column_major {
					project = project.with_column_major();
				}
				Ok(Arc::new(project) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan projections with the fast path: use `SelectProject` for simple field
	/// selection and `Compute` for complex expressions, avoiding the full
	/// per-row `PhysicalExpr` evaluation and object construction in `Project`.
	///
	/// Falls back to [`plan_projections`] when projection functions, nested
	/// output paths, or shadowing make the fast path inapplicable.
	pub(crate) async fn plan_projections_fast(
		&self,
		fields: LogicalFields,
		omit: Vec<LogicalExpr>,
		input: Arc<dyn ExecOperator>,
		registry: &mut ExpressionRegistry,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			LogicalFields::Value {
				expr,
				alias,
			} => {
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
				if let Some(name) = alias.as_ref().and_then(OutputPath::as_field)
					&& registry.contains_name(name)
					&& !omit_fields.iter().any(|f| f.single_field() == Some(name))
				{
					// A bare field read of what `Compute` already evaluated.
					let expr = self.physical_expr(LogicalExpr::Field(name.into())).await?;
					return Ok(Arc::new(
						ProjectValue::new(input, expr, omit_fields).with_repeat_is_free(),
					) as Arc<dyn ExecOperator>);
				}
				// `ProjectValue` holds one expression, so what this answers is
				// whether evaluating it again is free, not whether two columns
				// may be interleaved; see `ProjectValue::repeat_is_free`.
				let repeat_is_free = is_effect_free(&expr, self.function_registry())
					|| !may_signal_control_flow(&expr);
				let expr = self.physical_expr(expr).await?;
				let mut project = ProjectValue::new(input, expr, omit_fields);
				if repeat_is_free {
					project = project.with_repeat_is_free();
				}
				Ok(Arc::new(project) as Arc<dyn ExecOperator>)
			}

			LogicalFields::Select(ref field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(LogicalField::All));

				if is_select_all {
					if Self::has_complex_omit(&omit) {
						return self.plan_projections(fields, omit, input).await;
					}

					let mut projections = vec![Projection::All];
					for expr in &omit {
						if let LogicalExpr::Field(name) = expr {
							projections.push(Projection::Omit(name.as_str().into()));
						}
					}
					return Ok(Arc::new(SelectProject::new(
						input,
						projections,
						Arc::new(OperatorMetrics::new()),
					)) as Arc<dyn ExecOperator>);
				}

				let has_wildcard = field_list.iter().any(|f| matches!(f, LogicalField::All));

				// Bail out early if OMIT contains complex expressions (nested
				// paths, function calls, params) — the fast SelectProject path
				// can't handle them, and checking now avoids compiling physical
				// expressions we'd throw away.
				if Self::has_complex_omit(&omit) {
					return self.plan_projections(fields, omit, input).await;
				}

				// Pre-classify every field WITHOUT touching the registry. This
				// keeps the disqualifying checks (projection functions,
				// multi-part aliases, multi-part output paths, shadowing)
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
							effects,
						} => {
							let internal_name = simulated_registry.register_physical(
								expr_key.clone(),
								Arc::clone(physical),
								ComputePoint::Project,
								Some(output_name.clone()),
								*effects,
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
							effects,
						} => {
							Self::register_and_push_projection(
								&mut projections,
								registry,
								expr_key,
								physical,
								output_name,
								effects,
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
					if let LogicalExpr::Field(name) = expr {
						projections.push(Projection::Omit(name.as_str().into()));
					}
				}

				// Create Compute operator if any complex expressions were registered
				let computed = if registry.has_expressions_for_point(ComputePoint::Project) {
					let compute_fields = registry
						.get_expressions_for_point(ComputePoint::Project)
						.into_iter()
						.map(|(name, expr)| (crate::val::Strand::new(name), expr))
						.collect();
					let mut compute = Compute::new(input, compute_fields);
					if registry.point_may_run_column_major(ComputePoint::Project) {
						compute = compute.with_column_major();
					}
					Arc::new(compute) as Arc<dyn ExecOperator>
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
	async fn classify_select_field(&self, field: &LogicalField) -> Result<ProjectedField, Error> {
		let (expr, alias) = match field {
			LogicalField::All => return Ok(ProjectedField::All),
			LogicalField::Single {
				expr,
				alias,
				..
			} => (expr, alias),
		};

		let physical = self.physical_expr(expr.clone()).await?;

		// Projection functions produce dynamic field bindings and require
		// the full Project operator.
		if physical.is_projection_function() {
			return Ok(ProjectedField::Fallback("projection function"));
		}

		if let Some(alias) = alias {
			// Multi-part aliases (e.g. `AS status.events`) require nested
			// object construction, and so does an alias written with a step
			// past its key (`AS arr[0]`, `AS arr[$]`): `SelectProject` can
			// only rename into one bare key. Single-part aliases whose
			// identifier contains a dot (e.g. `` `foo.bar` ``) stay on the
			// fast path — SelectProject renames them into a flat key.
			if alias.parts().len() > 1 || !alias.exact {
				return Ok(ProjectedField::Fallback("multi-part alias"));
			}

			let output_name = alias.name();
			if let Some(field_name) = physical.try_simple_field() {
				return Ok(ProjectedField::Rename {
					from: field_name.to_string(),
					to: output_name,
				});
			}
			Ok(ProjectedField::Compute {
				expr_key: expr.to_sql(),
				physical,
				output_name,
				// Judged from the expression as written; see
				// `Compute::column_major`.
				effects: ColumnEffects::of(expr, self.function_registry()),
			})
		} else {
			if let Some(field_name) = physical.try_simple_field() {
				return Ok(ProjectedField::Include(field_name.to_string()));
			}
			// A path whose output is nested (`a.b`, `->likes.name`) needs the
			// full operator's object construction; any other expression lands
			// under one flat key.
			let output_path = expr.output_path();
			if output_path.parts().len() > 1 {
				return Ok(ProjectedField::Fallback("multi-part output path"));
			}
			Ok(ProjectedField::Compute {
				expr_key: expr.to_sql(),
				physical,
				output_name: output_path.name(),
				// Judged from the expression as written; see
				// `Compute::column_major`.
				effects: ColumnEffects::of(expr, self.function_registry()),
			})
		}
	}

	/// Register a complex expression in the `ExpressionRegistry` and push the
	/// corresponding `Include` or `Rename` projection.
	///
	/// One implementation for both shapes that need it (aliased and
	/// unaliased expressions).
	fn register_and_push_projection(
		projections: &mut Vec<Projection>,
		registry: &mut ExpressionRegistry,
		expr_key: String,
		physical: Arc<dyn crate::exec::PhysicalExpr>,
		output_name: String,
		effects: ColumnEffects,
	) {
		let internal_name = registry.register_physical(
			expr_key,
			physical,
			ComputePoint::Project,
			Some(output_name.clone()),
			effects,
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
	/// `SelectProject` only handles flat `Projection::Omit` of bare fields.
	/// Nested paths like `opts.age`, function calls like `type::field(...)`, and
	/// parameters all require the full `Project` operator via [`plan_omit`].
	fn has_complex_omit(omit: &[LogicalExpr]) -> bool {
		omit.iter().any(|e| !matches!(e, LogicalExpr::Field(_)))
	}

	/// The non-negative literal integer an index step holds, if any. Anything
	/// else names its index only once evaluated.
	fn literal_index(index: &LogicalExpr) -> Option<usize> {
		match index {
			LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(idx)) => {
				usize::try_from(*idx).ok()
			}
			_ => None,
		}
	}

	/// Plan OMIT fields — resolve each expression to the paths it names, and
	/// compile each path to the steps that reach a location and the one action
	/// that removes something there.
	///
	/// A step whose meaning comes from evaluation — a filter selecting the
	/// elements to remove, a parameter spelling an index — carries the
	/// compiled expression, evaluated per row where the path is applied. A
	/// step that names no location at all, such as a method call answering
	/// with a value, compiles to an action that removes nothing, which is what
	/// the path does.
	pub(crate) async fn plan_omit(&self, omit: Vec<LogicalExpr>) -> Result<Vec<OmitPath>, Error> {
		let mut fields = Vec::with_capacity(omit.len());
		for expr in omit {
			for path in self.resolve_field_paths(expr).await? {
				fields.push(self.compile_omit_path(path).await?);
			}
		}
		Ok(fields)
	}

	/// One `OMIT` path, compiled. The outermost node names what is removed;
	/// everything under it is the walk to where.
	async fn compile_omit_path(&self, path: LogicalExpr) -> Result<OmitPath, Error> {
		let (base, action) = match path {
			LogicalExpr::Field(name) => (None, OmitAction::RemoveField(name)),
			LogicalExpr::Access {
				base,
				field,
			} => (Some(*base), OmitAction::RemoveField(field)),
			LogicalExpr::Each(base) => (Some(*base), OmitAction::Clear),
			LogicalExpr::First(base) => (Some(*base), OmitAction::RemoveFirst),
			LogicalExpr::Last(base) => (Some(*base), OmitAction::RemoveLast),
			LogicalExpr::Index {
				base,
				index,
			} => {
				let action = match Self::literal_index(&index) {
					Some(idx) => OmitAction::RemoveIndex(idx),
					None => OmitAction::RemoveIndexAt(self.physical_expr(*index).await?),
				};
				(Some(*base), action)
			}
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				let predicate = self.physical_expr(*predicate).await?;
				(Some(*base), OmitAction::RemoveMatching(predicate))
			}
			LogicalExpr::Destructure {
				base,
				fields,
			} => (Some(*base), OmitAction::Destructure(fields.into())),
			// A `Broadcast` applies its body to everything its base reaches,
			// which is the base's walk followed by the body's own path.
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				let mut compiled = Box::pin(self.compile_omit_path(*body)).await?;
				let mut steps = self.compile_omit_steps(*base).await?;
				steps.append(&mut compiled.steps);
				return Ok(OmitPath {
					steps,
					action: compiled.action,
				});
			}
			// Answers with a value rather than naming a place, so there is
			// nothing to remove.
			_ => (None, OmitAction::Nothing),
		};
		let steps = match base {
			Some(base) => self.compile_omit_steps(base).await?,
			None => Vec::new(),
		};
		Ok(OmitPath {
			steps,
			action,
		})
	}

	/// The walk `path` describes, as the steps that reach its locations.
	///
	/// A step that reaches nothing makes the whole walk reach nothing, which
	/// the caller renders as a path that removes nothing.
	fn compile_omit_steps(
		&self,
		path: LogicalExpr,
	) -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<OmitStep>, Error>> + Send + '_>> {
		Box::pin(async move {
			let (base, step) = match path {
				LogicalExpr::Current => return Ok(Vec::new()),
				LogicalExpr::Field(name) => (None, OmitStep::Field(name)),
				LogicalExpr::Access {
					base,
					field,
				} => (Some(*base), OmitStep::Field(field)),
				LogicalExpr::Each(base) => (Some(*base), OmitStep::Each),
				LogicalExpr::First(base) => (Some(*base), OmitStep::First),
				LogicalExpr::Last(base) => (Some(*base), OmitStep::Last),
				LogicalExpr::Index {
					base,
					index,
				} => {
					let step = match Self::literal_index(&index) {
						Some(idx) => OmitStep::Index(idx),
						None => OmitStep::IndexAt(self.physical_expr(*index).await?),
					};
					(Some(*base), step)
				}
				LogicalExpr::Filter {
					base,
					predicate,
				} => {
					let predicate = self.physical_expr(*predicate).await?;
					(Some(*base), OmitStep::Matching(predicate))
				}
				LogicalExpr::Destructure {
					base,
					fields,
				} => (Some(*base), OmitStep::Destructure(fields.into())),
				LogicalExpr::Broadcast {
					base,
					body,
					..
				} => {
					let mut steps = self.compile_omit_steps(*base).await?;
					steps.append(&mut self.compile_omit_steps(*body).await?);
					return Ok(steps);
				}
				// Reaches nothing, so neither does anything built on it.
				_ => return Ok(vec![OmitStep::Nowhere]),
			};
			let mut steps = match base {
				Some(base) => self.compile_omit_steps(base).await?,
				None => Vec::new(),
			};
			steps.push(step);
			Ok(steps)
		})
	}
}
