//! SELECT statement planning for the planner.
//!
//! Handles the full SELECT pipeline: source → filter → split → aggregate →
//! sort → limit → fetch → project → timeout.

use std::sync::Arc;

use super::Planner;
use super::util::{
	all_value_sources, check_forbidden_group_by_params, derive_field_name, extract_bruteforce_knn,
	extract_count_field_names, extract_matches_context, extract_record_id_point_lookup,
	extract_version, get_effective_limit_literal, has_knn_k_operator, has_knn_operator,
	has_top_level_or, idiom_to_field_name, idiom_to_field_path, index_covers_ordering,
	is_count_all_eligible, order_is_scan_compatible, strip_index_conditions,
	strip_knn_from_condition,
};
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::cnf::MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE;
use crate::err::Error;
use crate::exec::expression_registry::{ComputePoint, ExpressionRegistry, resolve_order_by_alias};
use crate::exec::field_path::FieldPath;
use crate::exec::index::access_path::{AccessPath, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
#[cfg(all(storage, not(target_family = "wasm")))]
use crate::exec::operators::ExternalSort;
use crate::exec::operators::scan::determine_scan_direction;
use crate::exec::operators::{
	Aggregate, AnalyzePlan, Compute, DynamicScan, ExplainPlan, Fetch, FieldSelection, Filter,
	KnnTopK, Limit, Project, ProjectValue, Projection, RandomShuffle, RecordIdScan, SelectProject,
	Sort, SortByKey, SortDirection, SortKey, SortTopK, SortTopKByKey, SourceExpr, Split, TableScan,
	Timeout, Union, UnionIndexScan, UnwrapExactlyOne,
};
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::field::{Field, Fields};
use crate::expr::{Cond, Expr, Idiom, Literal};

/// Configuration for the SELECT pipeline.
///
/// Bundles optional clauses from a SELECT statement to reduce parameter counts.
#[derive(Debug, Default)]
pub(crate) struct SelectPipelineConfig {
	pub cond: Option<crate::expr::cond::Cond>,
	pub split: Option<crate::expr::split::Splits>,
	pub group: Option<crate::expr::group::Groups>,
	pub order: Option<crate::expr::order::Ordering>,
	pub limit: Option<crate::expr::limit::Limit>,
	pub start: Option<crate::expr::start::Start>,
	pub omit: Vec<Expr>,
	pub is_value_source: bool,
	pub tempfiles: bool,
	/// True when the WHERE predicate has been pushed into the Scan operator.
	/// Currently informational; the actual guard is `cond: None` in the config.
	#[allow(dead_code)]
	pub filter_pushed: bool,
	/// Pre-compiled predicate. When set, `plan_pipeline` uses this directly
	/// instead of re-compiling `cond` into a new PhysicalExpr. This avoids
	/// duplicate compilation when the same predicate was already compiled
	/// for scan pushdown but ended up not being consumed by the source.
	pub precompiled_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
}

/// Describes how the WHERE predicate should be handled after source planning.
pub(crate) enum FilterAction {
	/// Source did not analyze the predicate. Use the original `cond_for_filter`.
	UseOriginal,
	/// All conditions consumed by the source. No Filter needed.
	FullyConsumed,
	/// Partial residual remains. Create a Filter with this condition only.
	Residual(Cond),
}

/// Result of planning FROM sources.
///
/// Tracks how the WHERE predicate and limit/start were handled by the
/// source operator, so the caller can avoid duplicating them in the
/// outer pipeline.
pub(crate) struct PlannedSource {
	operator: Arc<dyn ExecOperator>,
	/// How the WHERE predicate was handled by the source.
	filter_action: FilterAction,
	/// The limit and start values were consumed by the source operator.
	limit_pushed: bool,
}

impl<'ctx> Planner<'ctx> {
	/// Plan the SELECT pipeline after the source is determined.
	pub(crate) async fn plan_pipeline(
		&self,
		source: Arc<dyn ExecOperator>,
		fields: Option<Fields>,
		config: SelectPipelineConfig,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let SelectPipelineConfig {
			cond,
			split,
			group,
			order,
			limit,
			start,
			omit,
			is_value_source,
			tempfiles,
			filter_pushed: _,
			precompiled_predicate,
		} = config;

		let filtered = if let Some(predicate) = precompiled_predicate {
			// Use the pre-compiled predicate to avoid duplicate compilation
			Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
		} else if let Some(cond) = cond {
			let predicate = self.physical_expr(cond.0).await?;
			Arc::new(Filter::new(source, predicate)) as Arc<dyn ExecOperator>
		} else {
			source
		};

		let split_op = if let Some(splits) = split {
			let idioms: Vec<_> = splits.into_iter().map(|s| s.0).collect();
			Arc::new(Split {
				input: filtered,
				idioms,
				metrics: Arc::new(OperatorMetrics::new()),
			}) as Arc<dyn ExecOperator>
		} else {
			filtered
		};

		let fields = fields.unwrap_or_else(Fields::all);

		let (grouped, skip_projections) = if let Some(groups) = group {
			let group_by: Vec<_> = groups.0.into_iter().map(|g| g.0).collect();
			check_forbidden_group_by_params(&fields)?;

			let (aggregates, group_by_exprs) = self.plan_aggregation(&fields, &group_by).await?;

			(
				Arc::new(Aggregate::new(split_op, group_by, group_by_exprs, aggregates))
					as Arc<dyn ExecOperator>,
				true,
			)
		} else {
			(split_op, false)
		};

		let (sorted, sort_only_omits) = if let Some(order) = order {
			// Sort elimination: if the input is already sorted in the required
			// order, skip creating a Sort operator entirely.
			if self.can_eliminate_sort(&grouped, &order) {
				(grouped, vec![])
			} else if skip_projections {
				// GROUP BY queries use the legacy sort path because the
				// consolidated approach's Compute operator would try to
				// evaluate aggregate expressions (e.g., math::sum) on
				// individual rows rather than grouped arrays.
				(self.plan_sort(grouped, order, &start, &limit, tempfiles).await?, vec![])
			} else {
				self.plan_sort_consolidated(grouped, order, &fields, &start, &limit, tempfiles)
					.await?
			}
		} else {
			(grouped, vec![])
		};

		let limited = if limit.is_some() || start.is_some() {
			let limit_expr = match limit {
				Some(l) => Some(self.physical_expr(l.0).await?),
				None => None,
			};
			let offset_expr = match start {
				Some(s) => Some(self.physical_expr(s.0).await?),
				None => None,
			};
			Arc::new(Limit::new(sorted, limit_expr, offset_expr)) as Arc<dyn ExecOperator>
		} else {
			sorted
		};

		let mut all_omit = omit;
		for field_name in sort_only_omits {
			all_omit.push(Expr::Idiom(Idiom::field(field_name)));
		}

		let projected = if skip_projections {
			if !all_omit.is_empty() {
				let omit_fields = self.plan_omit(all_omit).await?;
				Arc::new(Project::new(limited, vec![], omit_fields, true)) as Arc<dyn ExecOperator>
			} else {
				limited
			}
		} else {
			self.plan_projections(fields, all_omit, limited, is_value_source).await?
		};

		Ok(projected)
	}

	/// Check whether the input operator's output ordering already satisfies
	/// the requested ORDER BY, allowing the Sort operator to be eliminated.
	fn can_eliminate_sort(
		&self,
		input: &Arc<dyn ExecOperator>,
		order: &crate::expr::order::Ordering,
	) -> bool {
		use crate::exec::operators::SortDirection;
		use crate::exec::ordering::SortProperty;
		use crate::expr::order::Ordering;

		let Ordering::Order(order_list) = order else {
			return false; // Random ordering can't be eliminated
		};

		// Convert the ORDER BY clause to SortProperty requirements,
		// including collate/numeric modifiers so that the satisfies
		// check rejects mismatches against raw key ordering.
		let required: Vec<SortProperty> = order_list
			.iter()
			.filter_map(|field| {
				// Only simple field paths can be matched
				crate::exec::field_path::FieldPath::try_from(&field.value).ok().map(|path| {
					let direction = if field.direction {
						SortDirection::Asc
					} else {
						SortDirection::Desc
					};
					SortProperty {
						path,
						direction,
						collate: field.collate,
						numeric: field.numeric,
					}
				})
			})
			.collect();

		// If we couldn't convert all fields, can't eliminate
		if required.len() != order_list.len() {
			return false;
		}

		// Check if the input's output ordering satisfies the requirement
		input.output_ordering().satisfies(&required)
	}

	/// Plan projections (SELECT fields or SELECT VALUE).
	pub(crate) async fn plan_projections(
		&self,
		fields: Fields,
		omit: Vec<Expr>,
		input: Arc<dyn ExecOperator>,
		is_value_source: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				let expr = self.physical_expr(selector.expr).await?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
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

				if is_value_source
					&& !has_wildcard
					&& field_list.len() == 1
					&& let Some(Field::Single(selector)) = field_list.first()
					&& selector.alias.is_none()
					&& let Expr::Param(_) = &selector.expr
				{
					let expr = self.physical_expr(selector.expr.clone()).await?;
					return Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>);
				}

				let mut field_selections = Vec::with_capacity(field_list.len());

				for field in field_list {
					if let Field::Single(selector) = field {
						let field_selection = if let Some(alias) = &selector.alias {
							let output_name = idiom_to_field_name(alias);
							let expr = self.physical_expr(selector.expr).await?;
							FieldSelection::with_alias(output_name, expr)
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
								Err(output_name) => FieldSelection::new(output_name, expr),
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

	/// Plan FETCH clause.
	pub(crate) async fn plan_fetch(
		&self,
		fetch: Option<crate::expr::fetch::Fetchs>,
		input: Arc<dyn ExecOperator>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let Some(fetchs) = fetch else {
			return Ok(input);
		};

		let mut fields = Vec::with_capacity(fetchs.len());
		for fetch_item in fetchs {
			let mut idioms = self.resolve_field_idioms(fetch_item.0).await?;
			fields.append(&mut idioms);
		}

		Ok(Arc::new(Fetch {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}) as Arc<dyn ExecOperator>)
	}

	/// Plan ORDER BY.
	pub(crate) async fn plan_sort(
		&self,
		input: Arc<dyn ExecOperator>,
		order: crate::expr::order::Ordering,
		start: &Option<crate::expr::start::Start>,
		limit: &Option<crate::expr::limit::Limit>,
		#[allow(unused)] tempfiles: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::expr::order::Ordering;

		match order {
			Ordering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok(Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>)
			}
			Ordering::Order(order_list) => {
				let order_by = self.convert_order_list(order_list).await?;

				#[cfg(all(storage, not(target_family = "wasm")))]
				if tempfiles && let Some(temp_dir) = self.ctx.temporary_directory() {
					return Ok(
						Arc::new(ExternalSort::new(input, order_by, temp_dir.to_path_buf()))
							as Arc<dyn ExecOperator>,
					);
				}

				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize
				{
					return Ok(Arc::new(SortTopK::new(input, order_by, effective_limit))
						as Arc<dyn ExecOperator>);
				}

				Ok(Arc::new(Sort::new(input, order_by)) as Arc<dyn ExecOperator>)
			}
		}
	}

	/// Plan ORDER BY with consolidated expression evaluation.
	pub(crate) async fn plan_sort_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		order: crate::expr::order::Ordering,
		fields: &Fields,
		start: &Option<crate::expr::start::Start>,
		limit: &Option<crate::expr::limit::Limit>,
		#[allow(unused)] tempfiles: bool,
	) -> Result<(Arc<dyn ExecOperator>, Vec<String>), Error> {
		use crate::expr::order::Ordering;
		use crate::expr::part::Part;

		match order {
			Ordering::Random => {
				let effective_limit = get_effective_limit_literal(start, limit);
				Ok((
					Arc::new(RandomShuffle::new(input, effective_limit)) as Arc<dyn ExecOperator>,
					vec![],
				))
			}
			Ordering::Order(order_list) => {
				let mut registry = ExpressionRegistry::new();
				let mut sort_keys = Vec::with_capacity(order_list.len());
				let mut sort_only_fields: Vec<String> = Vec::new();

				for order_field in order_list.iter() {
					let idiom = &order_field.value;

					let field_path = if let Some((resolved_expr, alias)) =
						resolve_order_by_alias(idiom, fields)
					{
						match &resolved_expr {
							Expr::Idiom(inner_idiom) => {
								let has_lookups =
									inner_idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

								if has_lookups {
									let name = registry
										.register(
											&resolved_expr,
											ComputePoint::Sort,
											Some(alias.clone()),
											self.ctx,
										)
										.await?;
									FieldPath::field(name)
								} else {
									match FieldPath::try_from(inner_idiom) {
										Ok(path) => path,
										Err(_) => {
											let name = registry
												.register(
													&resolved_expr,
													ComputePoint::Sort,
													Some(alias.clone()),
													self.ctx,
												)
												.await?;
											FieldPath::field(name)
										}
									}
								}
							}
							_ => {
								let name = registry
									.register(
										&resolved_expr,
										ComputePoint::Sort,
										Some(alias.clone()),
										self.ctx,
									)
									.await?;
								FieldPath::field(name)
							}
						}
					} else {
						match FieldPath::try_from(idiom) {
							Ok(path) => path,
							Err(_) => {
								let expr = Expr::Idiom(idiom.clone());
								let name = registry
									.register(&expr, ComputePoint::Sort, None, self.ctx)
									.await?;
								sort_only_fields.push(name.clone());
								FieldPath::field(name)
							}
						}
					};

					let direction = if order_field.direction {
						SortDirection::Asc
					} else {
						SortDirection::Desc
					};

					let mut key = SortKey::new(field_path);
					key.direction = direction;
					key.collate = order_field.collate;
					key.numeric = order_field.numeric;
					sort_keys.push(key);
				}

				let computed = if registry.has_expressions_for_point(ComputePoint::Sort) {
					let compute_fields = registry.get_expressions_for_point(ComputePoint::Sort);
					Arc::new(Compute::new(input, compute_fields)) as Arc<dyn ExecOperator>
				} else {
					input
				};

				// Use heap-based TopK when the effective limit is small.
				if let Some(effective_limit) = get_effective_limit_literal(start, limit)
					&& effective_limit <= *MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE as usize
				{
					return Ok((
						Arc::new(SortTopKByKey::new(computed, sort_keys, effective_limit))
							as Arc<dyn ExecOperator>,
						sort_only_fields,
					));
				}

				Ok((
					Arc::new(SortByKey::new(computed, sort_keys)) as Arc<dyn ExecOperator>,
					sort_only_fields,
				))
			}
		}
	}

	/// Plan SELECT projections with consolidated approach.
	#[allow(dead_code)]
	pub(crate) async fn plan_projections_consolidated(
		&self,
		input: Arc<dyn ExecOperator>,
		fields: &Fields,
		omit: &[Expr],
		computed_fields: &[(String, String)],
	) -> Result<Arc<dyn ExecOperator>, Error> {
		match fields {
			Fields::Value(selector) => {
				if !omit.is_empty() {
					return Err(Error::Query {
						message: "OMIT clause with SELECT VALUE not supported".to_string(),
					});
				}
				let expr = self.physical_expr(selector.expr.clone()).await?;
				Ok(Arc::new(ProjectValue::new(input, expr)) as Arc<dyn ExecOperator>)
			}

			Fields::Select(field_list) => {
				let is_select_all =
					field_list.len() == 1 && matches!(field_list.first(), Some(Field::All));

				if is_select_all {
					let omit_names: Vec<String> = omit
						.iter()
						.filter_map(|e| {
							if let Expr::Idiom(idiom) = e {
								Some(idiom_to_field_name(idiom))
							} else {
								None
							}
						})
						.collect();
					let projections: Vec<Projection> = std::iter::once(Projection::All)
						.chain(omit_names.into_iter().map(Projection::Omit))
						.collect();
					return Ok(Arc::new(SelectProject::new(
						input,
						projections,
						Arc::new(OperatorMetrics::new()),
					)) as Arc<dyn ExecOperator>);
				}

				let mut projections = Vec::with_capacity(field_list.len());
				let has_wildcard = field_list.iter().any(|f| matches!(f, Field::All));

				if has_wildcard {
					projections.push(Projection::All);
				}

				for field in field_list {
					match field {
						Field::All => {}
						Field::Single(selector) => {
							let output_name = if let Some(alias) = &selector.alias {
								idiom_to_field_name(alias)
							} else {
								derive_field_name(&selector.expr)
							};

							let maybe_computed =
								computed_fields.iter().find(|(_, out)| out == &output_name);

							if let Some((internal_name, _)) = maybe_computed {
								if internal_name != &output_name {
									projections.push(Projection::Rename {
										from: internal_name.clone(),
										to: output_name,
									});
								} else {
									projections.push(Projection::Include(output_name));
								}
							} else {
								projections.push(Projection::Include(output_name));
							}
						}
					}
				}

				if !omit.is_empty() {
					for e in omit {
						if let Expr::Idiom(idiom) = e {
							projections.push(Projection::Omit(idiom_to_field_name(idiom)));
						}
					}
				}

				Ok(Arc::new(SelectProject::new(
					input,
					projections,
					Arc::new(OperatorMetrics::new()),
				)) as Arc<dyn ExecOperator>)
			}
		}
	}

	// ========================================================================
	// Field Resolution Helpers
	// ========================================================================

	/// Resolve a field expression to one or more idioms.
	pub(crate) async fn resolve_field_idioms(
		&self,
		expr: Expr,
	) -> Result<Vec<crate::expr::idiom::Idiom>, Error> {
		use crate::expr::Function;

		match expr {
			Expr::Idiom(idiom) => Ok(vec![idiom]),
			Expr::Param(ref param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				let s = value.clone().coerce_to::<String>().map_err(|_| Error::InvalidFetch {
					value: value.into_literal(),
				})?;
				let idiom: Idiom = crate::syn::idiom(&s)
					.map_err(|_| Error::InvalidFetch {
						value: expr,
					})?
					.into();
				Ok(vec![idiom])
			}
			Expr::FunctionCall(ref call) => match &call.receiver {
				Function::Normal(name) if self.function_registry().is_projection(name) => {
					// Generic projection function handling: resolve each argument
					// as a string (single field) or array of strings (multiple fields)
					// and parse each as an idiom.
					let mut idioms = Vec::new();
					for arg in &call.arguments {
						match self.resolve_expr_to_string(arg) {
							Ok(s) => {
								let idiom: Idiom = crate::syn::idiom(&s)
									.map_err(|e| Error::Query {
										message: format!(
											"Failed to parse field path '{}': {}",
											s, e
										),
									})?
									.into();
								idioms.push(idiom);
							}
							Err(_) => {
								// Try resolving as an array of strings
								let strings =
									self.resolve_expr_to_string_array(arg).map_err(|_| {
										Error::Query {
											message: format!(
												"Projection function '{}' argument could not \
												 be resolved to a field path",
												name
											),
										}
									})?;
								for s in strings {
									let idiom: Idiom = crate::syn::idiom(&s)
										.map_err(|e| Error::Query {
											message: format!(
												"Failed to parse field path '{}': {}",
												s, e
											),
										})?
										.into();
									idioms.push(idiom);
								}
							}
						}
					}
					if idioms.is_empty() {
						return Err(Error::Query {
							message: format!(
								"Projection function '{}' requires at least one argument",
								name
							),
						});
					}
					Ok(idioms)
				}
				_ => Err(Error::InvalidFetch {
					value: expr,
				}),
			},
			other => Err(Error::InvalidFetch {
				value: other,
			}),
		}
	}

	fn resolve_expr_to_string(&self, expr: &Expr) -> Result<String, Error> {
		match expr {
			Expr::Literal(Literal::String(s)) => Ok(s.clone()),
			Expr::Param(param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				value.coerce_to::<String>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	fn resolve_expr_to_string_array(&self, expr: &Expr) -> Result<Vec<String>, Error> {
		match expr {
			Expr::Literal(Literal::Array(items)) => {
				items.iter().map(|item| self.resolve_expr_to_string(item)).collect()
			}
			Expr::Param(param) => {
				let value =
					self.ctx.value(param.as_str()).cloned().unwrap_or(crate::val::Value::None);
				value.coerce_to::<Vec<String>>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to an array of strings"
						.to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	/// Extract the set of field names needed by a SELECT statement.
	///
	/// Returns `None` if all fields are needed (SELECT *, wildcard present, or
	/// opaque expressions prevent static analysis). Returns `Some(set)` with
	/// the root field names needed by projections, WHERE, ORDER, GROUP, SPLIT.
	pub(crate) fn extract_needed_fields(
		fields: &Fields,
		omit: &[Expr],
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		group: Option<&crate::expr::group::Groups>,
		split: Option<&crate::expr::split::Splits>,
	) -> Option<std::collections::HashSet<String>> {
		use crate::expr::Part;
		use crate::expr::visit::{Visit, Visitor};

		// Check for SELECT * (wildcard) -- need all fields
		match fields {
			Fields::Select(field_list) => {
				if field_list.iter().any(|f| matches!(f, Field::All)) {
					return None;
				}
			}
			Fields::Value(_) => {
				// SELECT VALUE expr -- still selective
			}
		}

		/// Visitor that collects root field names from idioms and detects opaque expressions.
		struct NeededFieldExtractor {
			fields: std::collections::HashSet<String>,
			has_opaque: bool,
		}

		impl Visitor for NeededFieldExtractor {
			type Error = std::convert::Infallible;

			fn visit_idiom(&mut self, idiom: &crate::expr::Idiom) -> Result<(), Self::Error> {
				if let Some(Part::Field(name)) = idiom.0.first() {
					self.fields.insert(name.clone());
				}
				// Walk nested parts for embedded expressions
				for p in idiom.0.iter() {
					self.visit_part(p)?;
				}
				Ok(())
			}

			fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
				match expr {
					// Parameters could reference any field
					Expr::Param(_) => {
						self.has_opaque = true;
					}
					_ => {
						expr.visit(self)?;
					}
				}
				Ok(())
			}
		}

		let mut extractor = NeededFieldExtractor {
			fields: std::collections::HashSet::new(),
			has_opaque: false,
		};

		// Walk projection expressions
		match fields {
			Fields::Value(selector) => {
				let _ = extractor.visit_expr(&selector.expr);
			}
			Fields::Select(field_list) => {
				for field in field_list {
					if let Field::Single(selector) = field {
						let _ = extractor.visit_expr(&selector.expr);
						if let Some(alias) = &selector.alias {
							let _ = extractor.visit_idiom(alias);
						}
					}
				}
			}
		}

		// Walk OMIT fields (they may reference computed fields that need evaluation)
		for expr in omit {
			let _ = extractor.visit_expr(expr);
		}

		// Walk WHERE condition
		if let Some(cond) = cond {
			let _ = extractor.visit_expr(&cond.0);
		}

		// Walk ORDER BY
		if let Some(ordering) = order {
			match ordering {
				crate::expr::order::Ordering::Random => {}
				crate::expr::order::Ordering::Order(order_list) => {
					for order in order_list.iter() {
						let _ = extractor.visit_idiom(&order.value);
					}
				}
			}
		}

		// Walk GROUP BY
		if let Some(groups) = group {
			for group in groups.0.iter() {
				let _ = extractor.visit_idiom(&group.0);
			}
		}

		// Walk SPLIT
		if let Some(splits) = split {
			for split in splits.iter() {
				let _ = extractor.visit_idiom(&split.0);
			}
		}

		if extractor.has_opaque {
			None
		} else {
			Some(extractor.fields)
		}
	}

	// ========================================================================
	// SELECT planning with plan-time index resolution
	// ========================================================================

	/// Plan a SELECT statement.
	///
	/// Performs plan-time index resolution when a transaction is available,
	/// enabling sort elimination and concrete scan operators.
	pub(crate) async fn plan_select_statement(
		&self,
		mut select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let explain = select.explain.take();
		let plan = Box::pin(self.plan_select_core(select)).await?;
		match explain {
			Some(crate::expr::explain::Explain(full)) => {
				if full {
					Ok(Arc::new(AnalyzePlan {
						plan,
						format: crate::expr::ExplainFormat::Json,
						redact_volatile_explain_attrs: self.ctx.redact_volatile_explain_attrs(),
					}))
				} else {
					Ok(Arc::new(ExplainPlan {
						plan,
						format: crate::expr::ExplainFormat::Json,
					}))
				}
			}
			None => Ok(plan),
		}
	}

	/// Core SELECT planning logic.
	///
	/// Resolves sources (with plan-time index analysis when a transaction is
	/// available), then builds the pipeline: filter → split → aggregate →
	/// sort (with elimination) → limit → project → fetch → timeout.
	async fn plan_select_core(
		&self,
		select: crate::expr::statements::SelectStatement,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let crate::expr::statements::SelectStatement {
			fields,
			omit,
			only,
			what,
			with,
			cond,
			split,
			group,
			order,
			limit,
			start,
			fetch,
			version,
			timeout,
			explain: _,
			tempfiles,
		} = select;

		let version = extract_version(version, self).await?;

		// COUNT fast-path
		if is_count_all_eligible(&fields, &group, &cond, &split, &order, &fetch, &omit, &what) {
			use crate::exec::operators::CountScan;
			let table_expr = self
				.physical_expr(what.into_iter().next().expect("what verified non-empty"))
				.await?;
			let field_names = extract_count_field_names(&fields);
			let count_scan: Arc<dyn ExecOperator> =
				Arc::new(CountScan::new(table_expr, version, field_names));
			let timed = match timeout {
				Expr::Literal(Literal::None) => count_scan,
				te => {
					let tp = self.physical_expr(te).await?;
					Arc::new(Timeout::new(count_scan, Some(tp))) as Arc<dyn ExecOperator>
				}
			};
			return if only {
				Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
			} else {
				Ok(timed)
			};
		}

		// Fast path: SELECT [*|fields] FROM <literal RecordId>
		if what.len() == 1
			&& matches!(&what[0], Expr::Literal(Literal::RecordId(_)))
			&& cond.is_none()
			&& order.is_none()
			&& group.is_none()
			&& split.is_none()
			&& fetch.is_none()
			&& with.is_none()
		{
			let rid_expr = match what.into_iter().next() {
				Some(e @ Expr::Literal(Literal::RecordId(_))) => self.physical_expr(e).await?,
				_ => unreachable!("verified above"),
			};
			let scan: Arc<dyn ExecOperator> =
				Arc::new(RecordIdScan::new(rid_expr, version, None, None));
			let limited = if limit.is_some() || start.is_some() {
				let limit_expr = match limit {
					Some(l) => Some(self.physical_expr(l.0).await?),
					None => None,
				};
				let start_expr = match start {
					Some(s) => Some(self.physical_expr(s.0).await?),
					None => None,
				};
				Arc::new(Limit::new(scan, limit_expr, start_expr)) as Arc<dyn ExecOperator>
			} else {
				scan
			};
			let projected = self.plan_projections(fields, omit, limited, false).await?;
			let timed = match timeout {
				Expr::Literal(Literal::None) => projected,
				te => {
					let tp = self.physical_expr(te).await?;
					Arc::new(Timeout::new(projected, Some(tp))) as Arc<dyn ExecOperator>
				}
			};
			return if only {
				Ok(Arc::new(UnwrapExactlyOne::new(timed, true)))
			} else {
				Ok(timed)
			};
		}

		let is_value_source = all_value_sources(&what);
		// Prefer literal tables over parameter-resolved tables so that
		// `FROM $t, article` binds MATCHES context to `article`, not `$t`.
		let primary_table = what
			.iter()
			.find_map(|e| match e {
				Expr::Table(t) => Some(t.clone()),
				_ => None,
			})
			.or_else(|| {
				what.iter().find_map(|e| match e {
					Expr::Param(p) => {
						if let Some(crate::val::Value::Table(t)) = self.ctx.value(p.as_str()) {
							Some(t.clone())
						} else {
							None
						}
					}
					_ => None,
				})
			});
		let has_knn_early = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));

		let planning_ctx: std::borrow::Cow<'_, crate::ctx::FrozenContext> =
			if let Some(ref c) = cond {
				let mc = extract_matches_context(c);
				let hm = !mc.is_empty();
				if hm || has_knn_early {
					let mut child = crate::ctx::Context::new(self.ctx);
					if hm {
						let mut mc = mc;
						if let Some(ref t) = primary_table {
							mc.set_table(t.clone());
						}
						child.set_matches_context(mc);
					}
					if has_knn_early {
						child.set_knn_context(std::sync::Arc::new(
							crate::exec::function::KnnContext::new(),
						));
					}
					std::borrow::Cow::Owned(child.freeze())
				} else {
					std::borrow::Cow::Borrowed(self.ctx)
				}
			} else {
				std::borrow::Cow::Borrowed(self.ctx)
			};

		// Propagate txn to the inner planner
		let pp = if let Some(ref txn) = self.txn {
			Planner::with_txn(&planning_ctx, txn.clone(), self.ns.clone(), self.db.clone())
		} else {
			Planner::new(&planning_ctx)
		};

		let needed_fields = Self::extract_needed_fields(
			&fields,
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group.as_ref(),
			split.as_ref(),
		);
		let source_is_single_scan = what.len() == 1
			&& matches!(what[0], Expr::Table(_) | Expr::FunctionCall(_) | Expr::Postfix { .. })
			|| (what.len() == 1
				&& matches!(&what[0], Expr::Param(p) if {
					matches!(self.ctx.value(p.as_str()), Some(crate::val::Value::Table(_)))
				}));

		// KNN handling
		let has_knn = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));
		let brute_force_knn = if has_knn {
			cond.as_ref().and_then(extract_bruteforce_knn)
		} else {
			None
		};

		let (cond_for_index, cond_for_filter) = if has_knn {
			let stripped = cond.as_ref().and_then(strip_knn_from_condition);
			if stripped.as_ref().is_some_and(|c| has_knn_operator(&c.0)) {
				return Err(Error::Query {
					message: "KNN operators nested in OR/NOT expressions or mixed with \
					 unsupported KNN variants are not supported"
						.to_string(),
				});
			}
			if brute_force_knn.is_some() {
				(stripped.clone(), stripped)
			} else if cond.as_ref().is_some_and(|c| has_knn_k_operator(&c.0)) {
				return Err(Error::PlannerUnimplemented(
					"Brute-force KNN with parameter-based vectors is not supported \
					 in the streaming executor"
						.to_string(),
				));
			} else {
				(cond, stripped)
			}
		} else {
			let c = cond;
			(c.clone(), c)
		};

		let scan_predicate = if source_is_single_scan {
			match cond_for_filter.as_ref() {
				Some(c) => Some(pp.physical_expr(c.0.clone()).await?),
				None => None,
			}
		} else {
			None
		};

		// Check prerequisites for limit pushdown that don't depend on the
		// access path. The per-access-path decision (whether the scan
		// ordering covers the ORDER BY) is made inside plan_source().
		let can_push_limit = source_is_single_scan
			&& brute_force_knn.is_none()
			&& !has_top_level_or(cond_for_filter.as_ref())
			&& limit.is_some()
			&& split.is_none()
			&& group.is_none();

		let can_soft_push_limit = !can_push_limit
			&& source_is_single_scan
			&& brute_force_knn.is_none()
			&& limit.is_some()
			&& split.is_some()
			&& group.is_none();

		let (scan_limit, scan_start) = if can_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.0.clone()).await?),
					None => None,
				},
				match start.as_ref() {
					Some(s) => Some(pp.physical_expr(s.0.clone()).await?),
					None => None,
				},
			)
		} else if can_soft_push_limit {
			(
				match limit.as_ref() {
					Some(l) => Some(pp.physical_expr(l.0.clone()).await?),
					None => None,
				},
				None,
			)
		} else {
			(None, None)
		};

		// Keep a clone of the scan predicate so we can reuse it as a
		// precompiled predicate for the pipeline Filter when the source
		// does not consume it (FilterAction::UseOriginal). This avoids
		// compiling the same AST expression into a PhysicalExpr twice.
		let scan_predicate_for_reuse = scan_predicate.clone();

		// Source resolution with plan-time index analysis.
		// The result tracks whether the predicate and limit/start were
		// consumed by the source operator, so we can avoid duplicating
		// them in the outer pipeline.
		let mut planned = pp
			.plan_sources(
				what,
				version,
				cond_for_index.as_ref(),
				order.as_ref(),
				with.as_ref(),
				needed_fields,
				scan_predicate,
				scan_limit,
				scan_start,
			)
			.await?;

		if can_soft_push_limit {
			planned.limit_pushed = false;
		}

		// Resolve the pipeline condition from the filter action.
		// - FullyConsumed: the source handles the entire predicate, no Filter.
		// - Residual: only the residual part needs a Filter.
		// - UseOriginal: the source did not analyze the predicate, use as-is.
		//
		// When UseOriginal and we already compiled a scan_predicate from the
		// same expression, reuse it as precompiled_predicate to avoid paying
		// the PhysicalExpr compilation cost a second time.
		let (pipeline_cond, precompiled_predicate) = match planned.filter_action {
			FilterAction::FullyConsumed => (None, None),
			FilterAction::Residual(residual) => (Some(residual), None),
			FilterAction::UseOriginal => {
				if scan_predicate_for_reuse.is_some() {
					// Reuse the already-compiled predicate
					(None, scan_predicate_for_reuse)
				} else {
					(cond_for_filter, None)
				}
			}
		};

		// KNN wrapping
		let had_bruteforce_knn = brute_force_knn.is_some();
		let source = if let Some(kp) = brute_force_knn {
			// Residual predicates (non-KNN WHERE conditions) must be applied
			// BEFORE ranking by distance. Otherwise rows that don't satisfy
			// the WHERE clause can consume top-K slots and push out valid rows.
			//
			// When the predicate was fully consumed by the source operator, it
			// is already applied there. Otherwise we add an explicit Filter.
			let input = if let Some(c) = &pipeline_cond {
				let pred = pp.physical_expr(c.0.clone()).await?;
				Arc::new(Filter::new(planned.operator, pred)) as Arc<dyn ExecOperator>
			} else {
				planned.operator
			};
			let knn_ctx = planning_ctx.get_knn_context().cloned();
			Arc::new(
				KnnTopK::new(input, kp.field, kp.vector, kp.k as usize, kp.distance)
					.with_knn_context(knn_ctx),
			) as Arc<dyn ExecOperator>
		} else {
			planned.operator
		};

		// Build pipeline.
		// When the predicate was consumed (fully or partially) by the source,
		// use the computed pipeline_cond (which may be None or a residual).
		// When limit/start were pushed, omit them to avoid double application.
		// ORDER BY is always passed through — sort elimination via
		// `can_eliminate_sort()` in `plan_pipeline()` handles it independently.
		let config = SelectPipelineConfig {
			cond: if had_bruteforce_knn {
				None
			} else {
				pipeline_cond
			},
			split,
			group,
			order,
			limit: if planned.limit_pushed {
				None
			} else {
				limit
			},
			start: if planned.limit_pushed {
				None
			} else {
				start
			},
			omit,
			is_value_source,
			tempfiles,
			filter_pushed: false,
			precompiled_predicate: if had_bruteforce_knn {
				None
			} else {
				precompiled_predicate
			},
		};

		let projected = pp.plan_pipeline(source, Some(fields), config).await?;
		let fetched = pp.plan_fetch(fetch, projected).await?;
		let timed = match timeout {
			Expr::Literal(Literal::None) => fetched,
			te => {
				let tp = pp.physical_expr(te).await?;
				Arc::new(Timeout::new(fetched, Some(tp))) as Arc<dyn ExecOperator>
			}
		};
		if only {
			Ok(Arc::new(UnwrapExactlyOne::new(timed, !is_value_source)))
		} else {
			Ok(timed)
		}
	}

	/// Plan FROM sources with plan-time index resolution.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_sources(
		&self,
		what: Vec<Expr>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
		needed_fields: Option<std::collections::HashSet<String>>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
	) -> Result<PlannedSource, Error> {
		if what.is_empty() {
			return Err(Error::Query {
				message: "SELECT requires at least one source".to_string(),
			});
		}
		let mut plans = Vec::with_capacity(what.len());
		for expr in what {
			let p = self
				.plan_source(
					expr,
					version.clone(),
					cond,
					order,
					with,
					needed_fields.clone(),
					scan_predicate.clone(),
					scan_limit.clone(),
					scan_start.clone(),
				)
				.await?;
			plans.push(p);
		}
		if plans.len() == 1 {
			Ok(plans.pop().expect("verified non-empty"))
		} else {
			// Multiple sources are combined via Union; pushdowns are not
			// applicable because source_is_single_scan is false when
			// what.len() > 1, so scan_predicate/scan_limit are always None.
			let operators = plans.into_iter().map(|p| p.operator).collect();
			Ok(PlannedSource {
				operator: Arc::new(Union::new(operators)),
				filter_action: FilterAction::UseOriginal,
				limit_pushed: false,
			})
		}
	}

	/// Plan a single FROM source.
	///
	/// When the planner has a transaction and the source is a table,
	/// resolves the access path at plan time and creates the concrete
	/// operator (IndexScan, FullTextScan, KnnScan) directly. This
	/// avoids redundant index analysis at execution time and enables
	/// sort elimination via `output_ordering()`.
	///
	/// Without a transaction, creates a generic `Scan` that resolves
	/// its access path at execution time.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn plan_source(
		&self,
		expr: Expr,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
		needed_fields: Option<std::collections::HashSet<String>>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::{FullTextScan, IndexScan, KnnScan};

		// Optimisation: WHERE id = <RecordId> -> point lookup.
		// Detects `id = <RecordId literal>` in the top-level AND chain and
		// converts the table scan into a RecordIdScan, avoiding index
		// analysis and full-table iteration entirely.
		//
		// Skipped when the condition contains a KNN operator, because the
		// KNN access path (KnnScan / HNSW) must be resolved by
		// resolve_access_path() to populate KnnContext correctly.
		if let Expr::Table(ref table_name) = expr
			&& !cond.is_some_and(|c| has_knn_operator(&c.0))
			&& let Some(rid_expr) = cond.and_then(|c| extract_record_id_point_lookup(c, table_name))
		{
			let filter_action = if scan_predicate.is_some() {
				FilterAction::FullyConsumed
			} else {
				FilterAction::UseOriginal
			};
			let record_id_expr = self.physical_expr(rid_expr).await?;
			return Ok(PlannedSource {
				operator: Arc::new(RecordIdScan::new(
					record_id_expr,
					version,
					needed_fields,
					scan_predicate,
				)) as Arc<dyn ExecOperator>,
				filter_action,
				limit_pushed: false,
			});
		}

		// When we have a txn and the source is a table, resolve the
		// access path at plan time and create the concrete operator.
		if let Expr::Table(ref table_name) = expr
			&& let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db)
		{
			let resolved =
				self.resolve_access_path(txn, ns, db, table_name, cond, order, with).await;
			if let Ok(Some((access_path, direction))) = resolved {
				let table = table_name.clone();
				let knn_ctx = self.ctx.get_knn_context().cloned();
				match access_path {
					AccessPath::BTreeScan {
						index_ref,
						access,
						direction,
					} => {
						// Strip index-covered conditions from the WHERE
						// clause. If all conditions are consumed, no Filter
						// operator will be created in the pipeline.
						let filter_action = if let Some(c) = cond {
							match strip_index_conditions(c, &access, &index_ref.cols) {
								None => FilterAction::FullyConsumed,
								Some(residual) => FilterAction::Residual(residual),
							}
						} else {
							FilterAction::FullyConsumed
						};
						// Push limit to IndexScan when the index ordering
						// covers the ORDER BY (or there is no ORDER BY).
						let push = scan_limit.is_some()
							&& match order {
								None => true,
								Some(ord) => index_covers_ordering(&index_ref, direction, ord),
							};
						let (idx_limit, idx_start, limit_pushed) = if push {
							(scan_limit.clone(), scan_start.clone(), true)
						} else {
							(None, None, false)
						};
						return Ok(PlannedSource {
							operator: Arc::new(IndexScan::new(
								index_ref,
								access,
								direction,
								table,
								idx_limit,
								idx_start,
								version.clone(),
							)) as Arc<dyn ExecOperator>,
							filter_action,
							limit_pushed,
						});
					}
					AccessPath::FullTextSearch {
						index_ref,
						query,
						operator,
					} => {
						return Ok(PlannedSource {
							operator: Arc::new(FullTextScan::new(
								index_ref,
								query,
								operator,
								table,
								version.clone(),
							)) as Arc<dyn ExecOperator>,
							filter_action: FilterAction::UseOriginal,
							limit_pushed: false,
						});
					}
					AccessPath::KnnSearch {
						index_ref,
						vector,
						k,
						ef,
					} => {
						// Strip KNN operators from the condition to get the residual
						// (non-KNN predicates). These are pushed into the HNSW search
						// so that non-matching rows don't consume top-K slots.
						let residual_cond = cond.and_then(strip_knn_from_condition);
						return Ok(PlannedSource {
							operator: Arc::new(KnnScan::new(
								index_ref,
								vector,
								k,
								ef,
								table,
								version.clone(),
								knn_ctx,
								residual_cond,
							)) as Arc<dyn ExecOperator>,
							filter_action: FilterAction::UseOriginal,
							limit_pushed: false,
						});
					}
					AccessPath::TableScan => {
						let filter_action = if scan_predicate.is_some() {
							FilterAction::FullyConsumed
						} else {
							FilterAction::UseOriginal
						};
						// TableScan can only provide ordering for `id ASC/DESC`.
						// Push limit only when ORDER BY is compatible with the
						// natural KV scan direction.
						let push =
							scan_limit.is_some() && order_is_scan_compatible(&order.cloned());
						let (tbl_limit, tbl_start, limit_pushed) = if push {
							(scan_limit.clone(), scan_start.clone(), true)
						} else {
							(None, None, false)
						};
						return Ok(PlannedSource {
							operator: Arc::new(TableScan::new(
								table,
								direction,
								version,
								scan_predicate,
								tbl_limit,
								tbl_start,
								needed_fields,
							)) as Arc<dyn ExecOperator>,
							filter_action,
							limit_pushed,
						});
					}
					AccessPath::Union(paths) => {
						// Create a UnionIndexScan with a sub-operator for
						// each OR branch. This is consistent with how
						// BTreeScan/FullTextSearch/KnnSearch create their
						// operators at plan time. The residual WHERE
						// predicate is handled by a Filter above
						// (filter_action = UseOriginal).
						let mut sub_operators: Vec<Arc<dyn ExecOperator>> =
							Vec::with_capacity(paths.len());
						for path in paths {
							let sub_op: Arc<dyn ExecOperator> = match path {
								AccessPath::BTreeScan {
									index_ref,
									access,
									direction,
								} => Arc::new(IndexScan::new(
									index_ref,
									access,
									direction,
									table.clone(),
									None,
									None,
									version.clone(),
								)),
								AccessPath::FullTextSearch {
									index_ref,
									query,
									operator,
								} => Arc::new(FullTextScan::new(
									index_ref,
									query,
									operator,
									table.clone(),
									version.clone(),
								)),
								AccessPath::KnnSearch {
									index_ref,
									vector,
									k,
									ef,
								} => {
									let residual_cond = cond.and_then(strip_knn_from_condition);
									Arc::new(KnnScan::new(
										index_ref,
										vector,
										k,
										ef,
										table.clone(),
										version.clone(),
										knn_ctx.clone(),
										residual_cond,
									))
								}
								// TableScan and nested Union should not
								// appear as sub-paths; fall back safely.
								_ => Arc::new(TableScan::new(
									table.clone(),
									direction,
									None,
									None,
									None,
									None,
									None,
								)),
							};
							sub_operators.push(sub_op);
						}
						// UnionIndexScan handles field-level permissions
						// and computed-field materialization internally
						// (same pattern as TableScan). The outer
						// pipeline handles Filter, Sort, and Limit.
						return Ok(PlannedSource {
							operator: Arc::new(UnionIndexScan::new(
								table,
								sub_operators,
								needed_fields,
							)) as Arc<dyn ExecOperator>,
							filter_action: FilterAction::UseOriginal,
							limit_pushed: false,
						});
					}
				}
			}
		}

		// Fallback: create a generic Scan operator (index resolved at runtime)
		let knn_ctx = self.ctx.get_knn_context().cloned();

		match expr {
			Expr::Table(_) => {
				let filter_action = if scan_predicate.is_some() {
					FilterAction::FullyConsumed
				} else {
					FilterAction::UseOriginal
				};
				let limit_pushed = scan_limit.is_some();
				let table_expr = self.physical_expr(expr).await?;
				Ok(PlannedSource {
					operator: Arc::new(
						DynamicScan::new(
							table_expr,
							version,
							cond.cloned(),
							order.cloned(),
							with.cloned(),
							needed_fields,
							scan_predicate,
							scan_limit,
							scan_start,
						)
						.with_knn_context(knn_ctx),
					) as Arc<dyn ExecOperator>,
					filter_action,
					limit_pushed,
				})
			}
			Expr::Literal(crate::expr::literal::Literal::RecordId(rid)) => {
				let record_id_expr = self
					.physical_expr(Expr::Literal(crate::expr::literal::Literal::RecordId(rid)))
					.await?;
				Ok(PlannedSource {
					operator: Arc::new(RecordIdScan::new(
						record_id_expr,
						version,
						needed_fields,
						None,
					)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			Expr::Select(inner_select) => {
				if version.is_some() {
					return Err(Error::Query {
						message: "VERSION clause cannot be used with a subquery source. \
								  Place the VERSION clause inside the subquery instead."
							.to_string(),
					});
				}
				Ok(PlannedSource {
					operator: self.plan_select_statement(*inner_select).await?,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			Expr::Param(ref param) => match self.ctx.value(param.as_str()) {
				Some(crate::val::Value::Table(_)) => {
					let filter_action = if scan_predicate.is_some() {
						FilterAction::FullyConsumed
					} else {
						FilterAction::UseOriginal
					};
					let limit_pushed = scan_limit.is_some();
					let table_expr = self.physical_expr(expr).await?;
					Ok(PlannedSource {
						operator: Arc::new(
							DynamicScan::new(
								table_expr,
								version,
								cond.cloned(),
								order.cloned(),
								with.cloned(),
								needed_fields,
								scan_predicate,
								scan_limit,
								scan_start,
							)
							.with_knn_context(knn_ctx),
						) as Arc<dyn ExecOperator>,
						filter_action,
						limit_pushed,
					})
				}
				Some(crate::val::Value::RecordId(_)) => {
					let record_id_expr = self.physical_expr(expr).await?;
					Ok(PlannedSource {
						operator: Arc::new(RecordIdScan::new(
							record_id_expr,
							version,
							needed_fields,
							None,
						)) as Arc<dyn ExecOperator>,
						filter_action: FilterAction::UseOriginal,
						limit_pushed: false,
					})
				}
				Some(_) | None => {
					let phys_expr = self.physical_expr(expr).await?;
					Ok(PlannedSource {
						operator: Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>,
						filter_action: FilterAction::UseOriginal,
						limit_pushed: false,
					})
				}
			},
			Expr::FunctionCall(_)
			| Expr::Postfix {
				..
			} => {
				let filter_action = if scan_predicate.is_some() {
					FilterAction::FullyConsumed
				} else {
					FilterAction::UseOriginal
				};
				let limit_pushed = scan_limit.is_some();
				let source_expr = self.physical_expr(expr).await?;
				Ok(PlannedSource {
					operator: Arc::new(
						DynamicScan::new(
							source_expr,
							version,
							cond.cloned(),
							order.cloned(),
							with.cloned(),
							needed_fields,
							scan_predicate,
							scan_limit,
							scan_start,
						)
						.with_knn_context(knn_ctx),
					) as Arc<dyn ExecOperator>,
					filter_action,
					limit_pushed,
				})
			}
			other => {
				let phys_expr = self.physical_expr(other).await?;
				Ok(PlannedSource {
					operator: Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
		}
	}

	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns `None` if the namespace/database/table cannot be resolved.
	/// Resolve the optimal access path for a table at plan time.
	///
	/// Performs index analysis using the WHERE condition and ORDER BY clause.
	/// Returns the selected `AccessPath` and scan direction, or `None` if
	/// the namespace/database cannot be resolved.
	#[allow(clippy::too_many_arguments)]
	async fn resolve_access_path(
		&self,
		txn: &crate::kvs::Transaction,
		ns_name: &str,
		db_name: &str,
		table_name: &crate::val::TableName,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		with: Option<&crate::expr::with::With>,
	) -> Result<Option<(AccessPath, crate::idx::planner::ScanDirection)>, Error> {
		let direction = determine_scan_direction(&order.cloned());

		if matches!(with, Some(crate::expr::with::With::NoIndex)) {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Look up namespace and database to get IDs
		let ns_def = match txn.get_ns_by_name(ns_name).await {
			Ok(Some(ns)) => ns,
			_ => return Ok(None),
		};
		let db_def = match txn.get_db_by_name(ns_name, db_name).await {
			Ok(Some(db)) => db,
			_ => return Ok(None),
		};

		// Fetch indexes for the table
		let indexes =
			match txn.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name).await {
				Ok(idx) => idx,
				Err(_) => return Ok(None),
			};

		if indexes.is_empty() {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		let analyzer = IndexAnalyzer::new(indexes, with);
		let candidates = analyzer.analyze(cond, order);

		if candidates.is_empty() {
			if let Some(path) = analyzer.try_or_union(cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding IN operators into union of equality lookups
			if let Some(path) = analyzer.try_in_expansion(cond, direction) {
				return Ok(Some((path, direction)));
			}
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		let path = select_access_path(candidates, with, direction);

		// When the best single-index path is a full-range scan (ORDER BY
		// only, no WHERE selectivity), also try a multi-index union for
		// OR conditions. The union reads only matching rows from each
		// branch, which is typically far better than scanning every row
		// in the index. The outer pipeline adds a Sort when the union
		// does not satisfy ORDER BY.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_or_union(cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}

		Ok(Some((path, direction)))
	}
}
