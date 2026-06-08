//! SELECT statement planning for the planner.
//!
//! Handles the full SELECT pipeline: source → filter → split → aggregate →
//! sort → limit → project → fetch → timeout.
//!
//! Projection uses a fast path that classifies SELECT fields at plan time:
//! - **Simple field paths** (e.g. `name`, `age`): handled by `SelectProject` with synchronous field
//!   selection — zero async/expression overhead.
//! - **Complex expressions** (e.g. `math::sum(scores) AS total`): pre-evaluated by a `Compute`
//!   operator, then picked by `SelectProject`.
//! - **Projection functions** or **nested output paths**: fall back to the full `Project` operator.
//!
//! An `ExpressionRegistry` is shared between ORDER BY and projection planning
//! to deduplicate expressions that appear in both clauses.

mod pipeline;
mod projection;

use std::sync::Arc;

pub(crate) use pipeline::{
	FilterAction, PlannedSource, SelectPipelineConfig, WhereClauseState,
	filter_action_for_predicate,
};

use super::Planner;
use super::util::{
	SELECT_ITERATION_PARAMS, all_value_sources, derive_field_name, extract_bruteforce_knn,
	extract_count_field_names, extract_matches_context, extract_record_id_point_lookup,
	extract_version, fold_condition_expressions, has_knn_k_operator, has_knn_ktree_operator,
	has_knn_operator, has_top_level_or, idiom_to_field_name, index_covers_ordering,
	is_bounded_topk_downstream, is_count_all_eligible, is_indexed_count_eligible,
	order_is_scan_compatible, resolve_condition_params, resolve_param_value,
	resolve_projection_field_idioms, strip_fts_condition, strip_index_conditions,
	strip_knn_from_condition, strip_union_index_conditions,
};
use crate::catalog::Index;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, BTreeAccess, IndexRef, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::operators::scan::determine_scan_direction;
use crate::exec::operators::scan::resolved::{ResolvedTableContext, resolve_table_context};
use crate::exec::operators::{
	AnalyzePlan, DynamicScan, ExplainPlan, Fetch, Filter, KnnTopK, Limit, RecordIdScan,
	SortDirection, SourceExpr, TableScan, Timeout, Union, UnionIndexScan, UnwrapExactlyOne,
	VersionScope,
};
use crate::exec::pre_decode_filter::pre_decode_filter_status_at_plan_time;
use crate::exec::{ExecOperator, OperatorMetrics};
use crate::expr::field::{Field, Fields};
use crate::expr::order::Ordering as OrderClause;
use crate::expr::with::With;
use crate::expr::{Cond, Expr, Idiom, Literal};
use crate::idx::planner::ScanDirection;
use crate::kvs::Transaction;
use crate::kvs::index::filter_online_indexes;
use crate::val::TableName;

impl<'ctx> Planner<'ctx> {
	/// Resolve a parameter to its value at plan time.
	///
	/// Delegates to [`resolve_param_value`] with the planner's context and
	/// namespace/database IDs (looked up from the transaction when available).
	async fn resolve_param(&self, name: &str) -> Option<crate::val::Value> {
		let ns_db = self.ns_db_ids().await;
		resolve_param_value(name, self.ctx, ns_db, SELECT_ITERATION_PARAMS).await
	}

	/// Look up (NamespaceId, DatabaseId) from the planner's transaction.
	///
	/// Cached on first hit — `(self.ns, self.db, self.txn)` are immutable
	/// for the planner's lifetime, so callers (parameter resolution,
	/// SELECT planning, etc.) can hit this many times per plan without
	/// re-reading the catalog.
	///
	/// Returns `None` when the transaction or namespace/database is
	/// unavailable; callers fall back to runtime resolution.
	///
	/// Logging policy on the *first* lookup:
	/// - **`Ok(None)`** ("ns/db doesn't exist yet"): `debug!` — common during schema-creation
	///   flows; not actionable.
	/// - **`Err(e)`** (storage error, lock contention, etc.): `warn!` — exceptional. Plan-time
	///   index resolution / sort elimination silently degrades, so it should be visible in
	///   production monitoring.
	async fn ns_db_ids(&self) -> Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)> {
		self.ns_db_ids_cache
			.get_or_init(|| async {
				let (txn, ns, db) = match (&self.txn, &self.ns, &self.db) {
					(Some(txn), Some(ns), Some(db)) => (txn, ns, db),
					_ => return None,
				};
				match txn.get_db_by_name(ns, db, None).await {
					Ok(Some(db_def)) => Some((db_def.namespace_id, db_def.database_id)),
					Ok(None) => {
						tracing::debug!(
							ns = %ns,
							db = %db,
							"plan-time db lookup returned None; falling back to runtime resolution",
						);
						None
					}
					Err(e) => {
						tracing::warn!(
							ns = %ns,
							db = %db,
							error = %e,
							"plan-time db lookup failed; falling back to runtime resolution \
							 (plan-time optimisations may be unavailable)",
						);
						None
					}
				}
			})
			.await
			.as_ref()
			.copied()
	}

	/// Try to evaluate a source expression to a concrete `Value` at plan time.
	///
	/// Recursively resolves parameters and evaluates synchronous built-in
	/// function calls when all arguments are known. Returns `None` when any
	/// part of the expression cannot be resolved (e.g. row-scoped variables,
	/// async functions, or unknown parameters).
	async fn try_resolve_expr_value(&self, expr: &Expr) -> Option<crate::val::Value> {
		use crate::expr::function::Function;
		use crate::val::Value;

		match expr {
			Expr::Param(param) => self.resolve_param(param.as_str()).await,
			Expr::Literal(lit) => super::util::try_literal_to_value(lit),
			Expr::Table(name) => Some(Value::Table(name.clone())),
			Expr::FunctionCall(fc) => {
				let Function::Normal(ref name) = fc.receiver else {
					return None;
				};
				self.ctx.check_allowed_function(name).ok()?;
				let mut args = Vec::with_capacity(fc.arguments.len());
				for arg in &fc.arguments {
					args.push(Box::pin(self.try_resolve_expr_value(arg)).await?);
				}
				crate::fnc::synchronous(self.ctx, None, name, args).ok()
			}
			_ => None,
		}
	}

	/// Try to resolve FROM source expressions at plan time.
	///
	/// Walks each source in the `what` vector and attempts to evaluate it
	/// to a concrete value. When a source resolves to `Value::Table`, it is
	/// rewritten to `Expr::Table` so that downstream planning (index
	/// resolution, limit pushdown, sort elimination) works identically to
	/// literal table names.
	async fn resolve_source_exprs(&self, what: &mut [Expr]) {
		for expr in what.iter_mut() {
			match expr {
				Expr::Table(_) | Expr::Literal(_) | Expr::Select(_) => continue,
				_ => {}
			}
			if let Some(value) = self.try_resolve_expr_value(expr).await {
				match value {
					crate::val::Value::Table(t) => *expr = Expr::Table(t),
					crate::val::Value::RecordId(rid) => {
						*expr = crate::val::Value::RecordId(rid).into_literal();
					}
					_ => *expr = value.into_literal(),
				}
			}
		}
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
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
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
						match self.resolve_expr_to_string(arg).await {
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
								let strings = self
									.resolve_expr_to_string_array(arg)
									.await
									.map_err(|_| Error::Query {
										message: format!(
											"Projection function '{}' argument could not \
												 be resolved to a field path",
											name
										),
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

	async fn resolve_expr_to_string(&self, expr: &Expr) -> Result<String, Error> {
		match expr {
			Expr::Literal(Literal::String(s)) => Ok(s.as_str().to_owned()),
			Expr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
				value.coerce_to::<String>().map_err(|_| Error::Query {
					message: "OMIT/FETCH parameter did not resolve to a string".to_string(),
				})
			}
			_ => Err(Error::Query {
				message: "OMIT/FETCH with computed expressions not yet supported".to_string(),
			}),
		}
	}

	async fn resolve_expr_to_string_array(&self, expr: &Expr) -> Result<Vec<String>, Error> {
		match expr {
			Expr::Literal(Literal::Array(items)) => {
				let mut result = Vec::with_capacity(items.len());
				for item in items {
					result.push(self.resolve_expr_to_string(item).await?);
				}
				Ok(result)
			}
			Expr::Param(param) => {
				let value =
					self.resolve_param(param.as_str()).await.unwrap_or(crate::val::Value::None);
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
		use crate::expr::visit::{Visit, Visitor};
		use crate::expr::{Expr, Part};

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
		///
		/// Idiom roots determine whose row a field access belongs to, via
		/// [`super::row_scope::classify_idiom_root`]:
		///
		/// - `ThisRow` (unrooted, `$this.x`, `$self.x`): leading `Part::Field` names are columns of
		///   the current row's table — add them to the needed set.
		/// - `OuterRow` (`$parent.x`): subsequent `Part::Field` names belong to the outer row, NOT
		///   the current table. Skip them; only walk nested predicates (which may reference `$this`
		///   legitimately).
		/// - `Opaque` (computed start, parameter-bound table): mark `has_opaque` and bail to "all
		///   fields needed."
		///
		/// Issue #7154 was about not treating `parent`/`this` as ordinary field
		/// names when they appear as `Part::Start(Expr::Param(...))`. Earlier
		/// fixes did that for the leading segment but still added subsequent
		/// segments under `$parent` to the needed set, inflating selective
		/// scans for the outer row's columns. The classifier-driven path
		/// below is the single source of truth.
		struct NeededFieldExtractor {
			fields: std::collections::HashSet<String>,
			has_opaque: bool,
			/// Depth of `Part::Where(...)` predicates currently being
			/// walked.
			///
			/// At depth 0 (top-level) the row scope follows the natural
			/// SELECT semantics: `$this` is the current row, `$parent` is
			/// the outer SELECT's row.
			///
			/// At depth 1 (inside one `[WHERE …]`) the runtime rebinds
			/// scope: `current_value` is the iteration element and
			/// `$parent` is bound to the enclosing `document_root`. So
			/// `.x` and `$this.x` reference the iteration element (NOT
			/// this row's columns), while `$parent.x` references the
			/// current row.
			///
			/// At depth ≥ 2 we don't model the chain and conservatively
			/// fall back to "all fields needed" via `has_opaque`.
			filter_depth: u32,
		}

		impl Visitor for NeededFieldExtractor {
			type Error = std::convert::Infallible;

			fn visit_idiom(&mut self, idiom: &crate::expr::Idiom) -> Result<(), Self::Error> {
				use super::row_scope::{IdiomRoot, classify_idiom_root};

				let root = classify_idiom_root(idiom);

				match (self.filter_depth, root) {
					// Top-level: standard scope.
					(0, IdiomRoot::OuterRow) => {
						// `$parent.X.Y...` — fields belong to the outer
						// row, not this row's table. Do not add them.
						// Visit nested parts (Where predicates, method
						// args) so any `$this`-rooted idioms inside
						// still contribute.
						for p in idiom.0.iter().skip(1) {
							self.visit_part(p)?;
						}
					}
					(0, IdiomRoot::ThisRow) => {
						// Idiom is rooted at the current row. The first
						// `Part::Field` (or the field immediately after
						// a `Part::Start(Expr::Param)` for `$this`/
						// `$self`) is a column name; add it. Visit
						// nested parts for predicates that may
						// reference further fields.
						//
						// `Part::Start` is intentionally NOT visited:
						// doing so would dispatch into its inner
						// `Expr::Param`, which the `visit_expr` arm
						// treats as opaque (a param could reference any
						// field) and mark the whole analysis as such —
						// losing the selective scan entirely. The
						// explicit `$this`/`$self` anchor is
						// structural; only the parts after it describe
						// this row's columns.
						let mut leading_field_taken = false;
						for part in idiom.0.iter() {
							match part {
								Part::Start(_) => continue,
								Part::Field(name) if !leading_field_taken => {
									self.fields.insert(name.as_str().to_owned());
									leading_field_taken = true;
								}
								_ => {}
							}
							self.visit_part(part)?;
						}
					}
					(0, IdiomRoot::Opaque) => {
						self.has_opaque = true;
					}
					// Inside one filter predicate (`[WHERE …]`): scope flips.
					(1, IdiomRoot::OuterRow) => {
						// `$parent.X.Y...` inside the filter — `$parent`
						// is rebound to the enclosing row, so `X` is a
						// column of *this* row's table.
						let mut leading_field_taken = false;
						for part in idiom.0.iter().skip(1) {
							match part {
								Part::Field(name) if !leading_field_taken => {
									self.fields.insert(name.as_str().to_owned());
									leading_field_taken = true;
								}
								_ => {}
							}
							self.visit_part(part)?;
						}
					}
					(1, IdiomRoot::ThisRow) => {
						// `.X` or `$this.X` inside the filter — these
						// access the iteration element, not this row's
						// columns. Don't add `X` to needed-fields.
						// Recurse into nested parts so a deeper
						// `$parent.Y` (e.g. inside a method-arg subexpr)
						// still contributes.
						for part in idiom.0.iter() {
							if matches!(part, Part::Start(_)) {
								continue;
							}
							self.visit_part(part)?;
						}
					}
					// Filter-scope Opaque (`(1, Opaque)`) or any idiom at
					// depth ≥ 2 (nested filters whose scope chain we don't
					// model). The `2..` lower bound on the catch-all means
					// a new IdiomRoot variant added later trips the
					// exhaustiveness checker instead of silently
					// short-circuiting here.
					(1, IdiomRoot::Opaque) | (2.., _) => {
						self.has_opaque = true;
					}
				}
				Ok(())
			}

			fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
				if matches!(part, Part::Where(_)) {
					self.filter_depth = self.filter_depth.saturating_add(1);
					let r = part.visit(self);
					self.filter_depth = self.filter_depth.saturating_sub(1);
					r
				} else {
					part.visit(self)
				}
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
			filter_depth: 0,
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

	/// Wrap a planned SELECT operator with the standard tail layers:
	/// `Timeout` (if a non-`NONE` TIMEOUT clause), `VersionScope` (if a
	/// VERSION expression), and `UnwrapExactlyOne` (if `FROM ONLY`).
	///
	/// `only_none_on_empty` controls `UnwrapExactlyOne::new`'s
	/// `none_on_empty` flag: `true` for table-source SELECTs (zero rows →
	/// `NONE`) and `false` for array-source SELECTs (zero rows → error).
	/// See [`UnwrapExactlyOne`] for the contract.
	///
	/// Every SELECT fast path (COUNT, indexed COUNT, literal-RecordId)
	/// closes with this helper so the tail is built in exactly one place.
	async fn wrap_select_tail(
		&self,
		op: Arc<dyn ExecOperator>,
		timeout: Expr,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		only: bool,
		only_none_on_empty: bool,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		let timed = match timeout {
			Expr::Literal(Literal::None) => op,
			te => {
				let tp = self.physical_expr(te).await?;
				Arc::new(Timeout::new(op, Some(tp))) as Arc<dyn ExecOperator>
			}
		};
		let versioned: Arc<dyn ExecOperator> = match version {
			Some(v) => Arc::new(VersionScope::new(timed, v)),
			None => timed,
		};
		if only {
			Ok(Arc::new(UnwrapExactlyOne::new(versioned, only_none_on_empty)))
		} else {
			Ok(versioned)
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
			mut what,
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
				Arc::new(CountScan::new(table_expr, version.clone(), field_names));
			return self.wrap_select_tail(count_scan, timeout, version, only, true).await;
		}

		// Indexed COUNT fast-path (COUNT with WHERE + matching COUNT index)
		// Skip when WITH NOINDEX is specified — the user explicitly forbids
		// index-assisted execution.
		//
		// SECURITY: also skip when the WHERE clause references a field whose
		// SELECT permission is not `Full`. The indexed-count fast paths
		// (`IndexCountScan` with either a dedicated `Index::Count` or a
		// covering B-tree access) count index entries directly, bypassing
		// the document-level field reduction that hides restricted values.
		// Without this guard, a record user could learn the cardinality of
		// field values they are not permitted to SELECT.
		if is_indexed_count_eligible(&fields, &group, &cond, &split, &order, &fetch, &omit, &what)
			&& !matches!(with, Some(crate::expr::with::With::NoIndex))
			&& !self.cond_touches_restricted_select_field(&what, &cond).await
		{
			// Try COUNT index first, then B-tree index for key-only counting.
			let has_count_idx = self.has_matching_count_index(&what, &cond).await;
			let btree_access = if !has_count_idx {
				self.resolve_count_btree_access(&what, &cond, with.as_ref()).await
			} else {
				None
			};

			if has_count_idx || btree_access.is_some() {
				use crate::exec::operators::scan::index_count::IndexCountScan;
				// `is_indexed_count_eligible` proves that `what` is non-empty
				// and `cond` is `Some`. Either invariant breaking would be a
				// planner bug rather than user input — surface it as
				// `Error::Internal` so the failure message points at the
				// drift, instead of letting a future eligibility-rule edit
				// silently turn into a panic on real queries.
				let table_first = what.first().cloned().ok_or_else(|| {
					Error::Internal(
						"indexed COUNT fast path: `is_indexed_count_eligible` returned true but `what` is empty".into(),
					)
				})?;
				let table_expr = self.physical_expr(table_first).await?;
				let condition = cond.clone().ok_or_else(|| {
					Error::Internal(
						"indexed COUNT fast path: `is_indexed_count_eligible` returned true but `cond` is None".into(),
					)
				})?;
				let predicate = self.physical_expr(condition.0.clone()).await?;
				let field_names = extract_count_field_names(&fields);
				let index_count_scan: Arc<dyn ExecOperator> = Arc::new(
					IndexCountScan::new(
						table_expr,
						predicate,
						condition,
						version.clone(),
						field_names,
					)
					.with_btree_access(btree_access),
				);
				return self.wrap_select_tail(index_count_scan, timeout, version, only, true).await;
			}
		}

		// Fast path: SELECT [*|fields] FROM <literal RecordId>
		//
		// This fast path plans projections through `self.plan_projections`
		// on the outer planner, which has no `version` field set. Idiom-side
		// optimisations that depend on the enclosing VERSION (notably
		// [`try_fast_path_pair`], which intentionally declines the
		// TargetVertex collapse for versioned queries to avoid bypassing
		// historical edge SELECT permissions) would silently miss the
		// version flag here. Fall through to the main pipeline when a
		// VERSION expression is present so idiom planning runs on a
		// version-aware inner planner.
		if what.len() == 1
			&& matches!(&what[0], Expr::Literal(Literal::RecordId(_)))
			&& cond.is_none()
			&& order.is_none()
			&& group.is_none()
			&& split.is_none()
			&& fetch.is_none()
			&& with.is_none()
			&& version.is_none()
		{
			let needed_fields = Self::extract_needed_fields(
				&fields,
				&omit,
				cond.as_ref(),
				order.as_ref(),
				group.as_ref(),
				split.as_ref(),
			);

			// Extract the literal RecordId once and reuse — the outer guard
			// (`what.len() == 1 && matches!(&what[0], Expr::Literal(Literal::RecordId(_)))`)
			// already proved this shape, but pattern-matching the owned value
			// in a single place keeps the invariant local. If the guard
			// changes and this destructure desynchronizes, the explicit
			// `Error::Internal` fires with a useful message rather than an
			// `unreachable!` panic.
			let rid_lit = match what.into_iter().next() {
				Some(Expr::Literal(Literal::RecordId(rid_lit))) => rid_lit,
				_ => {
					return Err(Error::Internal(
						"literal RecordId fast path entered without a literal RecordId source"
							.into(),
					));
				}
			};
			let table_name_for_resolve = Some(rid_lit.table.clone());
			let rid_expr = self.physical_expr(Expr::Literal(Literal::RecordId(rid_lit))).await?;
			let resolved_table_ctx: Option<ResolvedTableContext> =
				if let Some(ref tb) = table_name_for_resolve {
					self.try_resolve_table_ctx(tb).await
				} else {
					None
				};
			let pdf = self.pre_decode_filter_status_for(
				resolved_table_ctx.as_ref(),
				None,
				needed_fields.as_ref(),
			);
			let mut scan = RecordIdScan::new(rid_expr, version.clone(), needed_fields, None);
			if let Some(tc) = resolved_table_ctx {
				scan = scan.with_resolved(tc);
			}
			scan = scan.with_pre_decode_filter(pdf);
			let scan: Arc<dyn ExecOperator> = Arc::new(scan);
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
			let projected = self.plan_projections(fields, omit, limited).await?;
			return self.wrap_select_tail(projected, timeout, version, only, true).await;
		}

		// Capture literal Expr::Table nodes BEFORE resolve_source_exprs so
		// that MATCHES context preferentially binds to tables written in the
		// query rather than param-resolved ones (e.g. FROM $t, article).
		let literal_primary_table = what.iter().find_map(|e| match e {
			Expr::Table(t) => Some(t.clone()),
			_ => None,
		});

		// Pre-resolve FROM sources so that params and function calls like
		// type::table($name) are rewritten to concrete Expr::Table nodes
		// before any downstream checks.
		self.resolve_source_exprs(&mut what).await;

		let is_value_source = all_value_sources(&what);
		let primary_table = literal_primary_table.or_else(|| {
			what.iter().find_map(|e| match e {
				Expr::Table(t) => Some(t.clone()),
				_ => None,
			})
		});
		let has_knn_early = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));

		let planning_ctx: std::borrow::Cow<'_, crate::ctx::FrozenContext> =
			if let Some(ref c) = cond {
				let mc = extract_matches_context(c, Some(self.ctx));
				let hm = !mc.is_empty();
				if hm || has_knn_early {
					let mut child = crate::ctx::Context::new_child(self.ctx);
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

		// Propagate txn, version, auth, and cycle guard to the inner
		// planner. Inheriting the cycle guard means a self-referential
		// permission or computed-field body whose subquery flows through
		// this nested planner will see the parent's in-progress tables
		// and fall back. Inheriting auth keeps fast-path eligibility
		// decisions consistent with the outer statement.
		let mut pp = if let Some(ref txn) = self.txn {
			Planner::with_txn(&planning_ctx, Arc::clone(txn), self.ns.clone(), self.db.clone())
		} else {
			Planner::new(&planning_ctx)
		}
		.with_version(version.clone())
		.with_cycle_guard(self.cycle_guard());
		if let Some(ref auth) = self.auth {
			pp = pp.with_auth(Arc::clone(auth));
		}

		let needed_fields = Self::extract_needed_fields(
			&fields,
			&omit,
			cond.as_ref(),
			order.as_ref(),
			group.as_ref(),
			split.as_ref(),
		);
		// After source resolution, params that resolved to tables are now
		// Expr::Table, so we only need to check concrete types here.
		let source_is_single_scan = what.len() == 1
			&& matches!(what[0], Expr::Table(_) | Expr::FunctionCall(_) | Expr::Postfix { .. });

		// Resolve bind-parameter references so that downstream index analysis
		// and KNN extraction see Expr::Literal instead of Expr::Param.
		// This covers LET bindings, client bind params, and DEFINE PARAM.
		let ns_db = self.ns_db_ids().await;
		let cond = match cond.as_ref() {
			Some(c) => {
				Some(resolve_condition_params(c, self.ctx, ns_db, SELECT_ITERATION_PARAMS).await)
			}
			None => None,
		};

		// Fold constant expressions to literals so that index analysis can
		// create proper range access patterns. Handles:
		// - time::now() - 365d → datetime literal
		// - math::floor(20.5) → 20 (any pure function with literal args)
		// - type::int('42') → 42
		let cond = match cond {
			Some(mut c) => {
				fold_condition_expressions(&mut c, self.function_registry());
				Some(c)
			}
			None => None,
		};

		// KNN handling
		let has_knn = cond.as_ref().is_some_and(|c| has_knn_operator(&c.0));
		let brute_force_knn = if has_knn {
			cond.as_ref().and_then(extract_bruteforce_knn)
		} else {
			None
		};

		let (cond_for_index, cond_for_filter) = if has_knn {
			let stripped = cond.as_ref().and_then(strip_knn_from_condition);
			if let Some(c) = stripped.as_ref()
				&& has_knn_operator(&c.0)
			{
				// `strip_knn_from_condition` removes `K` and `Approximate` from
				// the top-level AND chain; whatever is left is either a KTree
				// variant (no longer backed by any index) or a `K`/`Approximate`
				// nested under OR/NOT. Disambiguate so the message is actionable.
				let message = if has_knn_ktree_operator(&c.0) {
					"The `<|k|>` KNN operator (KTree / M-Tree) is no longer supported. \
					 Use `<|k, EF|>` against an HNSW index (e.g. \
					 `DEFINE INDEX … HNSW DIMENSION N`), or `<|k, DISTANCE|>` for a \
					 brute-force KNN with an explicit distance metric."
						.to_string()
				} else {
					"KNN operators must appear at the top level of the WHERE clause \
					 (joined with AND); nesting `<|k, …|>` inside OR or NOT is not \
					 supported."
						.to_string()
				};
				return Err(Error::Query {
					message,
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

		// Tell source planning whether the downstream pipeline contains a
		// bounded top-k sort. Used by UnionIndexScan to skip eager
		// per-sub-stream prefetch — the heap discards most rows anyway,
		// so prefetching only wastes memory under high concurrency.
		let downstream_topk = is_bounded_topk_downstream(
			order.as_ref(),
			&start,
			&limit,
			tempfiles,
			self.ctx.config.max_order_limit_priority_queue_size as usize,
		);

		// Source resolution with plan-time index analysis.
		// The result tracks whether the predicate and limit/start were
		// consumed by the source operator, so we can avoid duplicating
		// them in the outer pipeline.
		let mut planned = pp
			.plan_sources(
				what,
				version.clone(),
				cond_for_index.as_ref(),
				order.as_ref(),
				with.as_ref(),
				needed_fields,
				scan_predicate,
				scan_limit,
				scan_start,
				downstream_topk,
			)
			.await?;

		if can_soft_push_limit {
			planned.limit_pushed = false;
		}

		// Resolve the pipeline's WHERE state from the source's filter action.
		// - FullyConsumed: source handles the entire predicate, no Filter.
		// - Residual: only the residual part needs a Filter.
		// - UseOriginal: source didn't analyze the predicate; reuse the already-compiled predicate
		//   when available to avoid recompiling.
		let where_clause = match planned.filter_action {
			FilterAction::FullyConsumed => WhereClauseState::None,
			FilterAction::Residual(residual) => WhereClauseState::Original(residual),
			FilterAction::UseOriginal => {
				if let Some(pred) = scan_predicate_for_reuse {
					WhereClauseState::Precompiled(pred)
				} else if let Some(c) = cond_for_filter {
					WhereClauseState::Original(c)
				} else {
					WhereClauseState::None
				}
			}
		};

		// KNN wrapping. Residual predicates (non-KNN WHERE conditions) must
		// be applied BEFORE ranking by distance. Otherwise rows that don't
		// satisfy the WHERE clause can consume top-K slots and push out
		// valid rows.
		//
		let (source, where_clause) = if let Some(kp) = brute_force_knn {
			let input = match &where_clause {
				WhereClauseState::Original(c) => {
					let pred = pp.physical_expr(c.0.clone()).await?;
					Arc::new(Filter::new(planned.operator, pred)) as Arc<dyn ExecOperator>
				}
				WhereClauseState::Precompiled(predicate) => {
					Arc::new(Filter::new(planned.operator, Arc::clone(predicate)))
						as Arc<dyn ExecOperator>
				}
				WhereClauseState::None => planned.operator,
			};
			let knn_ctx = planning_ctx.get_knn_context().cloned();
			let wrapped = Arc::new(
				KnnTopK::new(input, kp.field, kp.vector, kp.k as usize, kp.distance)
					.with_knn_context(knn_ctx),
			) as Arc<dyn ExecOperator>;
			(wrapped, WhereClauseState::None)
		} else {
			(planned.operator, where_clause)
		};

		// Build pipeline.
		// When limit/start were pushed, omit them to avoid double application.
		// ORDER BY is always passed through — sort elimination via
		// `can_eliminate_sort()` in `plan_pipeline()` handles it independently.
		let config = SelectPipelineConfig {
			where_clause,
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
			tempfiles,
		};

		let projected = pp.plan_pipeline(source, Some(fields), config).await?;
		let fetched = pp.plan_fetch(fetch, projected).await?;
		pp.wrap_select_tail(fetched, timeout, version, only, !is_value_source).await
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
		downstream_topk: bool,
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
					downstream_topk,
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
		downstream_topk: bool,
	) -> Result<PlannedSource, Error> {
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
			let filter_action = filter_action_for_predicate(&scan_predicate);
			let record_id_expr = self.physical_expr(rid_expr).await?;
			let resolved_table_ctx: Option<ResolvedTableContext> =
				self.try_resolve_table_ctx(table_name).await;
			let pdf = self.pre_decode_filter_status_for(
				resolved_table_ctx.as_ref(),
				scan_predicate.as_ref(),
				needed_fields.as_ref(),
			);
			let mut scan =
				RecordIdScan::new(record_id_expr, version, needed_fields, scan_predicate.clone());
			if let Some(tc) = resolved_table_ctx {
				scan = scan.with_resolved(tc);
			}
			scan = scan.with_pre_decode_filter(pdf);
			return Ok(PlannedSource {
				operator: Arc::new(scan) as Arc<dyn ExecOperator>,
				filter_action,
				limit_pushed: false,
			});
		}

		// When we have a txn and the source is a table, resolve the
		// access path and table context at plan time.
		if let Expr::Table(ref table_name) = expr
			&& let (Some(txn), Some(ns), Some(db)) = (&self.txn, &self.ns, &self.db)
		{
			// Resolve table context (table def + field state) at plan time.
			// This eliminates runtime KV lookups in the operator's execute().
			let table_ctx: Option<ResolvedTableContext> =
				self.try_resolve_table_ctx(table_name).await;

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
						return self
							.plan_btree_scan_source(
								table,
								index_ref,
								access,
								direction,
								cond,
								order,
								scan_predicate,
								scan_limit,
								scan_start,
								needed_fields,
								version,
								table_ctx,
							)
							.await;
					}
					AccessPath::FullTextSearch {
						index_ref,
						query,
						operator,
					} => {
						return self
							.plan_fulltext_search_source(
								table,
								index_ref,
								query,
								operator,
								cond,
								needed_fields,
								version,
								table_ctx,
							)
							.await;
					}
					AccessPath::KnnSearch {
						index_ref,
						vector,
						k,
						ef,
					} => {
						return self
							.plan_knn_search_source(
								table,
								index_ref,
								vector,
								k,
								ef,
								cond,
								needed_fields,
								version,
								table_ctx,
								knn_ctx,
							)
							.await;
					}
					AccessPath::TableScan => {
						return self
							.plan_table_scan_source(
								table,
								direction,
								order,
								scan_predicate,
								scan_limit,
								scan_start,
								needed_fields,
								version,
								table_ctx,
							)
							.await;
					}
					AccessPath::Union {
						paths,
						dedupe,
					} => {
						return self
							.plan_union_index_source(
								table,
								paths,
								dedupe,
								order,
								scan_limit,
								cond,
								needed_fields,
								version,
								table_ctx,
								knn_ctx,
								downstream_topk,
							)
							.await;
					}
					AccessPath::EmptyScan => {
						return Ok(Self::plan_empty_source());
					}
				}
			}
		}

		// Fallback: create the appropriate operator (index resolved at runtime)
		let knn_ctx = self.ctx.get_knn_context().cloned();

		match expr {
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
			// Params that could be resolved were already rewritten to
			// Expr::Table / Expr::Literal by resolve_source_exprs().
			// Any remaining Expr::Param is unresolvable at plan time.
			Expr::Param(_) => {
				let phys_expr = self.physical_expr(expr).await?;
				Ok(PlannedSource {
					operator: Arc::new(SourceExpr::new(phys_expr)) as Arc<dyn ExecOperator>,
					filter_action: FilterAction::UseOriginal,
					limit_pushed: false,
				})
			}
			Expr::Table(_)
			| Expr::FunctionCall(_)
			| Expr::Postfix {
				..
			} => {
				self.plan_dynamic_scan(
					expr,
					version,
					cond,
					order,
					with,
					needed_fields,
					scan_predicate,
					scan_limit,
					scan_start,
					knn_ctx,
				)
				.await
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

	// ========================================================================
	// Per-access-path source planners (called by plan_source)
	// ========================================================================

	/// Build a `IndexScan` for [`AccessPath::BTreeScan`], computing the
	/// filter action (index-covered conditions stripped), limit/start
	/// pushdown when the index ordering covers the ORDER BY, and an
	/// optional batch-ceiling hint for residual filters.
	#[allow(clippy::too_many_arguments)]
	async fn plan_btree_scan_source(
		&self,
		table: crate::val::TableName,
		index_ref: crate::exec::index::access_path::IndexRef,
		access: BTreeAccess,
		direction: crate::idx::planner::ScanDirection,
		cond: Option<&Cond>,
		order: Option<&crate::expr::order::Ordering>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		needed_fields: Option<std::collections::HashSet<String>>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::IndexScan;

		// Strip index-covered conditions from the WHERE clause. If all
		// conditions are consumed, no Filter operator will be created in
		// the pipeline.
		let filter_action = if let Some(c) = cond {
			match strip_index_conditions(c, &access, &index_ref.cols) {
				None => FilterAction::FullyConsumed,
				Some(residual) => FilterAction::Residual(residual),
			}
		} else {
			FilterAction::FullyConsumed
		};
		// Push limit to IndexScan when the index ordering covers ORDER BY
		// (or there is no ORDER BY).
		//
		// IMPORTANT: only push when the filter is fully consumed by the
		// index. With a residual filter above the scan, pushing LIMIT
		// would make the scan return fewer rows than needed because the
		// post-filter may remove some of them.
		let order_covered =
			|| order.is_none_or(|ord| index_covers_ordering(&index_ref, &access, direction, ord));
		let push = scan_limit.is_some()
			&& matches!(filter_action, FilterAction::FullyConsumed)
			&& order_covered();
		// `scan_limit` flows to exactly one destination: pushed into the
		// scan when `push` is true (avoids reapplying LIMIT above),
		// otherwise used as a per-batch sizing hint when the index
		// already covers ORDER BY but a residual filter prevents real
		// LIMIT pushdown. Move it once instead of cloning.
		let (idx_limit, idx_start, limit_pushed, batch_ceiling) = if push {
			(scan_limit, scan_start, true, None)
		} else if let Some(sl) = scan_limit
			&& matches!(filter_action, FilterAction::Residual(_))
			&& order_covered()
		{
			(None, None, false, Some(sl))
		} else {
			(None, None, false, None)
		};
		// Only thread the compiled WHERE into IndexScan when there is no
		// outer Filter for the same condition. For `Residual`, the
		// pipeline adds a Filter above the scan; applying `scan_predicate`
		// again here would duplicate work and skew EXPLAIN row accounting.
		let index_where_predicate = match &filter_action {
			FilterAction::FullyConsumed => scan_predicate,
			FilterAction::Residual(_) | FilterAction::UseOriginal => None,
		};
		let mut scan = IndexScan::new(
			index_ref,
			access,
			direction,
			table,
			idx_limit,
			idx_start,
			version,
			Some(needed_fields),
			index_where_predicate,
		)
		.with_batch_ceiling(batch_ceiling);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
		})
	}

	/// Build a `FullTextScan` for [`AccessPath::FullTextSearch`]. The
	/// MATCHES predicate is stripped from the WHERE clause; any residual
	/// non-MATCHES conditions are returned in the filter action.
	#[allow(clippy::too_many_arguments)]
	async fn plan_fulltext_search_source(
		&self,
		table: crate::val::TableName,
		index_ref: crate::exec::index::access_path::IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		cond: Option<&Cond>,
		needed_fields: Option<std::collections::HashSet<String>>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::FullTextScan;

		let mut scan =
			FullTextScan::new(index_ref, query, operator, table, version, Some(needed_fields));
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		let filter_action = if let Some(c) = cond {
			match strip_fts_condition(c) {
				None => FilterAction::FullyConsumed,
				Some(residual) => FilterAction::Residual(residual),
			}
		} else {
			FilterAction::FullyConsumed
		};
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed: false,
		})
	}

	/// Build a `KnnScan` for [`AccessPath::KnnSearch`]. KNN operators are
	/// stripped from the condition; the residual non-KNN predicates are
	/// pushed into the HNSW search so non-matching rows don't consume
	/// top-K slots.
	#[allow(clippy::too_many_arguments)]
	async fn plan_knn_search_source(
		&self,
		table: crate::val::TableName,
		index_ref: crate::exec::index::access_path::IndexRef,
		vector: Vec<crate::val::Number>,
		k: u32,
		ef: u32,
		cond: Option<&Cond>,
		needed_fields: Option<std::collections::HashSet<String>>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Result<PlannedSource, Error> {
		use crate::exec::operators::KnnScan;

		let residual_cond = cond.and_then(strip_knn_from_condition);
		let mut scan = KnnScan::new(
			index_ref,
			vector,
			k,
			ef,
			table,
			version,
			knn_ctx,
			residual_cond,
			Some(needed_fields),
		);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action: FilterAction::UseOriginal,
			limit_pushed: false,
		})
	}

	/// Build a `EmptyScan` for [`AccessPath::EmptyScan`] — used when the
	/// analyzer proved the WHERE clause cannot match any rows (e.g. a
	/// contradictory range or empty `IN []`). Returns a `PlannedSource`
	/// that reports the predicate as fully consumed and the limit as
	/// pushed, so the outer pipeline does not add a Filter or Limit.
	fn plan_empty_source() -> PlannedSource {
		use crate::exec::operators::EmptyScan;
		PlannedSource {
			operator: Arc::new(EmptyScan::new()) as Arc<dyn ExecOperator>,
			filter_action: FilterAction::FullyConsumed,
			limit_pushed: true,
		}
	}

	/// Build a `TableScan` for [`AccessPath::TableScan`] — the fallback
	/// when no index access path covers the query. Pushes LIMIT/START
	/// only when ORDER BY is compatible with the natural KV scan
	/// direction (i.e. plain `id` ASC/DESC or absent).
	#[allow(clippy::too_many_arguments)]
	async fn plan_table_scan_source(
		&self,
		table: crate::val::TableName,
		direction: crate::idx::planner::ScanDirection,
		order: Option<&crate::expr::order::Ordering>,
		scan_predicate: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		scan_start: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		needed_fields: Option<std::collections::HashSet<String>>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
	) -> Result<PlannedSource, Error> {
		let filter_action = filter_action_for_predicate(&scan_predicate);
		let push = scan_limit.is_some() && order_is_scan_compatible(order);
		let (tbl_limit, tbl_start, limit_pushed) = if push {
			(scan_limit, scan_start, true)
		} else {
			(None, None, false)
		};
		let pdf = self.pre_decode_filter_status_for(
			table_ctx.as_ref(),
			scan_predicate.as_ref(),
			needed_fields.as_ref(),
		);
		let mut scan = TableScan::new(
			table,
			direction,
			version,
			scan_predicate,
			tbl_limit,
			tbl_start,
			needed_fields,
		);
		if let Some(tc) = table_ctx {
			scan = scan.with_resolved(tc);
		}
		scan = scan.with_pre_decode_filter(pdf);
		Ok(PlannedSource {
			operator: Arc::new(scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
		})
	}

	/// Build a `UnionIndexScan` for [`AccessPath::Union`] — one
	/// sub-operator per OR branch (or per IN-expansion / containment
	/// branch).  When every sub-path is an equality B-tree scan and
	/// ORDER BY is `id ASC/DESC` only, enables k-way merge-by-id so the
	/// Sort operator can be eliminated.
	///
	/// `dedupe` records whether branches can overlap on the same record
	/// — see [`AccessPath::Union`] for the contract. Drives the
	/// `ByIndexKey` vs `ByIndexKeyDedup` merge-mode selection when an
	/// ordered k-way merge is active.
	#[allow(clippy::too_many_arguments)]
	async fn plan_union_index_source(
		&self,
		table: crate::val::TableName,
		paths: Vec<AccessPath>,
		dedupe: bool,
		order: Option<&crate::expr::order::Ordering>,
		scan_limit: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		cond: Option<&Cond>,
		needed_fields: Option<std::collections::HashSet<String>>,
		version: Option<Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<ResolvedTableContext>,
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
		downstream_topk: bool,
	) -> Result<PlannedSource, Error> {
		// Enable merge-sort by record ID when ORDER BY is `id ASC/DESC`
		// only and every sub-path is an equality B-tree scan (each one
		// already produces records in record-ID order).
		let merge_dir = detect_order_by_id_only(order).filter(|_| {
			paths.iter().all(|p| {
				matches!(
					p,
					AccessPath::BTreeScan {
						access: BTreeAccess::Equality(_),
						..
					}
				)
			})
		});

		// If we couldn't merge-by-id, try the composite-index k-way merge:
		// every branch pins the same composite-index prefix and the ORDER
		// BY column is the next column of that index. Each branch is then
		// already sorted by the ORDER BY column (in the scan's direction).
		let merge_by_index_key = if merge_dir.is_none() {
			detect_order_for_composite_union(order, &paths)
		} else {
			None
		};

		// When the merge-by-index-key opportunity applies but a branch's
		// scan direction doesn't match the ORDER BY direction (this happens
		// when `try_in_expansion` used the default direction because the
		// chosen ORDER BY isn't on `id`), rewrite each branch to scan in
		// the correct direction. Each per-branch sub-scan must yield rows
		// already sorted by the suffix column in the merge's direction.
		let paths = if let Some((_, dir)) = &merge_by_index_key {
			let target = match dir {
				SortDirection::Asc => crate::idx::planner::ScanDirection::Forward,
				SortDirection::Desc => crate::idx::planner::ScanDirection::Backward,
			};
			paths
				.into_iter()
				.map(|p| match p {
					AccessPath::BTreeScan {
						index_ref,
						access,
						..
					} => AccessPath::BTreeScan {
						index_ref,
						access,
						direction: target,
					},
					other => other,
				})
				.collect::<Vec<_>>()
		} else {
			paths
		};

		// `dedupe` is the explicit contract set by the analyser: `true`
		// for OR-union and CONTAINS-on-array (branches can overlap on
		// the same record), `false` for scalar `IN`-expansion (each
		// row's field equals at most one literal). Drives the merge
		// variant choice below.

		// When merge mode is active and a downstream LIMIT exists, pass
		// it as a batch-ceiling hint to each sub-scan so the merge
		// terminates quickly.
		let merge_active = merge_dir.is_some() || merge_by_index_key.is_some();
		let merge_batch_ceiling = if merge_active {
			scan_limit
		} else {
			None
		};

		// Strip CONTAINSANY / ANYINSIDE leaves whose literal value set
		// is fully covered by the branches' prefix values.  If
		// everything is covered, the residual Filter goes away and
		// LIMIT can be pushed into the sub-scans (when merge is
		// active).  CONTAINSALL / ALLINSIDE leaves are NOT stripped
		// (intersection semantics; see `strip_union_index_conditions`
		// rustdoc and issue #236).
		//
		// SECURITY: stripping is also disabled when the WHERE clause
		// references a field whose SELECT permission is not `Full`.
		// `UnionIndexScan` sub-operators run a `ScanPipeline` with no
		// predicate (the union dedupes/merges and only the outer Filter
		// re-applies the WHERE). Field-level permissions then wipe the
		// restricted value from each document before it would have been
		// matched — but a stripped leaf is never re-evaluated, so the
		// index entries themselves become a membership oracle for the
		// hidden value. Leaving the leaf in the residual filter forces
		// the post-permission recheck and closes that channel.
		let filter_action = if let Some(c) = cond {
			let strip_safe = !self.cond_touches_restricted_select_field_for_table(&table, c).await;
			let stripped = if strip_safe {
				strip_union_index_conditions(c, &paths)
			} else {
				Some(c.clone())
			};
			match stripped {
				None => FilterAction::FullyConsumed,
				Some(residual) => FilterAction::Residual(residual),
			}
		} else {
			FilterAction::FullyConsumed
		};

		let mut sub_operators: Vec<Arc<dyn ExecOperator>> = Vec::with_capacity(paths.len());
		for path in paths {
			sub_operators.push(self.build_union_sub_operator(
				path,
				&table,
				cond,
				version.as_ref(),
				table_ctx.as_ref(),
				knn_ctx.as_ref(),
				merge_batch_ceiling.as_ref(),
			)?);
		}

		// UnionIndexScan handles field-level permissions and computed-field
		// materialization internally; the outer pipeline handles Filter,
		// Sort, and Limit.
		let mut union_scan = UnionIndexScan::new(table, sub_operators, needed_fields);
		if let Some(dir) = merge_dir {
			union_scan = union_scan.with_merge_by_id(dir);
		} else if let Some((path, dir)) = merge_by_index_key {
			// Composite-index k-way merge by the indexed sort column.
			// Use the deduping variant when the analyser flagged
			// branches as overlap-prone (see `AccessPath::Union::dedupe`).
			if dedupe {
				union_scan = union_scan.with_merge_by_index_key_dedup(path, dir);
			} else {
				union_scan = union_scan.with_merge_by_index_key(path, dir);
			}
		} else if downstream_topk {
			// No merge available — but a bounded top-k sort is downstream.
			// Skip eager per-sub-stream prefetch so the heap drives demand.
			union_scan = union_scan.with_downstream_topk();
		}
		if let Some(tc) = table_ctx {
			union_scan = union_scan.with_resolved(tc);
		}

		// `limit_pushed` stays `false` for unions: the per-sub-scan
		// `merge_batch_ceiling` only sizes the per-batch fetch, not the
		// total row count.  Cancellation is driven by the outer `Limit`
		// operator pulling N rows and dropping the stream — the
		// streaming pipeline cancels the union and its sub-scans on
		// drop.  Removing the outer `Limit` (as `limit_pushed: true`
		// would do) drops LIMIT semantics entirely on this path.
		Ok(PlannedSource {
			operator: Arc::new(union_scan) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed: false,
		})
	}

	/// Build one sub-operator for a `UnionIndexScan`. Each sub-operator
	/// is a per-OR-branch scan without computed-field materialization or
	/// `needed_fields` — those are handled at the union level.
	///
	/// `select_access_path` only emits `BTreeScan` / `FullTextSearch` /
	/// `KnnSearch` as union sub-paths; anything else is a planner bug
	/// and surfaces as `Error::Internal` rather than silently returning a
	/// full table scan.
	#[allow(clippy::too_many_arguments)]
	fn build_union_sub_operator(
		&self,
		path: AccessPath,
		table: &crate::val::TableName,
		cond: Option<&Cond>,
		version: Option<&Arc<dyn crate::exec::PhysicalExpr>>,
		table_ctx: Option<&ResolvedTableContext>,
		knn_ctx: Option<&Arc<crate::exec::function::KnnContext>>,
		merge_batch_ceiling: Option<&Arc<dyn crate::exec::PhysicalExpr>>,
	) -> Result<Arc<dyn ExecOperator>, Error> {
		use crate::exec::operators::{FullTextScan, IndexScan, KnnScan};

		match path {
			AccessPath::BTreeScan {
				index_ref,
				access,
				direction,
			} => {
				let mut scan = IndexScan::new(
					index_ref,
					access,
					direction,
					table.clone(),
					None,
					None,
					version.cloned(),
					None,
					None,
				);
				if let Some(ceiling) = merge_batch_ceiling {
					scan = scan.with_batch_ceiling(Some(Arc::clone(ceiling)));
				}
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone());
				}
				Ok(Arc::new(scan))
			}
			AccessPath::FullTextSearch {
				index_ref,
				query,
				operator,
			} => {
				let mut scan = FullTextScan::new(
					index_ref,
					query,
					operator,
					table.clone(),
					version.cloned(),
					None,
				);
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone());
				}
				Ok(Arc::new(scan))
			}
			AccessPath::KnnSearch {
				index_ref,
				vector,
				k,
				ef,
			} => {
				let residual_cond = cond.and_then(strip_knn_from_condition);
				let mut scan = KnnScan::new(
					index_ref,
					vector,
					k,
					ef,
					table.clone(),
					version.cloned(),
					knn_ctx.cloned(),
					residual_cond,
					None,
				);
				if let Some(tc) = table_ctx {
					scan = scan.with_resolved(tc.clone());
				}
				Ok(Arc::new(scan))
			}
			other => {
				// Server-side log carries the Debug-formatted access path
				// for diagnosis; the client-facing message stays opaque
				// so internal access-path details don't leak.
				tracing::error!(
					path = ?other,
					"UnionIndexScan sub-path produced an unexpected access path"
				);
				Err(Error::Internal(
					"UnionIndexScan sub-path produced an unexpected access path; \
					 only BTreeScan / FullTextSearch / KnnSearch are valid here"
						.into(),
				))
			}
		}
	}

	/// Plan a `DynamicScan` source that resolves its access path at runtime.
	///
	/// Used for `FROM type::table(...)`, `FROM $param` (when the param holds a
	/// table), and the `FROM tablename` fallback when plan-time catalog context
	/// is unavailable. Handles filter-action, limit pushdown with ORDER BY
	/// compatibility, and KNN context in a single place.
	#[allow(clippy::too_many_arguments)]
	async fn plan_dynamic_scan(
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
		knn_ctx: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Result<PlannedSource, Error> {
		let filter_action = filter_action_for_predicate(&scan_predicate);
		let push = scan_limit.is_some() && order_is_scan_compatible(order);
		let (dyn_limit, dyn_start, limit_pushed) = if push {
			(scan_limit, scan_start, true)
		} else {
			(None, None, false)
		};
		let resolved_table_ctx: Option<ResolvedTableContext> =
			if let Expr::Table(table_name) = &expr {
				self.try_resolve_table_ctx(table_name).await
			} else {
				None
			};
		let source_expr = self.physical_expr(expr).await?;
		let pdf = self.pre_decode_filter_status_for(
			resolved_table_ctx.as_ref(),
			scan_predicate.as_ref(),
			needed_fields.as_ref(),
		);
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
					dyn_limit,
					dyn_start,
				)
				.with_knn_context(knn_ctx)
				.with_pre_decode_filter(pdf),
			) as Arc<dyn ExecOperator>,
			filter_action,
			limit_pushed,
		})
	}

	/// Compute the plan-time [`PreDecodeFilterStatus`] for a KV scan from an optional resolved
	/// table context, the scan's WHERE predicate (if any), and the scan's projected fields.
	///
	/// Centralises the field-state projection and call into
	/// [`pre_decode_filter_status_at_plan_time`] used by [`TableScan`], [`DynamicScan`] and
	/// [`RecordIdScan`] planning paths.
	fn pre_decode_filter_status_for(
		&self,
		table_ctx: Option<&ResolvedTableContext>,
		predicate: Option<&Arc<dyn crate::exec::PhysicalExpr>>,
		needed_fields: Option<&std::collections::HashSet<String>>,
	) -> crate::exec::pre_decode_filter::PreDecodeFilterStatus {
		let projected = table_ctx.map(|tc| tc.field_state_for_projection(needed_fields));
		// `idiom_recursion_limit` is shared with the SurrealQL `RECURSE` /
		// `@@` planner (`compute_idiom_recursion`); reusing it caps both
		// the SurrealQL-level recursion and the pre-decode walker descent
		// at a single, user-configurable limit (default 256).
		pre_decode_filter_status_at_plan_time(
			predicate,
			projected.as_ref(),
			self.ctx.config.idiom_recursion_limit,
		)
	}

	/// Try to resolve a `ResolvedTableContext` for the given table.
	///
	/// Returns `None` if namespace/database lookup fails or the table doesn't
	/// exist. Errors in field state resolution are silently ignored (the
	/// operator will fall back to runtime resolution). Catalog-lookup failures
	/// are logged at debug level so the silent fallback is observable.
	async fn try_resolve_table_ctx(&self, table_name: &TableName) -> Option<ResolvedTableContext> {
		let ns = self.ns()?;
		let db = self.db()?;
		// Single cached catalog read for (ns_id, db_id); `ns_db_ids`
		// memoises across `resolve_param`, plan-time predicate folding,
		// and the per-source resolution path that this function backs.
		let (ns_id, db_id) = self.ns_db_ids().await?;

		// Cycle check: if this table is already being resolved on a parent
		// stack frame (typically a self-referential permission predicate
		// like `WHERE (SELECT FROM same_table) != NONE`), fall back to
		// runtime resolution for this subtree. The entry's `Drop` impl
		// pops the table when this function returns.
		let _entry = match self.cycle_guard().try_enter(ns_id, db_id, table_name.clone()) {
			Some(e) => e,
			None => {
				tracing::debug!(
					ns = %ns,
					db = %db,
					table = %table_name,
					"plan-time cycle detected; falling back to runtime resolution",
				);
				return None;
			}
		};

		match resolve_table_context(self, ns, db, ns_id, db_id, table_name).await {
			Ok(opt) => opt,
			Err(e) => {
				tracing::warn!(
					ns = %ns,
					db = %db,
					table = %table_name,
					error = %e,
					"plan-time table-context resolution failed; falling back to runtime",
				);
				None
			}
		}
	}

	/// Check at plan time whether a matching COUNT index exists for the query.
	///
	/// Returns `true` when:
	/// - Plan-time catalog access is available (txn, ns, db)
	/// - The source is a single table
	/// - The table has a `DEFINE INDEX ... COUNT WHERE <cond>` whose condition matches the query's
	///   WHERE clause
	async fn has_matching_count_index(&self, what: &[Expr], cond: &Option<Cond>) -> bool {
		let table_name = match what.first() {
			Some(Expr::Table(t)) => t,
			_ => return false,
		};
		let cond = match cond {
			Some(c) => c,
			None => return false,
		};
		let Some(txn) = self.txn.as_ref() else {
			return false;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return false;
		};
		let indexes = match txn.all_tb_indexes(ns_id, db_id, table_name, None).await {
			Ok(idx) => idx,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time index list failed in has_matching_count_index; assuming no count index",
				);
				return false;
			}
		};
		// COUNT fast paths must not use an index until the durable build
		// protocol has published it as online.
		let Ok(indexes) = filter_online_indexes(txn, ns_id, db_id, indexes).await else {
			return false;
		};
		indexes.iter().any(|ix| {
			if let Index::Count(ref idx_cond) = ix.index {
				idx_cond.as_ref() == Some(cond)
			} else {
				false
			}
		})
	}

	/// Returns true when permissions are being enforced for this plan AND
	/// the WHERE clause references a field whose SELECT permission on the
	/// target table is not `Full`.
	///
	/// Thin convenience wrapper for the indexed COUNT fast-path call site,
	/// which already has `(&[Expr], &Option<Cond>)` on hand: it extracts
	/// the target `TableName` from `what`, short-circuits on `None`
	/// conditions or non-table sources, then delegates to
	/// [`Self::cond_touches_restricted_select_field_for_table`]. Other
	/// call sites (e.g. the `UnionIndexScan` planner) that already hold a
	/// resolved `TableName` and `Cond` should call the `_for_table`
	/// helper directly.
	///
	/// The indexed COUNT fast paths (`IndexCountScan` over a dedicated
	/// `Index::Count` or a covering B-tree index) count index entries
	/// without going through the scan pipeline that applies field-level
	/// SELECT permissions. If the predicate references a restricted field,
	/// returning the materialised count leaks the cardinality of values the
	/// current user is not permitted to read.
	async fn cond_touches_restricted_select_field(
		&self,
		what: &[Expr],
		cond: &Option<Cond>,
	) -> bool {
		let Some(cond) = cond else {
			return false;
		};
		let table_name = match what.first() {
			Some(Expr::Table(t)) => t,
			_ => return false,
		};
		self.cond_touches_restricted_select_field_for_table(table_name, cond).await
	}

	/// Core of [`Self::cond_touches_restricted_select_field`] that operates
	/// on an already-resolved table name and condition. Also used by the
	/// `UnionIndexScan` planner to decide whether
	/// `strip_union_index_conditions` is safe — the union sub-operators
	/// don't re-evaluate the WHERE leaf after field-level permissions, so
	/// stripping a leaf on a restricted field would turn the index entries
	/// themselves into a membership oracle.
	async fn cond_touches_restricted_select_field_for_table(
		&self,
		table_name: &TableName,
		cond: &Cond,
	) -> bool {
		let (Some(ns_name), Some(db_name)) = (self.ns.as_deref(), self.db.as_deref()) else {
			// No catalog access at plan time — conservatively assume
			// permissions could apply.
			return true;
		};
		if !self.should_check_perms_for_view(ns_name, db_name) {
			return false;
		}
		let Some(txn) = self.txn.as_ref() else {
			return true;
		};
		let Some((ns_id, db_id)) = self.ns_db_ids().await else {
			return true;
		};
		let fields = match txn.all_tb_fields(ns_id, db_id, table_name, None).await {
			Ok(fs) => fs,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time field list failed in \
					 cond_touches_restricted_select_field_for_table; \
					 conservatively disabling index-strip fast paths",
				);
				return true;
			}
		};
		// Collect the field paths that are not unconditionally SELECT-able.
		let restricted_prefixes: Vec<crate::expr::Idiom> = fields
			.iter()
			.filter(|f| !matches!(f.select_permission, crate::catalog::Permission::Full))
			.map(|f| f.name.clone())
			.collect();
		if restricted_prefixes.is_empty() {
			return false;
		}
		let mut checker = RestrictedIdiomChecker {
			restricted_prefixes: &restricted_prefixes,
			found: false,
		};
		use crate::expr::visit::Visitor;
		let _ = checker.visit_expr(&cond.0);
		checker.found
	}

	/// Resolve a B-tree index access path covering the WHERE condition for
	/// key-only counting.  Returns `Some((IndexRef, BTreeAccess))` when the
	/// index analysis finds a B-tree index that fully covers the predicate
	/// (no residual filter), allowing `IndexCountScan` to count index keys
	/// instead of deserializing records.
	async fn resolve_count_btree_access(
		&self,
		what: &[Expr],
		cond: &Option<Cond>,
		with: Option<&With>,
	) -> Option<(IndexRef, BTreeAccess)> {
		let txn = self.txn.as_ref()?;
		let table_name = match what.first() {
			Some(Expr::Table(t)) => t,
			_ => return None,
		};
		let cond = cond.as_ref()?;

		let (ns_id, db_id) = self.ns_db_ids().await?;
		let indexes = match txn.all_tb_indexes(ns_id, db_id, table_name, None).await {
			Ok(idx) => idx,
			Err(e) => {
				tracing::warn!(
					table = %table_name,
					error = %e,
					"plan-time index list failed in resolve_count_btree_access; \
					 falling back to runtime",
				);
				return None;
			}
		};
		// Key-only count scans read index data directly, so restrict candidates
		// to durable-online indexes.
		let indexes = filter_online_indexes(txn, ns_id, db_id, indexes).await.ok()?;

		if indexes.is_empty() {
			return None;
		}

		let analyzer = IndexAnalyzer::new(indexes, with);
		let candidates = analyzer.analyze(Some(cond), None);

		// Look for a candidate that fully covers the WHERE condition
		// (no residual filter needed).
		for candidate in &candidates {
			// Check: does this index access fully cover the condition?
			// If strip_index_conditions returns None, the index
			// consumed the entire WHERE clause.
			if strip_index_conditions(cond, &candidate.access, &candidate.index_ref.cols).is_none()
			{
				return Some((candidate.index_ref.clone(), candidate.access.clone()));
			}
		}

		None
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
		txn: &Transaction,
		ns_name: &str,
		db_name: &str,
		table_name: &TableName,
		cond: Option<&Cond>,
		order: Option<&OrderClause>,
		with: Option<&With>,
	) -> Result<Option<(AccessPath, ScanDirection)>, Error> {
		let direction = determine_scan_direction(order);

		// If the entire WHERE clause folded to `false` (e.g. `field IN []`
		// short-circuited by `fold_condition_expressions`) the SELECT can
		// produce no rows. Skip index lookup entirely.
		if let Some(c) = cond
			&& matches!(&c.0, Expr::Literal(crate::expr::literal::Literal::Bool(false)))
		{
			return Ok(Some((AccessPath::EmptyScan, direction)));
		}

		if matches!(with, Some(With::NoIndex)) {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Look up namespace and database to get IDs
		let ns_def = match txn.get_ns_by_name(ns_name, None).await {
			Ok(Some(ns)) => ns,
			_ => return Ok(None),
		};
		let db_def = match txn.get_db_by_name(ns_name, db_name, None).await {
			Ok(Some(db)) => db,
			_ => return Ok(None),
		};

		// Fetch queryable indexes for the table. Building or errored durable
		// indexes stay in the catalog for write admission, but the planner must
		// ignore them until the durable phase is `Online`.
		let indexes = match txn
			.all_tb_indexes(ns_def.namespace_id, db_def.database_id, table_name, None)
			.await
		{
			Ok(idx) => {
				match filter_online_indexes(txn, ns_def.namespace_id, db_def.database_id, idx).await
				{
					Ok(idx) => idx,
					Err(_) => return Ok(None),
				}
			}
			Err(_) => return Ok(None),
		};

		if indexes.is_empty() {
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		// Rewrite projection function calls (e.g. type::field("name")) →
		// Idiom in a cloned condition so the index analyzer can match
		// against indexed columns.
		let rewritten_cond = cond.map(|c| {
			let mut c = c.clone();
			resolve_projection_field_idioms(&mut c, self.function_registry());
			c
		});
		let analysis_cond = rewritten_cond.as_ref();

		let analyzer = IndexAnalyzer::new(indexes, with);
		let candidates = analyzer.analyze(analysis_cond, order);

		if candidates.is_empty() {
			if let Some(path) = analyzer.try_or_union(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding IN operators into union of equality lookups
			if let Some(path) = analyzer.try_in_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			// Try expanding CONTAINSALL/CONTAINSANY into union of equality lookups
			if let Some(path) = analyzer.try_containment_expansion(analysis_cond, direction) {
				return Ok(Some((path, direction)));
			}
			return Ok(Some((AccessPath::TableScan, direction)));
		}

		let path = select_access_path(candidates, with, direction);

		// When the chosen index covers ORDER BY, derive the correct scan
		// direction from the ORDER BY clause rather than the default
		// `determine_scan_direction` (which only handles ORDER BY id).
		// This enables LIMIT pushdown and sort elimination for queries like
		// `ORDER BY metadata.payload_metadata.modified DESC LIMIT 25`.
		let (path, direction) = adjust_direction_for_order(path, order, direction);

		// When the best single-index path is a full-range scan (ORDER BY
		// only, no WHERE selectivity), also try a multi-index union for
		// OR conditions. The union reads only matching rows from each
		// branch, which is typically far better than scanning every row
		// in the index. The outer pipeline adds a Sort when the union
		// does not satisfy ORDER BY.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_or_union(analysis_cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}
		// Same logic for containment expansion (CONTAINSANY / ANYINSIDE
		// against an array-element index): the full-range scan would
		// walk every indexed entry — typically one per array element
		// per row — and filter post-hoc. A Union of per-value Compound
		// prefix scans reads only the matching prefix ranges, which is
		// the win we're after for crud-bench-style workloads. The k-way
		// merge with `MergeMode::ByIndexKeyDedup` preserves the trailing
		// ORDER BY column's sort order and dedupes rows that match
		// multiple branches.
		if path.is_full_range_scan()
			&& let Some(union_path) = analyzer.try_containment_expansion(analysis_cond, direction)
		{
			return Ok(Some((union_path, direction)));
		}
		// NOTE: We intentionally do NOT try try_in_expansion() here.
		// The full-range scan covers ORDER BY, enabling sort elimination
		// and early termination with the batch ceiling.  Replacing it
		// with a Union of prefix scans would require an expensive Sort
		// of ALL matching records, which is far worse for ORDER BY +
		// LIMIT queries.  IN expansion is only helpful in the
		// candidates.is_empty() fallback above when no index covers
		// ORDER BY at all.

		Ok(Some((path, direction)))
	}
}

/// Visitor that walks a `Cond` expression looking for any idiom governed by
/// a field path with restrictive SELECT permissions. Stops descending into
/// nested SELECT subqueries — those operate against their own tables and
/// will perform their own permission resolution.
struct RestrictedIdiomChecker<'a> {
	restricted_prefixes: &'a [Idiom],
	found: bool,
}

impl crate::expr::visit::Visitor for RestrictedIdiomChecker<'_> {
	type Error = std::convert::Infallible;

	fn visit_idiom(&mut self, idiom: &Idiom) -> Result<(), Self::Error> {
		if self.found {
			return Ok(());
		}
		for prefix in self.restricted_prefixes {
			if idiom.starts_with(prefix.0.as_slice()) {
				self.found = true;
				return Ok(());
			}
		}
		Ok(())
	}

	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		// Subqueries are evaluated against their own tables and apply their
		// own permission resolution at execute time.
		Ok(())
	}
}

/// Adjust the scan direction and access path when the chosen index covers
/// the ORDER BY clause.
///
/// `determine_scan_direction` only flips to `Backward` for `ORDER BY id DESC`.
/// When an index covers a non-`id` ORDER BY (e.g. a nested field like
/// `metadata.payload_metadata.modified DESC`), we must derive the correct
/// direction from the ORDER BY clause so that:
///
/// 1. `index_covers_ordering()` succeeds → LIMIT is pushed to the IndexScan
/// 2. `can_eliminate_sort()` succeeds → the Sort operator is eliminated
///
/// Without this fix, the index is scanned forward, LIMIT cannot be pushed
/// (direction mismatch), and all rows are read + sorted in memory.
fn adjust_direction_for_order(
	path: AccessPath,
	order: Option<&crate::expr::order::Ordering>,
	default_direction: crate::idx::planner::ScanDirection,
) -> (AccessPath, crate::idx::planner::ScanDirection) {
	use crate::exec::field_path::FieldPath;
	use crate::exec::index::access_path::BTreeAccess;
	use crate::expr::order::Ordering;
	use crate::idx::planner::ScanDirection;

	// Only adjust for BTreeScan paths that cover ORDER BY
	let AccessPath::BTreeScan {
		ref index_ref,
		ref access,
		..
	} = path
	else {
		return (path, default_direction);
	};

	// Need an ORDER BY clause to determine direction
	let Some(Ordering::Order(order_list)) = order else {
		return (path, default_direction);
	};

	let ix_def = index_ref.definition();

	// Collect equality-pinned column paths so we can skip ORDER BY fields
	// that reference them (those columns have a single constant value,
	// so any direction trivially satisfies the requirement).
	let equality_col_paths: Vec<FieldPath> = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => ix_def
			.cols
			.iter()
			.take(prefix.len())
			.filter_map(|idiom| FieldPath::try_from(idiom).ok())
			.collect(),
		BTreeAccess::Equality(_) => {
			ix_def.cols.iter().filter_map(|idiom| FieldPath::try_from(idiom).ok()).collect()
		}
		_ => vec![],
	};

	// Skip leading ORDER BY fields that match equality-pinned columns.
	let mut order_idx = 0;
	for field in order_list.0.iter() {
		if let Ok(fp) = FieldPath::try_from(&field.value)
			&& equality_col_paths.contains(&fp)
		{
			order_idx += 1;
			continue;
		}
		break;
	}

	// Get the first non-constant ORDER BY field
	let Some(first_order) = order_list.0.get(order_idx) else {
		// All ORDER BY fields are constant — direction doesn't matter,
		// keep the default.
		return (path, default_direction);
	};

	let Ok(order_path) = FieldPath::try_from(&first_order.value) else {
		return (path, default_direction);
	};

	// Determine which index column to match against.
	// For compound access with an equality prefix, match the column
	// immediately after the prefix.  For Equality access on a
	// single-column index, all index columns are skipped.
	let target_col_index = match access {
		BTreeAccess::Compound {
			prefix,
			..
		} => prefix.len(),
		BTreeAccess::Equality(_) => ix_def.cols.len(),
		_ => 0,
	};

	// If all index columns are equality-pinned, the effective ordering
	// is by record ID.  Check if the ORDER BY field is `id`.
	if target_col_index >= ix_def.cols.len() {
		// All columns are equality-pinned.  Match `ORDER BY id`.
		if order_path == FieldPath::field("id") {
			let new_direction = if first_order.direction {
				ScanDirection::Forward // ASC
			} else {
				ScanDirection::Backward // DESC
			};
			let new_path = AccessPath::BTreeScan {
				index_ref: index_ref.clone(),
				access: access.clone(),
				direction: new_direction,
			};
			return (new_path, new_direction);
		}
		return (path, default_direction);
	}

	let Some(target_col) = ix_def.cols.get(target_col_index) else {
		return (path, default_direction);
	};

	let Ok(col_path) = FieldPath::try_from(target_col) else {
		return (path, default_direction);
	};

	// If the target column matches the ORDER BY field,
	// set the direction based on the ORDER BY direction
	if order_path == col_path {
		let new_direction = if first_order.direction {
			ScanDirection::Forward // ASC
		} else {
			ScanDirection::Backward // DESC
		};

		let new_path = AccessPath::BTreeScan {
			index_ref: index_ref.clone(),
			access: access.clone(),
			direction: new_direction,
		};

		(new_path, new_direction)
	} else {
		(path, default_direction)
	}
}

/// Collect output field names from a SELECT field list.
///
/// These names are passed as the `reserved_names` argument of
/// `ExpressionRegistry::with_reserved_and_protected_names` so that
/// synthetic internal names (`_e0`, `_e1`, ...) do not collide with fields
/// the user explicitly selected.
pub(super) fn collect_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(_) => vec![], // SELECT VALUE has no object fields
		Fields::Select(field_list) => {
			let mut names = Vec::with_capacity(field_list.len());
			for field in field_list {
				if let Field::Single(selector) = field {
					let name = if let Some(alias) = &selector.alias {
						idiom_to_field_name(alias)
					} else {
						derive_field_name(&selector.expr)
					};
					names.push(name);
				}
			}
			names
		}
	}
}

/// Collect simple source fields read by SELECT projections.
///
/// These names must not be used as Compute internal names, even when a
/// computed expression has the same alias. Compute runs before projection, so
/// using one of these names internally would overwrite the source value before
/// another projection can read it.
pub(super) fn collect_simple_source_field_names(fields: &Fields) -> Vec<String> {
	match fields {
		Fields::Value(selector) => match &selector.expr {
			Expr::Idiom(idiom) => simple_field_name(idiom).into_iter().collect(),
			_ => vec![],
		},
		Fields::Select(field_list) => field_list
			.iter()
			.filter_map(|field| match field {
				Field::Single(selector) => match &selector.expr {
					Expr::Idiom(idiom) => simple_field_name(idiom),
					_ => None,
				},
				Field::All => None,
			})
			.collect(),
	}
}

fn simple_field_name(idiom: &Idiom) -> Option<String> {
	use crate::expr::part::Part;

	if idiom.len() == 1
		&& let Some(Part::Field(name)) = idiom.first()
	{
		return Some(name.as_str().to_owned());
	}
	None
}

/// Check whether the ORDER BY clause is exactly `ORDER BY id ASC` or
/// `ORDER BY id DESC` with no additional columns.
///
/// Returns `Some(SortDirection)` when the condition is met, allowing
/// callers to enable optimisations that rely on record-ID ordering
/// (e.g. merge-sort in `UnionIndexScan`).
fn detect_order_by_id_only(order: Option<&crate::expr::order::Ordering>) -> Option<SortDirection> {
	use crate::expr::order::Ordering;
	if let Some(Ordering::Order(order_list)) = order
		&& order_list.len() == 1
		&& let Some(first) = order_list.0.first()
		&& first.value.is_id()
		&& !first.collate
		&& !first.numeric
	{
		Some(if first.direction {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		})
	} else {
		None
	}
}

/// Detect the pattern "every branch pins the same composite-index prefix
/// to an equality value, and ORDER BY is the next column of that index".
///
/// When this holds, each per-branch sub-scan is already sorted by the
/// ORDER BY column (in the scan's direction), so a k-way merge over the
/// branches by that column yields a globally-sorted stream — with
/// early-stop on a downstream `LIMIT`.
///
/// Returns the field path to merge on and the required scan direction
/// (the caller may need to rewrite per-branch scan directions to match).
/// Returns `None` when the pattern is not applicable (different indexes,
/// non-equality access, ORDER BY column not the next index column,
/// COLLATE/NUMERIC modifiers, single-column indexes, duplicate branch
/// prefixes, etc).
fn detect_order_for_composite_union(
	order: Option<&crate::expr::order::Ordering>,
	paths: &[AccessPath],
) -> Option<(crate::exec::field_path::FieldPath, SortDirection)> {
	use crate::exec::field_path::FieldPath;
	use crate::exec::index::access_path::BTreeAccess;
	use crate::expr::order::Ordering;

	// Must be ORDER BY a single, plain column with no collation modifiers.
	let Some(Ordering::Order(order_list)) = order else {
		return None;
	};
	if order_list.len() != 1 {
		return None;
	}
	let order_field = order_list.0.first()?;
	if order_field.collate || order_field.numeric {
		return None;
	}
	let order_path = FieldPath::try_from(&order_field.value).ok()?;
	let direction = if order_field.direction {
		SortDirection::Asc
	} else {
		SortDirection::Desc
	};

	// All branches must share the same index. Take the first branch's
	// index_ref as the reference and check every branch against it.
	let mut branches = paths.iter();
	let first = branches.next()?;
	let (first_index_ref, first_prefix_len) = match first {
		AccessPath::BTreeScan {
			index_ref,
			access: BTreeAccess::Compound {
				prefix,
				range: None,
			},
			..
		} => (index_ref.clone(), prefix.len()),
		// Single-column equality on the ORDER BY column itself wouldn't
		// give us a "next column" to merge on.
		_ => return None,
	};
	// Composite index must have an additional column after the prefix
	// that is exactly the ORDER BY target.
	let ix_def = first_index_ref.definition();
	if ix_def.cols.len() <= first_prefix_len {
		return None;
	}
	let sort_col_idiom = ix_def.cols.get(first_prefix_len)?;
	let sort_col_path = FieldPath::try_from(sort_col_idiom).ok()?;
	if sort_col_path != order_path {
		return None;
	}

	// Every remaining branch must use the same index and same prefix
	// length. Branch directions need not match — the caller rewrites
	// each branch's scan direction to align with ORDER BY before
	// constructing the sub-operators. Branch prefixes must be distinct
	// so no record appears twice.
	let first_prefix = match first {
		AccessPath::BTreeScan {
			access: BTreeAccess::Compound {
				prefix,
				..
			},
			..
		} => prefix.as_slice(),
		_ => return None,
	};
	let mut seen_prefixes: Vec<&[crate::val::Value]> = Vec::with_capacity(paths.len());
	seen_prefixes.push(first_prefix);
	for path in branches {
		let (idx, access) = match path {
			AccessPath::BTreeScan {
				index_ref,
				access: access @ BTreeAccess::Compound {
					range: None,
					..
				},
				..
			} => (index_ref, access),
			_ => return None,
		};
		if idx != &first_index_ref {
			return None;
		}
		let BTreeAccess::Compound {
			prefix,
			..
		} = access
		else {
			return None;
		};
		if prefix.len() != first_prefix_len {
			return None;
		}
		if seen_prefixes.iter().any(|p| p == &prefix.as_slice()) {
			return None;
		}
		seen_prefixes.push(prefix.as_slice());
	}

	Some((order_path, direction))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;

	fn parse_select(src: &str) -> crate::expr::statements::SelectStatement {
		let ast = syn::parse(src).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement: got {} from {src:?}", exprs.len());
		let top: crate::expr::TopLevelExpr = exprs.remove(0).into();
		match top {
			crate::expr::TopLevelExpr::Expr(crate::expr::Expr::Select(s)) => *s,
			other => panic!("expected SELECT, got {other:?}"),
		}
	}

	/// Locks in the fix for the `$parent`-in-method-arg case. Pre-fix,
	/// `NeededFieldExtractor` added `refs` and `kind` (the field paths
	/// reached through `$parent`) to this row's needed-fields. The
	/// post-fix selective-scan set should contain only the fields that
	/// belong to *this* row's table (`cat`, reached via `$this.cat`).
	///
	/// This is the structural counterpart to issue #7154's runtime fix —
	/// that one was about not treating bare `parent` / `this` as field
	/// names when they appear as `Part::Start(Expr::Param)`. The current
	/// fix tightens the same path: not treating fields *under* `$parent`
	/// as this row's columns either.
	#[test]
	fn extract_needed_fields_excludes_outer_row_paths() {
		// `$parent.refs` reaches into the OUTER row's `refs` field — the
		// inner subquery's needed-fields should NOT include `refs`.
		// `$this.cat` reaches THIS row's `cat` — that one should be in.
		let stmt = parse_select(
			"SELECT cat FROM users \
			 WHERE $parent.refs CONTAINS $this.cat",
		);
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(
			needed.contains("cat"),
			"current row's `cat` should be in needed-fields: got {needed:?}",
		);
		assert!(
			!needed.contains("refs"),
			"$parent.refs is the OUTER row's `refs`, must NOT be in this row's \
			 needed-fields: got {needed:?}",
		);
	}

	/// Same shape but with a `Part::Where` inside `$parent.refs[WHERE ...]`,
	/// which is the bug class my review flagged: even when `$parent` is at
	/// `Part::Start`, predicates *inside* a downstream Where part used to
	/// inflate the needed-fields set with names from the outer row.
	#[test]
	fn extract_needed_fields_excludes_parent_paths_in_method_args() {
		let stmt = parse_select(
			"SELECT cat FROM users \
			 WHERE array::find($parent.refs, $this.cat) != NONE",
		);
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(needed.contains("cat"), "got {needed:?}");
		assert!(
			!needed.contains("refs"),
			"`$parent.refs` inside a function-call argument must not leak \
			 into this row's needed-fields: got {needed:?}",
		);
	}

	/// Sanity: `$this.x` paths still propagate into the needed-fields set
	/// (regression guard against over-aggressive filtering).
	#[test]
	fn extract_needed_fields_keeps_this_row_paths() {
		let stmt = parse_select("SELECT name FROM users WHERE $this.age > 18");
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("age"), "got {needed:?}");
	}

	/// Filter-predicate scoping: `t.refs[WHERE kind = 'tag']`. The bare
	/// `kind` references the *iteration element* of `refs`, not this
	/// row's columns. The previous `NeededFieldExtractor` walked the
	/// predicate naively and added `kind` to the current row's
	/// needed-fields, inflating the selective scan. After the fix, only
	/// `refs` (the array being iterated) is in the set.
	#[test]
	fn extract_needed_fields_filter_predicate_scope_excluded() {
		let stmt = parse_select(
			"SELECT name FROM users \
			 WHERE refs[WHERE kind = 'tag'] != []",
		);
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("refs"), "got {needed:?}");
		assert!(
			!needed.contains("kind"),
			"`kind` inside `[WHERE …]` references the iteration element, \
			 not this row: got {needed:?}",
		);
	}

	/// Filter-predicate scoping with `$parent`:
	/// `t.refs[WHERE $parent.cat = kind]`. Inside the filter `$parent`
	/// is rebound to this row, so `cat` IS a current-row field. Sibling
	/// iteration-scope idioms (`kind`) remain excluded.
	#[test]
	fn extract_needed_fields_filter_predicate_parent_promotes() {
		let stmt = parse_select(
			"SELECT name FROM users \
			 WHERE refs[WHERE $parent.cat = kind] != []",
		);
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(needed.contains("name"), "got {needed:?}");
		assert!(needed.contains("refs"), "got {needed:?}");
		assert!(
			needed.contains("cat"),
			"`$parent.cat` inside `[WHERE …]` is rebound to this row: \
			 got {needed:?}",
		);
		assert!(!needed.contains("kind"), "got {needed:?}");
	}

	/// Sanity: a bare field literally named `parent` (not the `$parent`
	/// parameter) is treated as a real column. Pinned by the existing
	/// `7154_parent_field_name_select.surql` reproduction at the
	/// runtime/integration level; this is the analyser-level guard.
	#[test]
	fn extract_needed_fields_treats_bare_parent_as_field() {
		let stmt = parse_select("SELECT parent.sub FROM table");
		let needed = Planner::extract_needed_fields(
			&stmt.fields,
			&stmt.omit,
			stmt.cond.as_ref(),
			stmt.order.as_ref(),
			stmt.group.as_ref(),
			stmt.split.as_ref(),
		)
		.expect("non-wildcard projection should produce a Some(set)");
		assert!(
			needed.contains("parent"),
			"bare `parent.sub` is a real column path: got {needed:?}",
		);
	}
}
