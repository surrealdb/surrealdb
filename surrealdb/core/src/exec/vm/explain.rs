//! Physical plan EXPLAIN tree representation for the VM.
//!
//! Converts a [`PhysicalPlan`] into an operator tree suitable for
//! [`ExplainPlan`] and [`AnalyzePlan`] rendering. Every statement the VM runs
//! is explained from the plan it is mapped to: the access path of each source,
//! the statements nested in its predicates that run as membership joins, and,
//! under `EXPLAIN ANALYZE`, what the run measured of a KNN search's prefilter.
//!
//! [`ExplainPlan`]: crate::exec::operators::ExplainPlan
//! [`AnalyzePlan`]: crate::exec::operators::AnalyzePlan

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use futures::StreamExt;
use surrealdb_strand::TableName;
use surrealdb_types::ToSql;

use super::plan::{
	BranchRead, Control, Nested, PhysicalPlan, Plan, Projection, SortKeyRead, Strategy, ValuesRead,
	WriteSubjects,
};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::metrics::OperatorMetrics;
use crate::exec::operators::scan::bitmap::evaluate_fusion;
use crate::exec::operators::scan::{
	BitmapNode, FusedCandidates, MergeMode, PrefilterTier, choose_prefilter_tier,
};
use crate::exec::planner::CountDecision;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, ValueBatch, ValueBatchStream,
};
use crate::expr::BinaryOperator;
use crate::expr::logical::{LogicalExpr, LogicalOp, SourceRef};

/// An operator tree node representing a step of a [`PhysicalPlan`].
#[derive(Debug)]
pub(crate) struct ExplainNode {
	pub(crate) name: &'static str,
	pub(crate) context: ContextLevel,
	pub(crate) attrs: Vec<(String, String)>,
	pub(crate) children: Vec<Arc<dyn ExecOperator>>,
	pub(crate) metrics: Option<Arc<OperatorMetrics>>,
	pub(crate) exec: Option<Arc<dyn ExecOperator>>,
	/// For a KNN scan under `EXPLAIN ANALYZE`: the strategy its prefilter's
	/// allow-list chose, once [`Self::measured`] has been evaluated.
	tier: Option<Arc<AtomicU8>>,
	/// For a bitmap scan under `EXPLAIN ANALYZE`: whether an overflowing
	/// branch made it answer with its fallback.
	fallback: Option<Arc<AtomicBool>>,
	/// For the root of an analysed plan: the bitmap trees it evaluates after
	/// the statement has run.
	measured: Vec<Arc<Measured>>,
	/// Whether every access path beneath renders as `<permission>`; see
	/// [`ExecOperator::redacts_access_paths_beneath`].
	redacts_access_paths: bool,
	/// Whether `EXPLAIN ANALYZE` withholds the metrics beneath; see
	/// [`ExecOperator::hides_metrics_beneath`].
	hides_metrics: bool,
}

impl ExplainNode {
	fn new(
		name: &'static str,
		context: ContextLevel,
		attrs: Vec<(String, String)>,
		children: Vec<Arc<dyn ExecOperator>>,
		metrics: Option<&Arc<OperatorMetrics>>,
	) -> Self {
		Self {
			name,
			context,
			attrs,
			children,
			metrics: metrics.cloned(),
			exec: None,
			tier: None,
			fallback: None,
			measured: Vec::new(),
			redacts_access_paths: false,
			hides_metrics: false,
		}
	}

	fn shared(self) -> Arc<dyn ExecOperator> {
		Arc::new(self)
	}
}

impl ExecOperator for ExplainNode {
	fn name(&self) -> &'static str {
		self.name
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = self.attrs.clone();
		if let Some(label) =
			self.tier.as_ref().and_then(|tier| PrefilterTier::label(tier.load(Ordering::Relaxed)))
		{
			attrs.push(("prefilter_tier".to_string(), label.to_string()));
		}
		if self.fallback.as_ref().is_some_and(|taken| taken.load(Ordering::Relaxed)) {
			attrs.push(("union_tier".to_string(), "fallback".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		self.context
	}

	fn redacts_access_paths_beneath(&self) -> bool {
		self.redacts_access_paths
	}

	fn hides_metrics_beneath(&self) -> bool {
		self.hides_metrics
	}

	fn access_mode(&self) -> AccessMode {
		if let Some(exec) = &self.exec {
			exec.access_mode()
		} else {
			AccessMode::ReadOnly
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		if let Some(exec) = &self.exec {
			exec.cardinality_hint()
		} else {
			CardinalityHint::AtMostOne
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children.iter().collect()
	}

	fn explain_children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		self.metrics.as_deref().or_else(|| self.exec.as_ref().and_then(|e| e.metrics()))
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let Some(exec) = &self.exec else {
			return Ok(Box::pin(futures::stream::empty()));
		};
		// The program records the counters of its host steps into the metrics
		// every node of the analysed plan reports.
		let stream = match &self.metrics {
			Some(metrics) => exec.execute(&ctx.with_vm_metrics(Arc::clone(metrics)))?,
			None => exec.execute(ctx)?,
		};
		let stream = if let Some(metrics) = &self.metrics {
			crate::exec::metrics::monitor_stream(stream, self.name, metrics)
		} else {
			stream
		};
		if self.measured.is_empty() {
			return Ok(stream);
		}
		// The bitmap trees are evaluated once the statement has run, before
		// the plan is rendered, which the end of this stream tells.
		let trees = self.measured.clone();
		let ctx = ctx.clone();
		let measured = futures::stream::once(async move {
			for tree in &trees {
				tree.measure(&ctx).await?;
			}
			Ok::<Option<ValueBatch>, crate::expr::ControlFlow>(None)
		})
		.filter_map(|measured| std::future::ready(measured.transpose()));
		Ok(Box::pin(stream.chain(measured)))
	}

	fn enable_metrics(&self) {
		if let Some(m) = &self.metrics {
			m.enable();
		}
		if let Some(exec) = &self.exec {
			exec.enable_metrics();
		}
	}
}

/// A bitmap tree of the plan, evaluated again for the analysed plan: its nodes
/// record the cardinality they produce, and the outcome of the evaluation
/// tells what the run chose by it.
///
/// A bitmap is a function of the transaction the statement ran in, so this
/// evaluation yields what the run's did.
#[derive(Debug)]
enum Measured {
	/// The allow-list a KNN search is restricted to, whose size chose the
	/// strategy of the search.
	Prefilter {
		table: TableName,
		bitmap: Arc<BitmapNode>,
		tier: Arc<AtomicU8>,
		/// Whether the statement reads at a `VERSION`, which no allow-list
		/// serves.
		versioned: bool,
	},
	/// The doc-IDs a fused plan resolves, or the overflow that made it answer
	/// with its fallback.
	Fusion {
		table: TableName,
		bitmap: Arc<BitmapNode>,
		/// Whether the plan has no fallback, so no branch may overflow.
		anchored: bool,
		fallback: Arc<AtomicBool>,
	},
}

impl Measured {
	async fn measure(&self, ctx: &ExecutionContext) -> FlowResult<()> {
		match self {
			Self::Prefilter {
				table,
				bitmap,
				tier,
				versioned,
			} => {
				let chosen = if *versioned {
					PrefilterTier::Fallback
				} else {
					match bitmap
						.build_allowlist(ctx, table, *surrealdb_cnf::BITMAP_BRANCH_BUDGET)
						.await?
					{
						Some(allowed) => choose_prefilter_tier(
							allowed.len(),
							*surrealdb_cnf::KNN_PREFILTER_EXACT_THRESHOLD,
							*surrealdb_cnf::KNN_PREFILTER_EF_BOOST_THRESHOLD,
						),
						None => PrefilterTier::Fallback,
					}
				};
				tier.store(chosen as u8, Ordering::Relaxed);
			}
			Self::Fusion {
				table,
				bitmap,
				anchored,
				fallback,
			} => {
				if matches!(
					evaluate_fusion(ctx, table, bitmap, *anchored).await?,
					FusedCandidates::Overflow
				) {
					fallback.store(true, Ordering::Relaxed);
				}
			}
		}
		Ok(())
	}
}

/// How a plan is turned into nodes. Its name differs from the plan-side `Walk` because the
/// plain-data census matches types by name.
struct Explainer<'a> {
	/// The metrics every node reports under `EXPLAIN ANALYZE`.
	metrics: Option<&'a Arc<OperatorMetrics>>,
	/// Whether the bitmap trees of the plan are measured by evaluating them
	/// after the statement runs, rather than described with the metrics the
	/// nodes share.
	measure: bool,
	/// Whether the plan being walked reads at a `VERSION`.
	versioned: bool,
	/// Whether the statement being walked asks for `TEMPFILES`, so its sorts
	/// that keep every row spill to disk when the datastore names a temporary
	/// directory.
	tempfiles: bool,
	/// The statements nested in the expressions of the plan being walked.
	nested: &'a [Arc<Nested>],
	measured: Vec<Arc<Measured>>,
	/// Whether the plan reads under permissions enforced on its caller. The
	/// rows beneath a node that enforces a definer's declaration on such a
	/// caller are rows it may not see, so `EXPLAIN ANALYZE` reports no
	/// metrics for the operators beneath it.
	checked: bool,
	/// The predicate a scan below judges of a record's stored bytes, for the
	/// scan to show as its own: set where a `Filter` stands directly over the
	/// scan, taken by the scan.
	prefilter: Option<String>,
	/// The paging a scan below takes, for the scan to show as its own: set
	/// where a `Limit` stands directly over the scan, taken by the scan.
	paging: Option<Vec<(String, String)>>,
}

impl<'a> Explainer<'a> {
	fn node(
		&self,
		name: &'static str,
		context: ContextLevel,
		attrs: Vec<(String, String)>,
		children: Vec<Arc<dyn ExecOperator>>,
	) -> ExplainNode {
		ExplainNode::new(name, context, attrs, children, self.metrics)
	}

	/// The nodes of `physical` and the clauses around it, with `physical`'s
	/// nested statements and `VERSION` in force beneath them.
	fn plan(&mut self, physical: &'a PhysicalPlan) -> ExplainNode {
		let outer = (self.nested, self.versioned, self.tempfiles);
		self.nested = &physical.nested;
		self.versioned = physical.tail.version.is_some();
		self.tempfiles = physical.tail.tempfiles;
		let mut root = self.root(&physical.root);
		if let Some(timeout) = &physical.tail.timeout {
			root = self.node(
				"Timeout",
				ContextLevel::Root,
				vec![("timeout".to_string(), timeout.to_sql())],
				vec![root.shared()],
			);
		}
		if let Some(version) = &physical.tail.version {
			root = self.node(
				"VersionScope",
				ContextLevel::Database,
				vec![("version".to_string(), version.to_sql())],
				vec![root.shared()],
			);
		}
		if physical.tail.only {
			root = self.node("UnwrapExactlyOne", ContextLevel::Root, vec![], vec![root.shared()]);
		}
		(self.nested, self.versioned, self.tempfiles) = outer;
		root
	}

	/// The statements nested in the membership conjuncts of `predicate`,
	/// beneath the strategy they run by.
	fn memberships(&mut self, predicate: &LogicalExpr) -> Vec<Arc<dyn ExecOperator>> {
		let mut found = Vec::new();
		membership_statements(predicate, &mut found);
		let mut children = Vec::new();
		for statement in found {
			let nested: &'a [Arc<Nested>] = self.nested;
			let Some(nested) = nested.iter().find(|nested| nested.op == *statement) else {
				continue;
			};
			let nested: &'a Nested = Arc::as_ref(nested);
			let strategy = match nested.strategy {
				Strategy::Apply => "apply",
				Strategy::Once => "once",
				Strategy::Hash => "hash",
				Strategy::Lookup if nested.hashed.is_some() => "lookup, then hash",
				Strategy::Lookup => "lookup",
			};
			let plan = self.plan(&nested.plan);
			children.push(
				self.node(
					"Subquery",
					ContextLevel::Database,
					vec![("strategy".to_string(), strategy.to_string())],
					vec![plan.shared()],
				)
				.shared(),
			);
		}
		children
	}

	/// The node for `control`: a block, a `LET`, a loop or a `RETURN` over the
	/// plans of the statements it runs.
	fn control(&mut self, control: &'a Control) -> ExplainNode {
		match control {
			Control::Seq(items) => {
				let children = items.iter().map(|item| self.control(item).shared()).collect();
				self.node(
					"Sequence",
					ContextLevel::Root,
					vec![("statements".to_string(), items.len().to_string())],
					children,
				)
			}
			Control::Let {
				name,
				value,
				..
			} => {
				let child = self.control(value).shared();
				self.node(
					"Let",
					ContextLevel::Root,
					vec![("name".to_string(), format!("${name}"))],
					vec![child],
				)
			}
			Control::For {
				param,
				range,
				body,
				..
			} => {
				let range = self.control(range).shared();
				let body = self.control(body).shared();
				self.node(
					"Foreach",
					ContextLevel::Database,
					vec![("param".to_string(), param.to_sql())],
					vec![range, body],
				)
			}
			Control::Return {
				value,
			} => {
				let child = self.control(value).shared();
				self.node("Return", ContextLevel::Root, vec![], vec![child])
			}
			Control::Sleep {
				duration,
			} => self.node(
				"Sleep",
				ContextLevel::Root,
				vec![("duration".to_string(), duration.to_sql())],
				vec![],
			),
			Control::Value(expr) => self.node(
				"ExprPlan",
				ContextLevel::Root,
				vec![("expr".to_string(), expr.to_sql())],
				vec![],
			),
			Control::If {
				branches,
				otherwise,
			} => {
				let mut children = Vec::new();
				for (condition, body) in branches {
					children.push(self.control(condition).shared());
					children.push(self.control(body).shared());
				}
				if let Some(otherwise) = otherwise {
					children.push(self.control(otherwise).shared());
				}
				self.node("If", ContextLevel::Root, vec![], children)
			}
			Control::Throw {
				value,
			} => {
				let child = self.control(value).shared();
				self.node("Throw", ContextLevel::Root, vec![], vec![child])
			}
			Control::Statement {
				plan,
				..
			} => self.plan(plan),
			Control::Break => self.node("Break", ContextLevel::Root, vec![], vec![]),
			Control::Continue => self.node("Continue", ContextLevel::Root, vec![], vec![]),
		}
	}

	/// A B-tree scan. A key range that leaves a fanning column open reaches a
	/// record through several entries, which the host collapses to one row per
	/// record, so the scan sits beneath a `DistinctRecords`.
	fn btree_scan(
		&mut self,
		table: &TableName,
		index: &crate::exec::index::access_path::IndexRef,
		access: &crate::exec::index::access_path::BTreeAccess,
		direction: crate::kvs::Direction,
	) -> ExplainNode {
		let mut attrs = index_scan_attrs(table, index.name.as_ref(), access, direction);
		attrs.extend(self.paging.take().unwrap_or_default());
		let scan = self.node("IndexScan", ContextLevel::Database, attrs, vec![]);
		if crate::exec::index::access_path::access_repeats_records(index, access) {
			self.node("DistinctRecords", ContextLevel::Database, vec![], vec![scan.shared()])
		} else {
			scan
		}
	}

	fn root(&mut self, plan: &'a Plan) -> ExplainNode {
		match plan {
			Plan::TableScan {
				table,
				direction,
				topk,
				..
			} => {
				let mut attrs = vec![
					("table".to_string(), table.to_string()),
					("direction".to_string(), format!("{direction:?}")),
				];
				let prefiltered = self.prefilter.take();
				if let Some(predicate) = &prefiltered {
					attrs.push(("predicate".to_string(), predicate.clone()));
				}
				attrs.extend(self.paging.take().unwrap_or_default());
				if prefiltered.is_some() {
					attrs.push(("pre_decode_filter".to_string(), "yes".to_string()));
				}
				if topk.is_some() {
					attrs.push(("topk_pushdown".to_string(), "yes".to_string()));
				}
				self.node("TableScan", ContextLevel::Database, attrs, vec![])
			}
			Plan::IndexScan {
				table,
				index,
				access,
				direction,
				..
			} => self.btree_scan(table, index, access, *direction),
			Plan::IndexLookup {
				table,
				index,
				field,
				key,
				..
			} => {
				let mut attrs = vec![
					("table".to_string(), table.to_string()),
					("index".to_string(), index.name.to_string()),
					("access".to_string(), format!("{field} = {}", key.expr().to_sql())),
				];
				attrs.extend(self.paging.take().unwrap_or_default());
				self.node("IndexLookup", ContextLevel::Database, attrs, vec![])
			}
			Plan::IndexUnion {
				table,
				branches,
				merge,
				..
			} => {
				let children = branches
					.iter()
					.map(|b| {
						match &b.read {
							BranchRead::BTree(access) => {
								self.btree_scan(table, &b.index, access, b.direction)
							}
							BranchRead::FullText {
								query,
								..
							} => self.node(
								"FullTextScan",
								ContextLevel::Database,
								vec![
									("table".to_string(), table.to_string()),
									("index".to_string(), b.index.name.to_string()),
									("query".to_string(), query.clone()),
								],
								vec![],
							),
						}
						.shared()
					})
					.collect();
				let mut attrs = vec![
					("table".to_string(), table.to_string()),
					("branches".to_string(), branches.len().to_string()),
				];
				match merge {
					Some(MergeMode::ById(direction)) => {
						attrs.push(("merge_by_id".to_string(), format!("{direction:?}")));
					}
					Some(MergeMode::ByIndexKey {
						path,
						direction,
					}) => {
						attrs.push((
							"merge_by_index_key".to_string(),
							format!("{path} {direction:?}"),
						));
					}
					Some(MergeMode::ByIndexKeyDedup {
						path,
						direction,
					}) => {
						attrs.push((
							"merge_by_index_key_dedup".to_string(),
							format!("{path} {direction:?}"),
						));
					}
					None => {}
				}
				self.node("UnionIndexScan", ContextLevel::Database, attrs, children)
			}
			Plan::Bitmap {
				table,
				root,
				fallback,
				..
			} => {
				let bitmap = crate::exec::planner::bitmap_plan_to_node(root.clone());
				let mut taken = None;
				let children = if self.measure {
					let cell = Arc::new(AtomicBool::new(false));
					self.measured.push(Arc::new(Measured::Fusion {
						table: table.clone(),
						bitmap: Arc::clone(&bitmap),
						anchored: fallback.is_none(),
						fallback: Arc::clone(&cell),
					}));
					taken = Some(cell);
					vec![bitmap as Arc<dyn ExecOperator>]
				} else if self.metrics.is_some() {
					vec![self.described(bitmap.as_ref()).shared()]
				} else {
					vec![bitmap as Arc<dyn ExecOperator>]
				};
				let mut node = self.node(
					"BitmapScan",
					ContextLevel::Database,
					vec![("table".to_string(), table.to_string())],
					children,
				);
				node.fallback = taken;
				node
			}
			Plan::FullTextScan {
				table,
				index,
				query,
				..
			} => self.node(
				"FullTextScan",
				ContextLevel::Database,
				vec![
					("table".to_string(), table.to_string()),
					("index".to_string(), index.name.to_string()),
					("query".to_string(), query.clone()),
				],
				vec![],
			),
			Plan::KnnScan {
				table,
				index,
				vector,
				k,
				ef,
				filter,
				ids_only,
				..
			} => {
				let mut attrs = vec![
					("index".to_string(), index.name.to_string()),
					("k".to_string(), k.to_string()),
					("ef".to_string(), ef.to_string()),
					("dimension".to_string(), vector.len().to_string()),
				];
				// A run that built its rows from the ids the search returned, and
				// whose search judged no candidate on its record, read no record.
				if *ids_only && filter.is_none() && self.metrics.is_some() {
					attrs.push(("records".to_string(), "not read".to_string()));
				}
				// The conjunct the search judges on each candidate: what the
				// allow-list does not cover when there is one.
				let prefiltered = filter
					.as_ref()
					.and_then(|filter| filter.prefilter.as_ref())
					.filter(|_| index.definition().uses_shared_doc_ids());
				let judged = match (prefiltered, filter) {
					(Some(prefilter), _) => prefilter.residual.as_ref(),
					(None, Some(filter)) => filter.residual.as_ref(),
					(None, None) => None,
				};
				if let Some(judged) = judged {
					attrs.push(("predicate".to_string(), judged.to_sql()));
				}
				let mut tier = None;
				let children = prefiltered
					.map(|prefilter| {
						let bitmap =
							crate::exec::planner::bitmap_plan_to_node(prefilter.root.clone());
						if self.measure {
							let cell = Arc::new(AtomicU8::new(0));
							self.measured.push(Arc::new(Measured::Prefilter {
								table: table.clone(),
								bitmap: Arc::clone(&bitmap),
								tier: Arc::clone(&cell),
								versioned: self.versioned,
							}));
							tier = Some(cell);
							vec![bitmap as Arc<dyn ExecOperator>]
						} else if self.metrics.is_some() {
							vec![self.described(bitmap.as_ref()).shared()]
						} else {
							vec![bitmap as Arc<dyn ExecOperator>]
						}
					})
					.unwrap_or_default();
				let mut node = self.node("KnnScan", ContextLevel::Database, attrs, children);
				node.tier = tier;
				node
			}
			Plan::KnnTopK {
				input,
				field,
				vector,
				k,
				distance,
			} => {
				let child = self.root(input).shared();
				self.node(
					"KnnTopK",
					ContextLevel::Database,
					vec![
						("field".to_string(), field.to_sql()),
						("k".to_string(), k.to_string()),
						("distance".to_string(), format!("{distance:?}")),
						(
							"dimension".to_string(),
							match vector {
								super::plan::KnnTarget::Literal(vector) => vector.len().to_string(),
								super::plan::KnnTarget::Deferred(_) => "deferred".to_string(),
							},
						),
					],
					vec![child],
				)
			}
			Plan::RecordScan {
				record,
				for_update,
				..
			} => {
				let mut attrs = vec![("record_id".to_string(), record.to_sql())];
				if *for_update {
					attrs.push(("for_update".to_string(), "true".to_string()));
				}
				self.node("RecordIdScan", ContextLevel::Database, attrs, vec![])
			}
			Plan::Values {
				expr,
				read,
			} => {
				let mut attrs = vec![("expr".to_string(), expr.to_sql())];
				if matches!(read, ValuesRead::Locked) {
					attrs.push(("for_update".to_string(), "true".to_string()));
				}
				self.node("Values", ContextLevel::Root, attrs, vec![])
			}
			Plan::Count {
				count,
				..
			} => match count {
				CountDecision::All {
					source,
					..
				} => {
					let source = match source {
						SourceRef::Table(table) => table.to_string(),
						SourceRef::RecordId(record) => {
							LogicalExpr::RecordId(record.clone()).to_sql()
						}
						_ => String::new(),
					};
					self.node(
						"CountScan",
						ContextLevel::Database,
						vec![("source".to_string(), source)],
						vec![],
					)
				}
				CountDecision::Indexed {
					table,
					cond,
					bitmap_plan,
					..
				} => {
					let children = bitmap_plan
						.iter()
						.map(|plan| {
							let bitmap = crate::exec::planner::bitmap_plan_to_node(plan.clone());
							if self.metrics.is_some() {
								self.described(bitmap.as_ref()).shared()
							} else {
								bitmap as Arc<dyn ExecOperator>
							}
						})
						.collect();
					self.node(
						"IndexCountScan",
						ContextLevel::Database,
						vec![
							("source".to_string(), table.to_string()),
							("condition".to_string(), cond.to_sql()),
						],
						children,
					)
				}
			},
			Plan::Subquery {
				plan,
			} => {
				let outer = (self.nested, self.versioned);
				self.nested = &plan.nested;
				self.versioned = plan.tail.version.is_some();
				let child = self.root(&plan.root).shared();
				(self.nested, self.versioned) = outer;
				self.node("Subquery", ContextLevel::Root, vec![], vec![child])
			}
			Plan::Probe {
				build,
				field,
				outer,
			} => {
				let child = self.root(build).shared();
				self.node(
					"Probe",
					ContextLevel::Database,
					vec![
						("field".to_string(), field.to_string()),
						("outer".to_string(), outer.to_sql()),
					],
					vec![child],
				)
			}
			Plan::Union {
				inputs,
			} => {
				let children = inputs.iter().map(|i| self.root(i).shared()).collect();
				self.node("Union", ContextLevel::Database, vec![], children)
			}
			// The scan's own enforcement is part of the scan it renders.
			Plan::Enforced {
				input,
				..
			} => self.root(input),
			Plan::PermissionFilter {
				input,
				predicate,
				auth,
				hides_access_paths,
				once,
				..
			} => {
				let child = self.root(input).shared();
				// The predicate is the definer's: a caller it is enforced on is
				// shown a placeholder in its place.
				let shown = |predicate: &LogicalExpr| {
					if self.checked {
						"<permission>".to_string()
					} else {
						predicate.to_sql()
					}
				};
				let mut attrs = Vec::new();
				if let Some(once) = once {
					attrs.push(("once".to_string(), shown(once)));
				}
				if once.is_none() || !is_true(predicate) {
					attrs.push(("predicate".to_string(), shown(predicate)));
				}
				if auth.is_some() {
					attrs.push(("auth".to_string(), "limited".to_string()));
				}
				let mut node =
					self.node("PermissionFilter", ContextLevel::Database, attrs, vec![child]);
				node.redacts_access_paths = *hides_access_paths;
				node.hides_metrics = self.checked;
				node
			}
			Plan::Layer {
				input,
				fields,
				..
			} => {
				let child = self.root(input).shared();
				let attrs = layer_attrs(fields, self.checked);
				let mut node =
					self.node("ProjectLayer", ContextLevel::Database, attrs, vec![child]);
				node.hides_metrics = self.checked;
				node
			}
			Plan::Filter {
				input,
				predicate,
				once: None,
			} if prefiltered_scan(input, predicate) => {
				// The scan judges the predicate of each record's stored bytes
				// before it decodes the record, and the body judges it again
				// of the records that remain.
				self.prefilter = Some(predicate_text(predicate));
				let node = self.root(input);
				self.prefilter = None;
				node
			}
			Plan::Filter {
				input,
				predicate,
				once: None,
			} if rechecking_scan(input, predicate) => {
				// The scan lists the records whose index entries lie in its key
				// range, which answers every conjunct of the predicate; the
				// filter is the scan's own check of each record it fetches.
				self.root(input)
			}
			Plan::Filter {
				input,
				predicate,
				once,
			} => {
				let mut children = vec![self.root(input).shared()];
				children.extend(self.memberships(predicate));
				let mut attrs = Vec::new();
				// The conjuncts judged once per run of the statement, before
				// those judged per row.
				if let Some(once) = once {
					attrs.push(("once".to_string(), predicate_text(once)));
				}
				if once.is_none() || !is_true(predicate) {
					attrs.push(("predicate".to_string(), predicate_text(predicate)));
				}
				self.node("Filter", ContextLevel::Database, attrs, children)
			}
			Plan::Fetch {
				input,
				fetches,
			} => {
				let child = self.root(input).shared();
				self.node(
					"Fetch",
					ContextLevel::Database,
					vec![(
						"fetches".to_string(),
						fetches.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(", "),
					)],
					vec![child],
				)
			}
			Plan::Step {
				input,
				step,
			} => {
				use super::binding::BindingStep;
				let mut attrs = Vec::new();
				let mut children = Vec::new();
				match &**step {
					BindingStep::Bind {
						name,
					} => attrs.push(("binding".to_string(), name.clone())),
					BindingStep::Expand(expand) => {
						let tables = if expand.edge_tables.is_empty() {
							"*".to_string()
						} else {
							expand
								.edge_tables
								.iter()
								.map(|table| table.as_str())
								.collect::<Vec<_>>()
								.join(", ")
						};
						attrs.push(("source".to_string(), expand.source.clone()));
						attrs.push((
							"direction".to_string(),
							match expand.direction {
								crate::exec::operators::ExpandDir::Out => "->",
								crate::exec::operators::ExpandDir::In => "<-",
							}
							.to_string(),
						));
						attrs.push(("tables".to_string(), tables));
						attrs.push((
							"edge".to_string(),
							match &expand.edge_binding {
								crate::exec::operators::EdgeBinding::Full(name)
								| crate::exec::operators::EdgeBinding::IdOnly(name) => name.clone(),
							},
						));
						attrs.push(("target".to_string(), expand.target_binding.clone()));
						if let Some(label) = &expand.target_label {
							attrs.push(("target_label".to_string(), label.as_str().to_string()));
						}
						if let Some(predicate) = &expand.predicate {
							attrs.push(("predicate".to_string(), predicate_text(predicate)));
						}
					}
					BindingStep::Endpoint(endpoint) => {
						attrs.push(("edge".to_string(), endpoint.edge.clone()));
						attrs.push(("field".to_string(), endpoint.field.field().to_string()));
						attrs.push(("node".to_string(), endpoint.name.clone()));
						if let Some(label) = &endpoint.label {
							attrs.push(("target_label".to_string(), label.as_str().to_string()));
						}
					}
					BindingStep::DistinctEdges {
						edges,
					} => attrs.push(("edges".to_string(), edges.join(", "))),
					BindingStep::Distinct => {}
					BindingStep::Drain => {}
					BindingStep::Path(path) => {
						let tables = if path.edge_tables.is_empty() {
							"*".to_string()
						} else {
							path.edge_tables
								.iter()
								.map(|table| table.as_str())
								.collect::<Vec<_>>()
								.join(", ")
						};
						attrs.push(("source".to_string(), path.source.clone()));
						attrs.push((
							"direction".to_string(),
							match path.direction {
								crate::exec::operators::ExpandDir::Out => "->",
								crate::exec::operators::ExpandDir::In => "<-",
							}
							.to_string(),
						));
						attrs.push(("tables".to_string(), tables));
						attrs.push(("min".to_string(), path.min.to_string()));
						attrs.push((
							"max".to_string(),
							path.max.map(|max| max.to_string()).unwrap_or_else(|| "∞".to_string()),
						));
						attrs.push(("target_binding".to_string(), path.target_binding.clone()));
						if let Some(selector) = path.selector {
							attrs.push(("search".to_string(), selector.attr()));
						}
						if let Some(label) = &path.target_label {
							attrs.push(("target_label".to_string(), label.as_str().to_string()));
						}
						if let Some(group) = &path.group_binding {
							attrs.push(("group".to_string(), group.clone()));
						}
						if let Some(binding) = &path.path_binding {
							attrs.push(("path".to_string(), binding.clone()));
						}
						if let Some(mode) = path.mode.attr() {
							attrs.push(("mode".to_string(), mode.to_string()));
						}
						if path.reversed {
							attrs.push(("reversed".to_string(), "true".to_string()));
						}
					}
					BindingStep::Mutate(mutation) => {
						use super::binding::MutationStep;
						use crate::exec::operators::mutate::UpdateOp;
						match mutation {
							MutationStep::Update {
								target,
								op,
							} => {
								attrs.push(("binding".to_string(), target.clone()));
								attrs.push((
									"op".to_string(),
									match op {
										UpdateOp::Set(_) => "set",
										UpdateOp::Unset(_) => "unset",
										UpdateOp::Content(_) => "content",
									}
									.to_string(),
								));
							}
							MutationStep::Delete {
								target,
								detach,
								..
							} => {
								attrs.push(("binding".to_string(), target.clone()));
								attrs.push((
									"mode".to_string(),
									match detach {
										crate::expr::match_plan::DetachMode::Detach => "detach",
										crate::expr::match_plan::DetachMode::NoDetach => "nodetach",
									}
									.to_string(),
								));
							}
							MutationStep::Insert {
								nodes,
								edges,
							} => {
								attrs.push(("nodes".to_string(), nodes.len().to_string()));
								attrs.push(("edges".to_string(), edges.len().to_string()));
							}
						}
					}
					BindingStep::Join(join) => {
						attrs.push(("type".to_string(), join.join_type.label().to_string()));
						if !join.keys.is_empty() {
							attrs.push(("keys".to_string(), join.keys.join(", ")));
						}
						if !join.null_template.is_empty() {
							attrs
								.push(("null_template".to_string(), join.null_template.join(", ")));
						}
						if let Some(residual) = &join.residual {
							attrs.push(("residual".to_string(), predicate_text(residual)));
						}
						children.push(self.root(&join.build).shared());
					}
				}
				children.push(self.root(input).shared());
				self.node(step.name(), ContextLevel::Database, attrs, children)
			}
			Plan::Split {
				input,
				targets,
			} => {
				let child = self.root(input).shared();
				self.node(
					"Split",
					ContextLevel::Database,
					vec![(
						"targets".to_string(),
						targets.iter().map(|t| t.to_sql()).collect::<Vec<_>>().join(", "),
					)],
					vec![child],
				)
			}
			Plan::Sort {
				input,
				keys,
				keep,
				random,
				..
			} => {
				let child = self.root(input).shared();
				let mut attrs = vec![];
				if *random {
					attrs.push(("sort_keys".to_string(), "RAND()".to_string()));
				} else if !keys.is_empty() {
					let keys: Vec<String> = keys
						.iter()
						.map(|key| {
							let read = match &key.read {
								SortKeyRead::Path(path) => path.to_string(),
								SortKeyRead::Computed(expr) => expr.to_sql(),
							};
							let mut text = format!(
								"{read} {}",
								if key.ascending {
									"ASC"
								} else {
									"DESC"
								}
							);
							if key.collate {
								text.push_str(" COLLATE");
							}
							if key.numeric {
								text.push_str(" NUMERIC");
							}
							text
						})
						.collect();
					attrs.push(("sort_keys".to_string(), keys.join(", ")));
				}
				if let Some(k) = keep {
					attrs.push(("keep".to_string(), k.to_string()));
				} else if self.tempfiles && !*random {
					attrs.push(("tempfiles".to_string(), "true".to_string()));
				}
				self.node("Sort", ContextLevel::Database, attrs, vec![child])
			}
			Plan::Aggregate {
				input,
				group,
				..
			} => {
				let child = self.root(input).shared();
				let attrs = if group.is_empty() {
					vec![("mode".to_string(), "GROUP ALL".to_string())]
				} else {
					vec![(
						"by".to_string(),
						group.iter().map(|g| g.to_sql()).collect::<Vec<_>>().join(", "),
					)]
				};
				self.node("Aggregate", ContextLevel::Database, attrs, vec![child])
			}
			Plan::Distinct {
				input,
				..
			} => {
				let child = self.root(input).shared();
				self.node("Distinct", ContextLevel::Database, vec![], vec![child])
			}
			Plan::Join {
				left,
				right,
				kind,
				keys,
				residual,
				alias,
				lookup,
				build: held,
				..
			} => {
				let looked_up = lookup.as_ref().map(|lookup| self.root(&lookup.right).shared());
				let build = self.root(right).shared();
				let probe = self.root(left).shared();
				let mut attrs = vec![
					(
						"join_type".to_string(),
						match kind {
							super::plan::JoinMatch::Inner => "Inner",
							super::plan::JoinMatch::Left => "Left",
							super::plan::JoinMatch::Semi => "Semi",
						}
						.to_string(),
					),
					("alias".to_string(), alias.clone()),
				];
				if !keys.is_empty() {
					attrs.push((
						"keys".to_string(),
						keys.iter()
							.map(|(left, right)| format!("{} = {}", left.to_sql(), right.to_sql()))
							.collect::<Vec<_>>()
							.join(", "),
					));
				}
				if let Some(residual) = residual {
					attrs.push(("residual".to_string(), residual.to_sql()));
				}
				if *held == super::plan::JoinBuild::Left {
					// The left input whose one row the join holds, then the
					// right input it streams past that row.
					return self.node(
						"SingleRowJoin",
						ContextLevel::Database,
						attrs,
						vec![probe, build],
					);
				}
				match looked_up {
					// The lookup's right input, the hash join's it hands the
					// rest of the left rows to, and the left input.
					Some(looked_up) => {
						attrs.push((
							"lookups".to_string(),
							format!("{}, then hash", super::plan::LOOKUP_EVALUATIONS),
						));
						self.node(
							"IndexLoopJoin",
							ContextLevel::Database,
							attrs,
							vec![looked_up, build, probe],
						)
					}
					None => {
						self.node("HashJoin", ContextLevel::Database, attrs, vec![build, probe])
					}
				}
			}
			Plan::Limit {
				input,
				limit,
				start,
			} => {
				let mut attrs = vec![];
				if let Some(l) = limit {
					attrs.push(("limit".to_string(), l.to_sql()));
				}
				if let Some(s) = start {
					attrs.push(("offset".to_string(), s.to_sql()));
				}
				// The program's pipeline stops reading the scan once the page
				// is full, so a limit that stands over the scan, or over the
				// layers of its table alone, is the scan's own paging.
				if limit.is_some() && takes_paging(input) {
					self.paging = Some(attrs);
					let node = self.root(input);
					self.paging = None;
					return node;
				}
				let child = self.root(input).shared();
				self.node("Limit", ContextLevel::Database, attrs, vec![child])
			}
			Plan::Project {
				input,
				projection,
			} => {
				let (name, attrs) = match projection {
					Projection::All => {
						("SelectProject", vec![("projections".to_string(), "*".to_string())])
					}
					Projection::Value(expr) => {
						("ProjectValue", vec![("expr".to_string(), expr.to_sql())])
					}
					Projection::OmittedValue {
						expr,
						omit,
					} => (
						"ProjectValue",
						vec![
							("expr".to_string(), expr.to_sql()),
							(
								"omit".to_string(),
								omit.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "),
							),
						],
					),
					Projection::SortedValue(_) => ("ProjectValue", vec![]),
					Projection::Star {
						fields,
						omit,
					} => {
						let mut p = "*".to_string();
						for f in fields {
							p.push_str(&format!(", {}", f.output));
						}
						let mut attrs = vec![("projections".to_string(), p)];
						if !omit.is_empty() {
							attrs.push((
								"omit".to_string(),
								omit.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "),
							));
						}
						("SelectProject", attrs)
					}
					Projection::Fields {
						fields,
						omit,
					} => {
						// A field landing at a nested output path needs the full
						// operator's object construction, which lists nothing.
						if fields.iter().any(|f| f.output.0.len() > 1) {
							("Project", Vec::new())
						} else {
							let p = fields
								.iter()
								.map(|f| f.output.to_string())
								.collect::<Vec<_>>()
								.join(", ");
							let mut attrs = vec![("projections".to_string(), p)];
							if !omit.is_empty() {
								attrs.push((
									"omit".to_string(),
									omit.iter()
										.map(|p| p.to_string())
										.collect::<Vec<_>>()
										.join(", "),
								));
							}
							("SelectProject", attrs)
						}
					}
				};
				let child = self.root(input).shared();
				self.node(name, ContextLevel::Database, attrs, vec![child])
			}
			Plan::Eval {
				expr,
				fetches,
			} => {
				let node = self.node(
					"ExprPlan",
					ContextLevel::Root,
					vec![("expr".to_string(), expr.to_sql())],
					vec![],
				);
				if fetches.is_empty() {
					node
				} else {
					self.node(
						"Fetch",
						ContextLevel::Database,
						vec![(
							"fetches".to_string(),
							fetches.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(", "),
						)],
						vec![node.shared()],
					)
				}
			}
			Plan::Ddl {
				..
			} => self.node("DdlPlan", ContextLevel::Root, vec![], vec![]),
			Plan::Info {
				..
			} => self.node("Info", ContextLevel::Database, vec![], vec![]),
			Plan::Control {
				control,
			} => self.control(control),
			Plan::Refused {
				..
			} => self.node("Refused", ContextLevel::Root, vec![], vec![]),
			Plan::Write {
				write,
				subjects,
			} => {
				let children = match subjects {
					WriteSubjects::Rows(input)
					| WriteSubjects::Guaranteed {
						rows: input,
						..
					} => vec![self.root(input).shared()],
					_ => vec![],
				};
				self.node(
					"WriteOperator",
					ContextLevel::Database,
					vec![("form".to_string(), write.statement.kind().to_string())],
					children,
				)
			}
		}
	}

	/// `operator`, and every operator beneath it, as nodes reporting this
	/// walk's metrics.
	fn described(&self, operator: &dyn ExecOperator) -> ExplainNode {
		let children = operator
			.explain_children()
			.into_iter()
			.map(|child| self.described(child.as_ref()).shared())
			.collect();
		self.node(operator.name(), operator.required_context(), operator.attrs(), children)
	}
}

/// The statements `predicate` tests membership in: of each conjunct that is
/// `x INSIDE (statement)` or `x NOT INSIDE (statement)`, the statement.
fn membership_statements<'a>(predicate: &'a LogicalExpr, out: &mut Vec<&'a LogicalOp>) {
	match predicate {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => {
			membership_statements(left, out);
			membership_statements(right, out);
		}
		LogicalExpr::Binary {
			op: BinaryOperator::Inside | BinaryOperator::NotInside,
			right,
			..
		} => {
			if let LogicalExpr::Subquery(statement) = right.as_ref() {
				out.push(statement);
			}
		}
		_ => {}
	}
}

/// The text of a filter's `predicate`. A statement a conjunct tests membership
/// in is shown as `(subquery)`: the plan of the statement is the `Subquery`
/// child the filter holds.
fn predicate_text(predicate: &LogicalExpr) -> String {
	let mut found = Vec::new();
	membership_statements(predicate, &mut found);
	if found.is_empty() {
		return predicate.to_sql();
	}
	conjunct_text(predicate)
}

fn conjunct_text(predicate: &LogicalExpr) -> String {
	match predicate {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => format!("{} AND {}", conjunct_text(left), conjunct_text(right)),
		LogicalExpr::Binary {
			left,
			op: op @ (BinaryOperator::Inside | BinaryOperator::NotInside),
			right,
		} if matches!(right.as_ref(), LogicalExpr::Subquery(_)) => {
			let word = if matches!(op, BinaryOperator::Inside) {
				"INSIDE"
			} else {
				"NOT INSIDE"
			};
			format!("{} {word} (subquery)", left.to_sql())
		}
		LogicalExpr::Binary {
			op: BinaryOperator::Or,
			..
		} => format!("({})", predicate.to_sql()),
		other => other.to_sql(),
	}
}

/// Whether `predicate`, the filter directly over `input`, is judged by the
/// table scan `input` of each record's stored bytes: a comparison of a static
/// field path with a literal, or a conjunction, disjunction or negation of
/// such. The rows of a scan under no enforcement are the stored records, so
/// the predicate reads what the bytes hold; the lowering attaches the shape to
/// the scan under the same test.
/// Whether `predicate` is the literal `true`: what a filter whose every
/// conjunct is judged once judges per row.
fn is_true(predicate: &LogicalExpr) -> bool {
	matches!(predicate, LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Bool(true)))
}

fn prefiltered_scan(input: &Plan, predicate: &LogicalExpr) -> bool {
	matches!(input, Plan::TableScan { .. })
		&& crate::exec::pre_decode_filter::PreDecodeShape::of(predicate).is_some()
}

/// Whether `predicate`, the filter directly over `input`, is answered in full
/// by the key range of the B-tree scan `input`: the conjuncts the access path
/// was chosen from are all there is to judge.
fn rechecking_scan(input: &Plan, predicate: &LogicalExpr) -> bool {
	let Plan::IndexScan {
		index,
		access,
		..
	} = input
	else {
		return false;
	};
	crate::exec::planner::util::strip_index_conditions(predicate, access, &index.cols).is_none()
}

/// Whether `plan` is a scan of one table, beneath the layers of its table
/// alone, that reads each record it lists once: a table scan, or a B-tree scan
/// whose key range does not reach a record through several entries. A layer
/// drops no row, so the rows a limit over such a plan counts are the scan's.
fn takes_paging(plan: &Plan) -> bool {
	match plan {
		Plan::TableScan {
			..
		} => true,
		Plan::Filter {
			input,
			predicate,
			once: None,
		} => {
			prefiltered_scan(input, predicate)
				|| (rechecking_scan(input, predicate) && takes_paging(input))
		}
		Plan::IndexScan {
			index,
			access,
			..
		} => !crate::exec::index::access_path::access_repeats_records(index, access),
		Plan::Layer {
			input,
			..
		}
		| Plan::Enforced {
			input,
			..
		} => takes_paging(input),
		_ => false,
	}
}

/// The attributes of a projection layer over `fields`: one rendered column per
/// entry, `name = <expression> [: kind] @frame`. A permission predicate and a
/// computed body are the definer's, so a `checked` caller is shown
/// `<permission>` or `<computed>` where the expression stands, and no text of
/// either.
fn layer_attrs(
	fields: &crate::expr::logical::LogicalFields,
	checked: bool,
) -> Vec<(String, String)> {
	use crate::expr::logical::EvalMode;
	let Ok(columns) = crate::exec::planner::fold::layer_columns(fields.clone()) else {
		return Vec::new();
	};
	let columns: Vec<String> = columns
		.into_iter()
		.map(|column| {
			let mut text = format!("{} = ", column.name);
			match (column.frame.mode, &column.guard) {
				(EvalMode::Permission | EvalMode::MutablePermission, _) if checked => {
					text.push_str("<permission>");
				}
				(EvalMode::Computed, _) if checked => text.push_str("<computed>"),
				(_, Some(guard)) => text.push_str(&format!(
					"IF {} THEN {} ELSE NONE END",
					guard.to_sql(),
					column.value.to_sql()
				)),
				(_, None) => text.push_str(&column.value.to_sql()),
			}
			if let Some(kind) = &column.kind {
				text.push_str(&format!(" : {}", kind.to_sql()));
			}
			text.push_str(match column.frame.mode {
				EvalMode::Caller => " @caller",
				EvalMode::Permission => " @permission",
				EvalMode::MutablePermission => " @mutable-permission",
				EvalMode::Computed => " @computed",
			});
			text
		})
		.collect();
	vec![("columns".to_string(), columns.join(", "))]
}

/// The attributes of an index scan of `table` through `index`: the key range
/// it reads, which an open range does not name, and the direction it reads in.
fn index_scan_attrs(
	table: &TableName,
	index: &str,
	access: &crate::exec::index::access_path::BTreeAccess,
	direction: crate::kvs::Direction,
) -> Vec<(String, String)> {
	let mut attrs =
		vec![("table".to_string(), table.to_string()), ("index".to_string(), index.to_string())];
	let access = access.describe();
	if !access.is_empty() {
		attrs.push(("access".to_string(), access));
	}
	attrs.push(("direction".to_string(), format!("{direction:?}")));
	attrs
}

/// Convert a [`PhysicalPlan`] into an [`ExecOperator`] tree for EXPLAIN rendering.
///
/// Given the operator `exec` that runs the statement, the tree is the one
/// `EXPLAIN ANALYZE` runs: it executes `exec`, every node reports the metrics
/// of that run, and the bitmap trees of its scans are evaluated when it ends.
///
/// `explained` is the statement `physical` was planned from by
/// [`map::explained_plan`](super::map::explained_plan): a `LET` is rendered as the binding of its
/// name over the plan of its value.
pub(crate) fn to_explain_operator(
	explained: &LogicalOp,
	physical: &PhysicalPlan,
	exec: Option<&Arc<dyn ExecOperator>>,
	checked: bool,
) -> Arc<dyn ExecOperator> {
	let metrics = exec.is_some().then(|| Arc::new(OperatorMetrics::new()));
	let mut walk = Explainer {
		metrics: metrics.as_ref(),
		measure: exec.is_some(),
		versioned: false,
		tempfiles: false,
		nested: &[],
		measured: Vec::new(),
		checked,
		paging: None,
		prefilter: None,
	};
	let mut root = walk.plan(physical);
	if let LogicalOp::Let(stmt) = explained {
		root = ExplainNode::new(
			"Let",
			root.context,
			vec![("name".to_string(), format!("${}", stmt.name))],
			vec![root.shared()],
			metrics.as_ref(),
		);
	}
	if let Some(exec) = exec {
		root.exec = Some(Arc::clone(exec));
		root.measured = std::mem::take(&mut walk.measured);
	}
	Arc::new(root)
}

/// The name of the operator at the top of the plan `EXPLAIN` renders for
/// `physical`.
pub(crate) fn root_name(physical: &PhysicalPlan) -> &'static str {
	let mut walk = Explainer {
		metrics: None,
		measure: false,
		versioned: false,
		tempfiles: false,
		nested: &[],
		measured: Vec::new(),
		checked: false,
		paging: None,
		prefilter: None,
	};
	walk.plan(physical).name
}

/// The tree of the plan for `EXPLAIN ANALYZE` of a statement a program runs
/// as a group of its own: every node reports the one set of metrics that run
/// records, and a bitmap tree is described by its nodes alone.
pub(crate) fn to_analysis_operator(
	physical: &PhysicalPlan,
	checked: bool,
) -> Arc<dyn ExecOperator> {
	let metrics = Arc::new(OperatorMetrics::new());
	let mut walk = Explainer {
		metrics: Some(&metrics),
		measure: false,
		versioned: false,
		tempfiles: false,
		nested: &[],
		measured: Vec::new(),
		checked,
		paging: None,
		prefilter: None,
	};
	Arc::new(walk.plan(physical))
}
