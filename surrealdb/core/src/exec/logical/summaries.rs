//! What the catalog's definitions do, as the effects the logical layer folds into legality.
//!
//! A pipeline's legality ([`super::pipelines`]) is derived from the effects of the expressions it
//! evaluates. Some of those expressions are not in the plan: a stored function is called by name,
//! and a `COMPUTED` field is derived from a body the catalog holds. [`CatalogSummaries`] states
//! what each such definition does, read from the catalog the statement is planned against, so that
//! the fold sees it as it sees a builtin's declaration.
//!
//! A summary is an [`EffectSet`] with one more answer, [`Summary::UNKNOWN`], the top of the
//! lattice: what the layer cannot see all of. It refuses everything a set of effects refuses.
//!
//! # What a summary holds
//!
//! The effects of the body: its expressions and statements, the implementations of the methods it
//! calls, and the summaries of the functions it calls, transitively. A body that writes also has
//! what the writes run on the tables it names: the table's synchronous events, the `VALUE`,
//! `ASSERT` and `DEFAULT` clauses of its fields, and the same for every table a view over it
//! writes. The catalog is read precisely for these: a table that runs events is not unknown for
//! that alone.
//!
//! A definition is [`Summary::UNKNOWN`] when the layer cannot state it:
//!
//! - its body cannot be read at the planned catalog version, is not in logical form, or is
//!   recursive;
//! - it runs a body it cannot see: a script, module, silo or model function, a value invoked, a
//!   closure handed to a builtin that is not written in the call;
//! - it writes a table that is not named where it is written, deletes records (whose reference
//!   actions run on any table), changes the catalog, or holds a `MATCH` plan;
//! - what a write runs is an asynchronous event, whose work is deferred out of the statement;
//! - the statement changes the catalog, so a definition it reads may not be the one a call reaches.
//!
//! The reads of a body that follow a record link, name a table from a value, or evaluate against
//! the caller's row are not writes: they add no effect a refusal names, and so they do not make a
//! summary unknown.
//!
//! # What a set does not hold
//!
//! A set holds the `COMPUTED` fields of the tables a statement names as sources. A read that
//! follows a record link, a graph edge or a dynamic source derives the `COMPUTED` fields of the
//! records it reaches, whose table the statement does not name, and the effects of deriving them
//! are not folded: a refusal such a field would name, a sequence draw or entropy, is not seen, and
//! the statement's pipelines are placed as if the field were absent.
//!
//! # A call's summary in the plan
//!
//! The summary of a stored function the statement calls does not make the call eligible: the
//! call is still an unseen body ([`SummaryOracle`]), and the summary adds what it does besides.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::Arc;

use surrealdb_strand::TableName;

use super::effects::{SummaryOracle, plan_effects, scalar_effects};
use super::footprint::write_targets;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseId, EventDefinition, FieldDefinition, FunctionDefinition, NamespaceId,
};
use crate::exec::AccessMode;
use crate::exec::function::FunctionRegistry;
use crate::exec::planner::{PlanTimeVersion, Planner};
use crate::expr::Function;
use crate::expr::logical::{
	ComputedBody, EffectSet, LogicalExpr, LogicalOp, LogicalSource, LogicalWriteStatement,
	SourceRef,
};
use crate::kvs::Transaction;

/// What a definition does: the effects of its body, or [`Self::UNKNOWN`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Summary {
	effects: EffectSet,
	unknown: bool,
}

impl Summary {
	/// A definition that does nothing beyond computing its result.
	pub(crate) const PURE: Self = Self {
		effects: EffectSet::PURE,
		unknown: false,
	};

	/// A definition the logical layer cannot state: every effect, and the answer that no set of
	/// effects is.
	pub(crate) const UNKNOWN: Self = Self {
		effects: EffectSet::TOP,
		unknown: true,
	};

	/// The summary of a body that declares `effects`.
	pub(crate) const fn of(effects: EffectSet) -> Self {
		Self {
			effects,
			unknown: false,
		}
	}

	/// The effects the definition has: every effect for an unknown one.
	pub(crate) const fn effects(self) -> EffectSet {
		self.effects
	}

	/// Whether the layer cannot state the definition.
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "the layer's own account of a summary, which tests read")
	)]
	pub(crate) const fn is_unknown(self) -> bool {
		self.unknown
	}

	/// What a body that runs both does.
	pub(crate) const fn union(self, other: Self) -> Self {
		if self.unknown || other.unknown {
			return Self::UNKNOWN;
		}
		Self::of(self.effects.union(other.effects))
	}
}

/// The summaries of the stored functions and `COMPUTED` fields a statement reaches.
///
/// A set that [`Self::none`] makes holds nothing: every function it is asked of is as the
/// registry alone says, an unseen body, and no field is computed. A set that [`Self::of`] reads
/// from the catalog states every definition the statement reaches, and answers
/// [`Summary::UNKNOWN`] for a `COMPUTED` field it was not read for.
#[derive(Clone, Debug, Default)]
pub(crate) struct CatalogSummaries {
	/// Whether the set was read from the catalog.
	built: bool,
	functions: HashMap<String, Summary>,
	computed: HashMap<(TableName, String), Summary>,
	/// The union of the summaries of the fields of one name, whatever their table.
	by_name: HashMap<String, Summary>,
}

impl CatalogSummaries {
	/// A set that states nothing: what a statement planned without a catalog is folded with.
	pub(crate) fn none() -> Self {
		Self::default()
	}

	/// The summary of the stored function `name` (without its `fn::` prefix), or `None` for one
	/// the set does not hold.
	pub(crate) fn function(&self, name: &str) -> Option<Summary> {
		self.functions.get(name).copied()
	}

	/// The summary of the `COMPUTED` field `field` of `table`, or `None` for one that is not.
	pub(crate) fn computed_field(&self, table: &TableName, field: &str) -> Option<Summary> {
		self.computed.get(&(table.clone(), field.to_owned())).copied()
	}

	/// What reading a field named `name` runs: the union of the `COMPUTED` fields of that name of
	/// every table the set holds. Nothing for a name no table computes.
	pub(crate) fn computed_reads(&self, name: &str) -> EffectSet {
		self.by_name.get(name).map_or(EffectSet::PURE, |summary| summary.effects)
	}

	/// Whether a field named `name` is computed in some table the set holds.
	pub(crate) fn reads_computed(&self, name: &str) -> bool {
		self.by_name.contains_key(name)
	}

	/// What deriving the `COMPUTED` field `field` of `table` runs. A field of a table the set was
	/// read for that it holds no summary of is unknown.
	pub(crate) fn computed_of(&self, table: &TableName, field: &str) -> EffectSet {
		match self.computed_field(table, field) {
			Some(summary) => summary.effects,
			None if self.built => Summary::UNKNOWN.effects,
			None => EffectSet::PURE,
		}
	}

	/// What deriving every `COMPUTED` field of `table` runs.
	pub(crate) fn computed_of_table(&self, table: &TableName) -> EffectSet {
		self.computed
			.iter()
			.filter(|((of, _), _)| of == table)
			.fold(EffectSet::PURE, |set, (_, summary)| set.union(summary.effects))
	}

	/// A set holding exactly `functions`, as a catalog read would state them.
	#[cfg(test)]
	pub(crate) fn from_functions(functions: &[(&str, Summary)]) -> Self {
		Self {
			built: true,
			functions: functions
				.iter()
				.map(|(name, summary)| ((*name).to_owned(), *summary))
				.collect(),
			..Self::default()
		}
	}

	/// The summaries of what `op` reaches, read from the catalog `planner` plans against.
	///
	/// The functions are those `op` calls and those the `COMPUTED` fields of its tables call; the
	/// fields are those of the resolved schemas of its table sources. The definitions are read at
	/// the planner's catalog version, through its transaction. Where the planner has no catalog to
	/// read, or its version is only known at run time, or `op` changes the catalog so that a
	/// definition read now may not be the one a later call reaches, every definition is
	/// [`Summary::UNKNOWN`].
	pub(crate) async fn of(planner: &Planner<'_>, op: &LogicalOp) -> Self {
		let registry = planner.function_registry();
		let computed = computed_fields(op);
		let mut reach = Reach::of_op(op);
		for body in computed.values() {
			reach.absorb(Reach::of_expr(&body.expr));
		}
		let roots = reach.calls.clone();
		// What the statement itself writes is no summary's: only what a function or a field it
		// reaches writes is read.
		let seed = Reach {
			calls: reach.calls,
			..Reach::default()
		};

		let catalog = match (planner.txn(), planner.ns_db_ids().await, planner.plan_time_version) {
			(Some(txn), Some((ns, db)), version)
				if !crate::exec::planner::fold::changes_the_catalog(op) =>
			{
				Some(Catalog {
					txn: Arc::clone(txn),
					ns,
					db,
					version: match version {
						PlanTimeVersion::At(stamp) => Some(stamp),
						PlanTimeVersion::Current => None,
					},
				})
			}
			_ => None,
		};

		let mut gather = Gather {
			registry,
			read: catalog.is_some(),
			definitions: HashMap::new(),
			tables: HashMap::new(),
			summaries: Self::default(),
		};
		if let Some(catalog) = &catalog {
			gather.load(catalog, &seed).await;
		}
		let mut stack = Vec::new();
		for name in &roots {
			gather.function(name, &mut stack);
		}
		for (table, field) in computed.keys() {
			gather.computed(table, field, &computed, &mut stack);
		}
		// What the resolver says of the call graph is a floor under the summary: a body it takes
		// to possibly write holds a write.
		for name in &roots {
			if planner.resolve_custom_function(name).await.access_mode == AccessMode::ReadWrite
				&& let Some(summary) = gather.summaries.functions.get_mut(name)
			{
				*summary = summary.union(Summary::of(EffectSet::WRITES_DATA));
			}
		}
		let mut summaries = gather.summaries;
		for ((_, field), summary) in &summaries.computed {
			let merged = summaries.by_name.get(field).map_or(*summary, |held| held.union(*summary));
			summaries.by_name.insert(field.clone(), merged);
		}
		summaries.built = true;
		summaries
	}
}

/// The catalog a set is read from.
struct Catalog {
	txn: Arc<Transaction>,
	ns: NamespaceId,
	db: DatabaseId,
	version: Option<u64>,
}

/// A definition of the catalog a summary is made of.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
	Function(String),
	/// What writing a table runs.
	Table(TableName),
	Computed(TableName, String),
}

/// A definition as read.
enum Definition {
	Function(Arc<FunctionDefinition>),
	Table(Triggers),
	/// A definition that is not there, or cannot be read.
	Unresolved,
}

/// What the catalog holds that runs when a table is written.
struct Triggers {
	events: Arc<[EventDefinition]>,
	fields: Arc<[FieldDefinition]>,
	/// The tables of the views defined over the table.
	views: Vec<TableName>,
}

/// A clause that runs when a table is written.
enum Clause<'a> {
	Expr(&'a LogicalExpr),
	/// A clause with no logical form, or one that runs apart from the statement.
	Unseen,
}

impl Triggers {
	/// The clauses that run when the table is written: its synchronous events' conditions and
	/// bodies, and its fields' `VALUE`, `ASSERT` and `DEFAULT` clauses.
	fn clauses(&self) -> Vec<Clause<'_>> {
		fn clause<'a>(read: anyhow::Result<&'a LogicalExpr>) -> Clause<'a> {
			read.map_or(Clause::Unseen, Clause::Expr)
		}
		let mut out = Vec::new();
		for event in self.events.iter() {
			if event.is_async() {
				out.push(Clause::Unseen);
				continue;
			}
			out.push(clause(event.when_logical()));
			out.extend(event.then_logical().map(clause));
		}
		for field in self.fields.iter() {
			out.extend(field.value_logical().map(clause));
			out.extend(field.assert_logical().map(clause));
			out.extend(field.default_logical().map(clause));
		}
		out
	}
}

/// The `COMPUTED` fields of the resolved table sources of `op`, by table and name.
fn computed_fields(op: &LogicalOp) -> HashMap<(TableName, String), &ComputedBody> {
	let mut out = HashMap::new();
	let _: ControlFlow<()> = op.visit_ops_deep(&mut |node| {
		if let LogicalOp::Source(LogicalSource {
			source,
			schema: Some(schema),
			..
		}) = node
		{
			let table = match source {
				SourceRef::Table(table) => Some(table),
				SourceRef::RecordId(rid) => Some(&rid.table),
				SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => None,
			};
			if let Some(table) = table {
				for field in &schema.fields {
					if let (Some(body), Some(name)) = (&field.computed, field.path.as_field()) {
						out.entry((table.clone(), name.as_str().to_owned())).or_insert(body);
					}
				}
			}
		}
		ControlFlow::Continue(())
	});
	out
}

/// What a body reaches that its summary is made of.
#[derive(Default)]
struct Reach {
	/// The stored functions it calls, without the `fn::` prefix.
	calls: BTreeSet<String>,
	/// The tables it writes.
	tables: BTreeSet<TableName>,
	/// Effects the operators of the body have that no scalar of it declares.
	extra: EffectSet,
	/// Whether it reaches something that is unknown by being there.
	unknown: bool,
}

impl Reach {
	fn of_op(op: &LogicalOp) -> Self {
		let mut reach = Self::default();
		reach.op(op);
		reach
	}

	fn of_expr(expr: &LogicalExpr) -> Self {
		let mut reach = Self::default();
		reach.expr(expr);
		reach
	}

	fn absorb(&mut self, other: Self) {
		self.calls.extend(other.calls);
		self.tables.extend(other.tables);
		self.extra = self.extra.union(other.extra);
		self.unknown |= other.unknown;
	}

	fn op(&mut self, op: &LogicalOp) {
		let _: ControlFlow<()> = op.visit(&mut |node| {
			self.node(node);
			ControlFlow::Continue(())
		});
		let walked: Result<(), Infallible> = op.visit_scalars_deep(&mut |scalar| {
			self.expr(scalar);
			Ok(())
		});
		let Ok(()) = walked;
	}

	fn node(&mut self, node: &LogicalOp) {
		match node {
			LogicalOp::Write(write) => {
				if matches!(write.statement, LogicalWriteStatement::Delete(_)) {
					// A delete runs the reference actions of every table that refers to it.
					self.unknown = true;
				}
				match write_targets(&write.source) {
					Some(tables) => self.tables.extend(tables),
					None => self.unknown = true,
				}
			}
			LogicalOp::Ddl(_) | LogicalOp::Match(_) => self.unknown = true,
			LogicalOp::Sleep(_) => self.extra = self.extra.union(EffectSet::BLOCKING),
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::If(_)
			| LogicalOp::Let(_)
			| LogicalOp::Return(_)
			| LogicalOp::Foreach(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Info(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_) => {}
		}
	}

	fn expr(&mut self, expr: &LogicalExpr) {
		let _: ControlFlow<()> = expr.visit_deep(&mut |inner| {
			if let LogicalExpr::Call(call) = inner {
				match &call.receiver {
					Function::Custom(name) => {
						self.calls.insert(name.clone());
					}
					Function::Script(_)
					| Function::Model(_)
					| Function::Module(..)
					| Function::Silo {
						..
					} => self.unknown = true,
					Function::Normal(_) => {}
				}
			} else if let LogicalExpr::Subquery(plan) = inner {
				let _: ControlFlow<()> = plan.visit(&mut |node| {
					self.node(node);
					ControlFlow::Continue(())
				});
			}
			ControlFlow::Continue(())
		});
	}
}

/// A body whose summary is being made.
#[derive(Clone, Copy)]
enum Body<'a> {
	Op(&'a LogicalOp),
	Expr(&'a LogicalExpr),
}

/// The definitions read, and the summaries made of them.
struct Gather<'a> {
	registry: &'a FunctionRegistry,
	/// Whether the definitions were read from a catalog.
	read: bool,
	definitions: HashMap<Node, Definition>,
	tables: HashMap<TableName, Summary>,
	summaries: CatalogSummaries,
}

impl Gather<'_> {
	/// Read every definition `seed` reaches: the functions it calls, and the triggers of the
	/// tables it writes, and what those reach in turn.
	async fn load(&mut self, catalog: &Catalog, seed: &Reach) {
		let mut queue: VecDeque<Node> = VecDeque::new();
		queue.extend(seed.calls.iter().cloned().map(Node::Function));
		queue.extend(seed.tables.iter().cloned().map(Node::Table));
		let mut seen: HashSet<Node> = HashSet::new();
		while let Some(node) = queue.pop_front() {
			if !seen.insert(node.clone()) {
				continue;
			}
			let (definition, reach) = match &node {
				Node::Function(name) => read_function(catalog, name).await,
				Node::Table(table) => read_triggers(catalog, table).await,
				Node::Computed(..) => continue,
			};
			queue.extend(reach.calls.iter().cloned().map(Node::Function));
			queue.extend(reach.tables.iter().cloned().map(Node::Table));
			if let Definition::Table(triggers) = &definition {
				queue.extend(triggers.views.iter().cloned().map(Node::Table));
			}
			self.definitions.insert(node, definition);
		}
	}

	/// What the stored function `name` does.
	fn function(&mut self, name: &str, stack: &mut Vec<Node>) -> Summary {
		if let Some(summary) = self.summaries.functions.get(name) {
			return *summary;
		}
		let node = Node::Function(name.to_owned());
		if stack.contains(&node) {
			return Summary::UNKNOWN;
		}
		let summary = match self.definitions.get(&node) {
			Some(Definition::Function(definition)) => {
				let definition = Arc::clone(definition);
				stack.push(node);
				let summary = self.definition(&definition, stack);
				stack.pop();
				summary
			}
			Some(Definition::Table(_) | Definition::Unresolved) | None => Summary::UNKNOWN,
		};
		self.summaries.functions.insert(name.to_owned(), summary);
		summary
	}

	/// What a call of the function `definition` does.
	fn definition(&mut self, definition: &FunctionDefinition, stack: &mut Vec<Node>) -> Summary {
		if let Ok(body) = definition.logical_body() {
			return self.body(Body::Op(body), stack);
		}
		// A body with no logical form is read as the facts of its syntax say: what it calls,
		// and the declarations of the builtins and methods it names.
		let facts = definition.block.function_facts_with(Some(self.registry));
		if facts.opaque_effects || facts.unseen_closures || facts.direct_writes {
			return Summary::UNKNOWN;
		}
		let mut summary = Summary::PURE;
		for call in &facts.calls {
			summary = summary.union(self.function(call, stack));
		}
		for name in &facts.builtins {
			summary = summary.union(match self.registry.get(name) {
				Some(function) => Summary::of(function.effects()),
				None => Summary::UNKNOWN,
			});
		}
		for name in &facts.methods {
			summary = summary.union(Summary::of(self.registry.method_effects(name)));
		}
		match summary.effects.contains(EffectSet::OPAQUE_BODY) {
			true => Summary::UNKNOWN,
			false => summary,
		}
	}

	/// What running `body` does: its own effects, those of the functions it calls, and what its
	/// writes run.
	fn body(&mut self, body: Body<'_>, stack: &mut Vec<Node>) -> Summary {
		let mut reach = Reach::default();
		match body {
			Body::Op(op) => reach.op(op),
			Body::Expr(expr) => reach.expr(expr),
		}
		if reach.unknown {
			return Summary::UNKNOWN;
		}
		let mut summary = Summary::PURE;
		for call in &reach.calls {
			summary = summary.union(self.function(call, stack));
		}
		let own = {
			let oracle = SummaryOracle::of_body(self.registry, &self.summaries);
			match body {
				Body::Op(op) => plan_effects(op, &oracle),
				Body::Expr(expr) => scalar_effects(expr, &oracle),
			}
		};
		if own.contains(EffectSet::OPAQUE_BODY) {
			return Summary::UNKNOWN;
		}
		summary = summary.union(Summary::of(own.union(reach.extra)));
		for table in &reach.tables {
			summary = summary.union(self.written(table, stack));
		}
		summary
	}

	/// What running the clauses a write to `table` triggers does.
	fn written(&mut self, table: &TableName, stack: &mut Vec<Node>) -> Summary {
		if let Some(summary) = self.tables.get(table) {
			return *summary;
		}
		let node = Node::Table(table.clone());
		if stack.contains(&node) {
			return Summary::UNKNOWN;
		}
		let summary = match self.definitions.get(&node) {
			Some(Definition::Table(_)) => {
				stack.push(node);
				let summary = self.triggered(table, stack);
				stack.pop();
				summary
			}
			Some(Definition::Function(_) | Definition::Unresolved) | None => Summary::UNKNOWN,
		};
		self.tables.insert(table.clone(), summary);
		summary
	}

	fn triggered(&mut self, table: &TableName, stack: &mut Vec<Node>) -> Summary {
		let Some(Definition::Table(triggers)) =
			self.definitions.remove(&Node::Table(table.clone()))
		else {
			return Summary::UNKNOWN;
		};
		let mut summary = Summary::PURE;
		for clause in triggers.clauses() {
			summary = summary.union(match clause {
				Clause::Expr(expr) => self.body(Body::Expr(expr), stack),
				Clause::Unseen => Summary::UNKNOWN,
			});
		}
		// A view over the table is written by the write, and runs what writing it runs.
		for view in &triggers.views {
			summary =
				summary.union(Summary::of(EffectSet::WRITES_DATA)).union(self.written(view, stack));
		}
		self.definitions.insert(Node::Table(table.clone()), Definition::Table(triggers));
		summary
	}

	/// What deriving the `COMPUTED` field `field` of `table` does.
	fn computed(
		&mut self,
		table: &TableName,
		field: &str,
		fields: &HashMap<(TableName, String), &ComputedBody>,
		stack: &mut Vec<Node>,
	) -> Summary {
		let key = (table.clone(), field.to_owned());
		if let Some(summary) = self.summaries.computed.get(&key) {
			return *summary;
		}
		let node = Node::Computed(key.0.clone(), key.1.clone());
		if stack.contains(&node) {
			return Summary::UNKNOWN;
		}
		let summary = match (self.read, fields.get(&key)) {
			(true, Some(body)) => {
				stack.push(node);
				// A computed field derives the computed fields of its own table that it reads.
				let mut summary = Summary::PURE;
				let mut reads: Vec<String> = Vec::new();
				let _: ControlFlow<()> = body.expr.visit_deep(&mut |inner| {
					if let LogicalExpr::Field(name) = inner
						&& name.as_str() != field
						&& fields.contains_key(&(table.clone(), name.as_str().to_owned()))
					{
						reads.push(name.as_str().to_owned());
					}
					ControlFlow::Continue(())
				});
				for name in &reads {
					summary = summary.union(self.computed(table, name, fields, stack));
				}
				summary = summary.union(self.body(Body::Expr(&body.expr), stack));
				stack.pop();
				summary
			}
			(false, _) | (true, None) => Summary::UNKNOWN,
		};
		self.summaries.computed.insert(key, summary);
		summary
	}
}

/// The stored function `name` as the catalog holds it, and what its body reaches.
async fn read_function(catalog: &Catalog, name: &str) -> (Definition, Reach) {
	let read = catalog.txn.get_db_function(catalog.ns, catalog.db, name, catalog.version).await;
	let definition = match read {
		Ok(definition) => definition,
		Err(_) => return (Definition::Unresolved, Reach::default()),
	};
	let reach = match definition.logical_body() {
		Ok(body) => Reach::of_op(body),
		Err(_) => {
			let facts = definition.block.function_facts();
			Reach {
				calls: facts.calls,
				..Reach::default()
			}
		}
	};
	(Definition::Function(definition), reach)
}

/// What writing `table` triggers as the catalog holds it, and what the clauses reach.
async fn read_triggers(catalog: &Catalog, table: &TableName) -> (Definition, Reach) {
	let (ns, db, version) = (catalog.ns, catalog.db, catalog.version);
	let events = catalog.txn.all_tb_events(ns, db, table, version).await;
	let fields = catalog.txn.all_tb_fields(ns, db, table, version).await;
	let views = catalog.txn.all_tb_views(ns, db, table, version).await;
	let (Ok(events), Ok(fields), Ok(views)) = (events, fields, views) else {
		return (Definition::Unresolved, Reach::default());
	};
	let triggers = Triggers {
		events,
		fields,
		views: views.iter().map(|view| view.name.clone()).collect(),
	};
	let mut reach = Reach::default();
	for clause in triggers.clauses() {
		if let Clause::Expr(expr) = clause {
			reach.expr(expr);
		}
	}
	(Definition::Table(triggers), reach)
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::super::bind;
	use super::super::pipelines::{
		ChainLegality, ChainRefusal, chain_legality_with, pipelines_of_with,
		statement_legality_with,
	};
	use super::super::test_fixture::{Fixture, owner, record_user};
	use super::*;
	use crate::dbs::Session;

	/// Tables, functions and events for the summaries below. `DEFINE FIELD` refuses a COMPUTED body
	/// whose callee advances state when the field is defined, so `fn::draw` takes its sequence
	/// draw after the fields that call it; only the summary sees it.
	pub(super) const SETUP: &str = "
		DEFINE SEQUENCE s;
		DEFINE TABLE task SCHEMALESS;
		DEFINE TABLE log SCHEMALESS;
		DEFINE FUNCTION fn::pure($x: int) { RETURN $x + 1 };
		DEFINE FUNCTION fn::pure_caller($x: int) { RETURN fn::pure($x) * 2 };
		DEFINE FUNCTION fn::noisy() { RETURN rand() };
		DEFINE FUNCTION fn::noisy_caller() { RETURN fn::noisy() };
		DEFINE FUNCTION fn::draw() { LET $n = 1; RETURN 0 };
		DEFINE FUNCTION fn::clocked() { LET $n = 1; RETURN time::now() };
		DEFINE FUNCTION fn::mk() { CREATE log:1 SET at = time::now() };
		DEFINE FUNCTION fn::mk_caller() { RETURN fn::mk() };
		DEFINE FUNCTION fn::note() { LET $n = 1; RETURN $n };
		DEFINE TABLE audited SCHEMALESS;
		DEFINE EVENT noted ON audited WHEN true THEN (fn::note());
		DEFINE FUNCTION fn::audit() { CREATE audited:1 };
		DEFINE TABLE shouting SCHEMALESS;
		DEFINE EVENT shout ON shouting WHEN true THEN (fn::noisy());
		DEFINE FUNCTION fn::shout() { CREATE shouting:1 };
		DEFINE TABLE slow SCHEMALESS;
		DEFINE EVENT lazy ON slow ASYNC WHEN true THEN (CREATE log:2);
		DEFINE FUNCTION fn::slow() { CREATE slow:1 };
		DEFINE FUNCTION fn::by_value($f: any) { RETURN $f(1) };
		DEFINE FUNCTION fn::touch($id: record) { UPDATE $id SET seen = time::now() };
		DEFINE FUNCTION fn::erase() { DELETE log:1 };
		DEFINE FUNCTION fn::ping() { RETURN fn::pong() };
		DEFINE FUNCTION fn::pong() { RETURN fn::ping() };
		DEFINE FUNCTION fn::orphan() { RETURN fn::missing() };
		DEFINE FUNCTION fn::sleepy() { SLEEP 1ms };
		DEFINE FUNCTION fn::fetches() { LET $n = 1; RETURN http::get('http://localhost/') };
		DEFINE TABLE vault SCHEMALESS;
		DEFINE FIELD r ON vault COMPUTED rand();
		DEFINE FIELD twice ON vault COMPUTED n * 2;
		DEFINE FIELD drawn ON vault COMPUTED fn::draw();
		DEFINE FIELD both ON vault COMPUTED r + twice;
		DEFINE FUNCTION OVERWRITE fn::draw() { LET $n = 1; RETURN sequence::nextval('s') };
	";

	/// `sql` bound as `session`, and the summaries read for it.
	pub(in crate::exec::logical) async fn summarised(
		fixture: &Fixture,
		session: &Session,
		sql: &str,
	) -> (LogicalOp, CatalogSummaries) {
		let ast = crate::syn::parse(sql).expect("parses");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let surrealdb_expr::parser::TopLevelExpr::Expr(statement) = top else {
			panic!("a bare expression is expected: {sql}");
		};
		let op = crate::expr::logical::lower(statement).expect("lowers");
		let bound = fixture.bound_as(session, Vec::new()).await;
		let bx = fixture.bind_context(&bound);
		let op = bind(op, &bx).await.expect("binds");
		let registry = FunctionRegistry::with_builtins();
		let planner = Planner::with_txn(
			&bound.ctx,
			&registry,
			Arc::clone(&bound.txn),
			Some("test".to_owned()),
			Some("test".to_owned()),
		);
		let summaries = CatalogSummaries::of(&planner, &op).await;
		(op, summaries)
	}

	/// The summary of each function named in `names`, over a statement that calls them all.
	async fn functions(fixture: &Fixture, names: &[&str]) -> Vec<(String, Summary)> {
		let calls: Vec<String> = names.iter().map(|name| format!("fn::{name}()")).collect();
		let sql = format!("RETURN [{}]", calls.join(", "));
		let (_, summaries) = summarised(fixture, &owner(), &sql).await;
		names
			.iter()
			.map(|name| {
				let summary =
					summaries.function(name).unwrap_or_else(|| panic!("fn::{name} has a summary"));
				((*name).to_owned(), summary)
			})
			.collect()
	}

	fn summary_of(all: &[(String, Summary)], name: &str) -> Summary {
		all.iter().find(|(held, _)| held == name).expect("a summary").1
	}

	/// The effects of `summary` that decide whether a call may run for several rows at once: what
	/// a refusal names, and the clock. The summary holds others, such as what a body reads.
	fn declared(summary: Summary) -> EffectSet {
		[
			EffectSet::WRITES_DATA,
			EffectSet::WRITES_EXTERNAL,
			EffectSet::READS_EXTERNAL,
			EffectSet::ADVANCES_STATE,
			EffectSet::ENTROPY,
			EffectSet::CLOCK,
			EffectSet::BLOCKING,
			EffectSet::OPAQUE_BODY,
		]
		.into_iter()
		.filter(|flag| summary.effects().contains(*flag))
		.fold(EffectSet::PURE, EffectSet::union)
	}

	#[tokio::test]
	async fn a_function_is_summarised_by_what_its_body_does() {
		let fixture = Fixture::new(SETUP).await;
		let all = functions(&fixture, &["noisy", "noisy_caller", "draw", "clocked", "note"]).await;
		let of = |name| summary_of(&all, name);
		assert!(!of("note").is_unknown());
		assert_eq!(declared(of("note")), EffectSet::PURE);
		assert_eq!(declared(of("clocked")), EffectSet::CLOCK);
		assert_eq!(declared(of("noisy")), EffectSet::ENTROPY);
		// A function that calls another has its effects.
		assert_eq!(declared(of("noisy_caller")), EffectSet::ENTROPY);
		assert_eq!(declared(of("draw")), EffectSet::ADVANCES_STATE);
		// Nothing but a clock read: the summary is deterministic but for the clock, as a builtin's.
		assert!(of("clocked").effects().is_deterministic_but_for_clock());
	}

	#[tokio::test]
	async fn a_function_that_writes_is_summarised_as_writes() {
		let fixture = Fixture::new(SETUP).await;
		let all = functions(&fixture, &["mk", "mk_caller", "sleepy"]).await;
		let writes = EffectSet::WRITES_DATA.union(EffectSet::CLOCK);
		for name in ["mk", "mk_caller"] {
			let summary = summary_of(&all, name);
			assert!(!summary.is_unknown(), "{name}: {summary:?}");
			assert_eq!(declared(summary), writes, "{name}");
		}
		let sleepy = summary_of(&all, "sleepy");
		assert!(!sleepy.is_unknown());
		assert_eq!(declared(sleepy), EffectSet::BLOCKING);
	}

	/// What a write runs on the table it names is part of the summary, resolved precisely: an
	/// event that only calls a pure function leaves the summary a write and no more, one that calls
	/// a function drawing entropy adds it.
	#[tokio::test]
	async fn a_write_runs_the_synchronous_events_of_its_table() {
		let fixture = Fixture::new(SETUP).await;
		let all = functions(&fixture, &["audit", "shout"]).await;
		let audit = summary_of(&all, "audit");
		assert!(!audit.is_unknown(), "{audit:?}");
		assert_eq!(declared(audit), EffectSet::WRITES_DATA);
		let shout = summary_of(&all, "shout");
		assert!(!shout.is_unknown(), "{shout:?}");
		assert_eq!(declared(shout), EffectSet::WRITES_DATA.union(EffectSet::ENTROPY));
	}

	#[tokio::test]
	async fn what_the_layer_cannot_see_is_unknown() {
		let fixture = Fixture::new(SETUP).await;
		let all = functions(
			&fixture,
			&["slow", "by_value", "erase", "ping", "pong", "orphan", "fetches"],
		)
		.await;
		// A write whose event is deferred out of the statement.
		assert!(summary_of(&all, "slow").is_unknown());
		// A closure the caller hands in.
		assert!(summary_of(&all, "by_value").is_unknown());
		// A delete runs the reference actions of every table.
		assert!(summary_of(&all, "erase").is_unknown());
		// Recursion, mutual recursion, and a callee the catalog does not define.
		assert!(summary_of(&all, "ping").is_unknown());
		assert!(summary_of(&all, "pong").is_unknown());
		assert!(summary_of(&all, "orphan").is_unknown());
		// A read outside the database is a declared effect, and not unknown.
		let fetches = summary_of(&all, "fetches");
		assert!(!fetches.is_unknown());
		assert_eq!(declared(fetches), EffectSet::READS_EXTERNAL);
	}

	/// A write whose table comes from a value may run any table's events.
	#[tokio::test]
	async fn a_dynamic_write_target_is_unknown() {
		let fixture = Fixture::new(SETUP).await;
		let (_, summaries) = summarised(&fixture, &owner(), "RETURN fn::touch(task:1)").await;
		let touch = summaries.function("touch").expect("a summary");
		assert!(touch.is_unknown(), "{touch:?}");
		assert!(touch.effects().contains(EffectSet::WRITES_DATA));
	}

	#[tokio::test]
	async fn a_catalog_change_in_the_statement_leaves_every_summary_unknown() {
		let fixture = Fixture::new(SETUP).await;
		let (_, summaries) = summarised(
			&fixture,
			&owner(),
			"{ DEFINE FUNCTION OVERWRITE fn::note() { RETURN 2 }; RETURN fn::note() }",
		)
		.await;
		assert_eq!(summaries.function("note"), Some(Summary::UNKNOWN));
	}

	#[tokio::test]
	async fn without_a_catalog_every_function_is_unknown() {
		let fixture = Fixture::new(SETUP).await;
		let (op, _) = summarised(&fixture, &owner(), "RETURN fn::pure_caller(1)").await;
		let bound = fixture.bound_as(&owner(), Vec::new()).await;
		let registry = FunctionRegistry::with_builtins();
		let planner = Planner::new(&bound.ctx, &registry);
		let summaries = CatalogSummaries::of(&planner, &op).await;
		assert_eq!(summaries.function("pure_caller"), Some(Summary::UNKNOWN));
	}

	#[tokio::test]
	async fn a_computed_field_is_summarised_by_its_body() {
		let fixture = Fixture::new(SETUP).await;
		let (_, summaries) = summarised(&fixture, &owner(), "SELECT * FROM vault").await;
		let vault = TableName::new("vault");
		let of = |field| summaries.computed_field(&vault, field).expect("a computed field");
		assert_eq!(declared(of("r")), EffectSet::ENTROPY);
		assert_eq!(declared(of("twice")), EffectSet::PURE);
		assert_eq!(declared(of("drawn")), EffectSet::ADVANCES_STATE);
		// A field computed from others has theirs.
		assert_eq!(declared(of("both")), EffectSet::ENTROPY);
		// A read of a name some table computes has the effects of deriving it.
		assert!(summaries.computed_reads("r").contains(EffectSet::ENTROPY));
		assert!(summaries.reads_computed("twice"));
		assert!(!summaries.reads_computed("n"));
		assert_eq!(summaries.computed_reads("n"), EffectSet::PURE);
	}

	/// The chain of each pipeline of `sql` and the refusal of the statement, as `session` binds it.
	async fn refusals(
		fixture: &Fixture,
		session: &Session,
		sql: &str,
	) -> (Vec<ChainLegality>, Option<ChainRefusal>) {
		let (op, summaries) = summarised(fixture, session, sql).await;
		let registry = FunctionRegistry::with_builtins();
		let chains: Vec<ChainLegality> = pipelines_of_with(&op, &registry, &summaries)
			.into_iter()
			.map(|pipeline| pipeline.legality.chain)
			.collect();
		let statement = statement_legality_with(&op, &registry, &summaries).refusal;
		// A refusal of any chain is a refusal of the statement.
		for chain in &chain_of(&chains) {
			assert!(
				statement.is_some(),
				"`{sql}`: the chain {chain:?} refuses and the statement does not"
			);
		}
		(chains, statement)
	}

	fn chain_of(chains: &[ChainLegality]) -> Vec<ChainLegality> {
		chains.iter().copied().filter(|chain| *chain != ChainLegality::RowLocal).collect()
	}

	/// A stored function is still an unseen body, so a call to it is refused even when its summary
	/// names no effect. Where the summary names effects, they decide which refusal the chain
	/// reports.
	#[tokio::test]
	async fn a_stored_function_refuses_by_what_its_summary_says() {
		use ChainLegality::Refused;
		let fixture = Fixture::new(SETUP).await;
		for (sql, refusal) in [
			("SELECT fn::noisy() FROM task", ChainRefusal::Entropy),
			("SELECT fn::noisy_caller() FROM task", ChainRefusal::Entropy),
			("SELECT fn::draw() FROM task", ChainRefusal::AdvancesState),
			("SELECT fn::mk() FROM task", ChainRefusal::Writes),
			("SELECT fn::mk_caller() FROM task", ChainRefusal::Writes),
			("SELECT fn::audit() FROM task", ChainRefusal::Writes),
			("SELECT fn::sleepy() FROM task", ChainRefusal::Blocks),
			// The call is an unseen body, which the order names before a read outside the
			// snapshot.
			("SELECT fn::fetches() FROM task", ChainRefusal::Opaque),
			("SELECT fn::by_value(1) FROM task", ChainRefusal::Writes),
			("SELECT fn::ping() FROM task", ChainRefusal::Writes),
		] {
			let (chains, statement) = refusals(&fixture, &owner(), sql).await;
			assert_eq!(chains, vec![Refused(refusal)], "{sql}");
			assert_eq!(statement, Some(refusal), "{sql}");
		}
	}

	#[tokio::test]
	async fn a_pure_udf_still_refuses_until_the_owner_confirms() {
		let fixture = Fixture::new(SETUP).await;
		let (op, summaries) =
			summarised(&fixture, &owner(), "SELECT fn::pure_caller(1) FROM task").await;
		let pure = summaries.function("pure_caller").expect("a summary");
		assert!(!pure.is_unknown());
		assert_eq!(declared(pure), EffectSet::PURE);
		let registry = FunctionRegistry::with_builtins();
		let chains: Vec<_> = pipelines_of_with(&op, &registry, &summaries)
			.into_iter()
			.map(|pipeline| pipeline.legality.chain)
			.collect();
		assert_eq!(chains, vec![ChainLegality::Refused(ChainRefusal::Opaque)]);
		assert_eq!(
			statement_legality_with(&op, &registry, &summaries).refusal,
			Some(ChainRefusal::Opaque)
		);
		// The registry alone says the same.
		let call = LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
			receiver: Function::Custom("pure_caller".to_owned()),
			args: Vec::new(),
		}));
		assert_eq!(
			chain_legality_with([&call], &registry, &CatalogSummaries::none()),
			ChainLegality::Refused(ChainRefusal::Opaque)
		);
	}

	/// A function that writes to a table named by its argument may run any table's events.
	#[tokio::test]
	async fn a_function_writing_to_a_dynamic_target_refuses_the_statement() {
		let fixture = Fixture::new(SETUP).await;
		let (chains, statement) =
			refusals(&fixture, &owner(), "SELECT *, fn::touch(id) FROM task").await;
		assert_eq!(chains, vec![ChainLegality::Refused(ChainRefusal::Writes)]);
		assert_eq!(statement, Some(ChainRefusal::Writes));
	}

	/// The same summary, held in a set built by hand, decides without a catalog.
	#[test]
	fn a_summary_decides_the_chain_of_a_call_it_names() {
		let registry = FunctionRegistry::with_builtins();
		let call = |name: &str| {
			LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
				receiver: Function::Custom(name.to_owned()),
				args: Vec::new(),
			}))
		};
		let summaries = CatalogSummaries::from_functions(&[
			("clock", Summary::of(EffectSet::CLOCK)),
			("noisy", Summary::of(EffectSet::ENTROPY)),
			("lost", Summary::UNKNOWN),
		]);
		let chain = |name: &str| chain_legality_with([&call(name)], &registry, &summaries);
		// The clock does not refuse by itself; the call is an unseen body all the same.
		assert_eq!(chain("clock"), ChainLegality::Refused(ChainRefusal::Opaque));
		assert_eq!(chain("noisy"), ChainLegality::Refused(ChainRefusal::Entropy));
		assert_eq!(chain("lost"), ChainLegality::Refused(ChainRefusal::Writes));
		// A function the set does not hold is as the registry alone says.
		assert_eq!(chain("other"), ChainLegality::Refused(ChainRefusal::Opaque));
	}

	/// A table permission judges its predicate over a copy of the row with the computed fields it
	/// reads derived, and that derivation is in no operator of the plan. `fn::draw` takes its
	/// sequence draw after the field that calls it, which `DEFINE FIELD` would refuse.
	#[tokio::test]
	async fn a_computed_field_a_permission_derives_decides_the_chain() {
		use ChainLegality::{Refused, RowLocal};
		let fixture = Fixture::new(
			"
			DEFINE TABLE vault PERMISSIONS FOR select WHERE r >= 0;
			DEFINE FIELD r ON vault COMPUTED rand();
			DEFINE TABLE token PERMISSIONS FOR select WHERE u != NONE;
			DEFINE FIELD u ON token COMPUTED rand::uuid::v7();
			DEFINE TABLE control PERMISSIONS FOR select WHERE d >= 0;
			DEFINE FIELD d ON control COMPUTED n * 2;
			DEFINE TABLE ext PERMISSIONS FOR select WHERE e != NONE;
			DEFINE FIELD e ON ext COMPUTED http::get('http://127.0.0.1:1/');
			DEFINE SEQUENCE s;
			DEFINE FUNCTION fn::draw() { LET $n = 1; RETURN 0 };
			DEFINE TABLE drawn PERMISSIONS FOR select WHERE k >= 0;
			DEFINE FIELD k ON drawn COMPUTED fn::draw();
			DEFINE FUNCTION OVERWRITE fn::draw() { LET $n = 1; RETURN sequence::nextval('s') };
			",
		)
		.await;
		for (sql, expected) in [
			("SELECT id, n FROM vault", Refused(ChainRefusal::Entropy)),
			("SELECT id, n FROM token", Refused(ChainRefusal::Entropy)),
			("SELECT id, n FROM ext", Refused(ChainRefusal::External)),
			("SELECT id, n FROM drawn", Refused(ChainRefusal::AdvancesState)),
			("SELECT id, n FROM control", RowLocal),
		] {
			let (chains, statement) = refusals(&fixture, &record_user(), sql).await;
			assert_eq!(chains, vec![expected], "{sql}");
			assert_eq!(
				statement,
				match expected {
					Refused(refusal) => Some(refusal),
					RowLocal => None,
				},
				"{sql}"
			);
		}
	}

	/// A computed field that calls a function with effects refuses the chain that derives it,
	/// whether the answer holds the field or not. `DEFINE FIELD` refuses a COMPUTED body whose
	/// callee writes or advances state when the field is defined, so the bodies of
	/// `fn::touch_page` and `fn::draw` are defined after the fields; only the summary sees them.
	#[tokio::test]
	async fn a_computed_field_calling_a_function_refuses_by_its_summary() {
		let fixture = Fixture::new(
			"
			DEFINE SEQUENCE s;
			DEFINE TABLE page SCHEMALESS;
			DEFINE FUNCTION fn::touch_page($id: record) { RETURN time::now() };
			DEFINE FUNCTION fn::draw() { LET $n = 1; RETURN 0 };
			DEFINE FIELD last_read ON page COMPUTED fn::touch_page(id);
			DEFINE FIELD drawn ON page COMPUTED fn::draw();
			DEFINE FUNCTION OVERWRITE fn::draw() { LET $n = 1; RETURN sequence::nextval('s') };
			DEFINE FUNCTION OVERWRITE fn::touch_page($id: record) {
				UPSERT stats:reads SET n += 1;
				RETURN time::now()
			};
			",
		)
		.await;
		let (chains, statement) = refusals(&fixture, &owner(), "SELECT * FROM page").await;
		assert_eq!(chains, vec![ChainLegality::Refused(ChainRefusal::Writes)]);
		assert_eq!(statement, Some(ChainRefusal::Writes));
		let (chains, statement) = refusals(&fixture, &owner(), "SELECT drawn FROM page").await;
		assert_eq!(chains, vec![ChainLegality::Refused(ChainRefusal::AdvancesState)]);
		assert_eq!(statement, Some(ChainRefusal::AdvancesState));
	}

	/// A field read by name derives the field of every table that computes one of that name.
	#[tokio::test]
	async fn a_read_of_a_computed_field_has_the_effects_of_deriving_it() {
		let fixture = Fixture::new(SETUP).await;
		let (_, summaries) = summarised(&fixture, &owner(), "SELECT * FROM vault").await;
		let registry = FunctionRegistry::with_builtins();
		let chain = |sql: &str| {
			let expr = {
				let ast = crate::syn::parse(sql).expect("parses");
				let surrealdb_expr::parser::TopLevelExpr::Expr(expr) =
					ast.expressions.into_iter().next().expect("one statement")
				else {
					panic!("a bare expression is expected: {sql}");
				};
				crate::expr::logical::lower_expr(expr).expect("lowers")
			};
			chain_legality_with([&expr], &registry, &summaries)
		};
		assert_eq!(chain("r >= 0"), ChainLegality::Refused(ChainRefusal::Entropy));
		assert_eq!(chain("both > 1"), ChainLegality::Refused(ChainRefusal::Entropy));
		assert_eq!(chain("drawn > 1"), ChainLegality::Refused(ChainRefusal::AdvancesState));
		assert_eq!(chain("twice > 1"), ChainLegality::RowLocal);
		assert_eq!(chain("n > 1"), ChainLegality::RowLocal);
	}
}
