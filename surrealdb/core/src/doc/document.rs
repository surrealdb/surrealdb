use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::OnceCell;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, EventDefinition, FieldDefinition, IndexDefinition, NamespaceDefinition,
	Record, SubscriptionDefinition, TableDefinition,
};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Operable, Processable};
use crate::doc::alter::ComputedData;
use crate::expr::computed_deps::{ComputedDeps, extract_computed_deps};
use crate::idx::planner::RecordStrategy;
use crate::idx::planner::iterators::IteratorRecord;
use crate::val::{RecordId, TableName, Value};

pub(crate) struct Document {
	/// The document context for this document
	pub(super) doc_ctx: DocumentContext,
	/// The record id of this document
	pub(super) id: Option<Arc<RecordId>>,
	/// The table that we should generate a record id from
	pub(super) r#gen: Option<TableName>,
	/// Whether this is the second iteration of the processing
	pub(super) retry: bool,
	/// The extras for this document
	pub(super) extras: Extras,
	/// The initial document
	pub(super) initial: CursorDoc,
	/// The current document
	pub(super) current: CursorDoc,
	/// The permissions reduced initial document
	pub(super) initial_reduced: Option<CursorDoc>,
	/// The permissions reduced current document
	pub(super) current_reduced: Option<CursorDoc>,
	/// The record strategy for this document
	pub(super) record_strategy: RecordStrategy,
	/// The computed input data for this document
	pub(super) input_data: Option<ComputedData>,
	/// Whether this document mutated the underlying KV store during
	/// processing. Set to `true` after `store_record_data` / `purge`
	/// complete a real KV write; consumed by the iterator's per-row
	/// dispatch to bump the per-statement affected-row counter exactly
	/// once per real mutation.
	///
	/// Stays `false` on pre-mutation `IgnoreError::Ignore` paths
	/// (`check_record_exists`, `check_where_condition`, permission
	/// gates, `ctx.is_done` short-circuits) and on no-op `set_record`
	/// calls suppressed by `!self.changed()`, so the counter never
	/// inflates from rows that were filtered or unchanged.
	pub(crate) mutated: bool,
	/// Memoized result of [`Self::is_modified`]. Populated on first
	/// access, after all mutation phases have run, and reused by the
	/// post-mutation gates (`process_table_views` / `process_table_events`
	/// / `process_table_lives` / `process_changefeeds` / `store_record_data`
	/// / `store_index_data`) so each `Document` deep-compares at most once.
	pub(super) modified: OnceCell<bool>,
}

/// Carries additional context needed by the Document
/// processor for specific statement types.
#[derive(Debug)]
pub(super) enum Extras {
	/// Used for SELECT, CREATE, UPDATE, DELETE, and UPSERT statements.
	Normal,
	/// Used for INSERT statements.
	/// Arguments in order:
	/// 1. Insertion value: The specific unique content for inserting
	/// - INSERT INTO thing { ... X ... };
	/// - INSERT INTO thing [{ ... X ... }, { ... Y ... }];
	/// - INSERT INTO thing (...) VALUES (... X ...), (... Y ...);
	Insert(Arc<Value>),
	/// Used for RELATE and INSERT RELATION statements.
	/// Arguments in order:
	/// 1. Record ID source: The 'from' side of the relation (e.g., person:tobie)
	/// 2. Record ID target: The 'to' side of the relation (e.g., post:123)
	/// 3. Insertion value: The specific unique content for inserting
	/// - INSERT RELATION INTO likes { ... X ... };
	/// - INSERT RELATION INTO likes [{ ... X ... }, { ... Y ... }];
	/// - INSERT RELATION INTO likes (id, in, out, desc) VALUES (1, person:1, person:2, ... X ...),
	///   (2, person:2, person:3, ... Y ...);
	Relate(RecordId, RecordId, Option<Arc<Value>>),
}

/// Context for a query which has a namespace and database
/// but does not belong to a table. This is used for queries
/// that are not associated with a specific table, for example:
///
/// SELECT * FROM [1,2,3,4,5];
/// SELECT * FROM { name: "John" };
#[derive(Clone, Debug)]
pub(crate) struct NsDbCtx {
	pub(crate) ns: Arc<NamespaceDefinition>,
	pub(crate) db: Arc<DatabaseDefinition>,
}

/// Context for a query which has a namespace and database
/// and a table. This is used for read-only queries that are
/// associated with a specific table, for example:
///
/// SELECT * FROM user;
/// SELECT * FROM user:test;
///
/// The `fields` slice is loaded eagerly so the per-row hot path can read
/// field definitions synchronously — needed for permission reduction,
/// computed-field evaluation, and SELECT projection. Catalog data that
/// only matters when mutating a record (events, foreign views, indexes,
/// live queries) lives on [`NsDbTbMutCtx`] instead.
#[derive(Clone, Debug)]
pub(crate) struct NsDbTbCtx {
	/// The namespace this document belongs to.
	pub(crate) ns: Arc<NamespaceDefinition>,
	/// The database this document belongs to.
	pub(crate) db: Arc<DatabaseDefinition>,
	/// The compiled definition of the table this document belongs to.
	pub(crate) tb: Arc<TableDefinition>,
	/// The table's compiled field definitions, eagerly loaded so the per-row
	/// hot path can run permission reduction, computed fields, and projection
	/// without async catalog calls or definition-text parses.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// Index into `fields` of the `id` field definition, if one is defined.
	/// Precomputed once so the write path can read the id field's kind and
	/// default in O(1) without rescanning the field list per record.
	pub(crate) id_field_idx: Option<usize>,
	/// Which same-table fields each computed field reads, keyed by field name.
	/// See [`computed_field_deps`].
	pub(crate) computed_deps: HashMap<String, ComputedDeps>,
	/// The order the write path processes this table's fields in, derived from
	/// the dependencies between their clauses. See [`field_eval_order`].
	pub(crate) field_order: Arc<FieldEvalOrder>,
}

/// Find the index of the `id` field within a table's field set, if one is
/// defined. Computed once when a table context is built so the record write
/// path never has to rescan the field list per record.
fn id_field_index(fields: &[FieldDefinition]) -> Option<usize> {
	fields.iter().position(|fd| fd.name.is_id())
}

/// The order in which a record's fields are processed on the write path.
///
/// A field's `DEFAULT` / `VALUE` / `COMPUTED` clause can produce its value from
/// other fields of the same record, so those fields have to be processed first
/// or the reader sees the pre-mutation value — or nothing at all. The field list
/// itself arrives in name order, which is no relation to that requirement, so
/// the order is derived from the dependencies instead. See
/// [`FieldDefinition::production_dependencies`].
#[derive(Clone, Debug)]
pub(crate) struct FieldEvalOrder {
	/// Indices into the table's field list, in dependency order: every field
	/// appears after the fields its clauses read, and after its own parent
	/// field so the optional-parent skip still works.
	///
	/// Ties are broken by field index, which is name order, so a table whose
	/// fields do not read each other is processed in exactly the order the
	/// field list arrives in.
	pub(crate) order: Vec<usize>,
	/// Indices of the `COMPUTED` fields that some other field's clause reads.
	/// These are the ones the write path has to materialise, transitively; a
	/// computed field nothing reads is left to the read side, which evaluates
	/// only what a projection asks for.
	///
	/// Held as indices rather than names because the write path tests membership
	/// once per computed field per record, and a name would have to be rendered
	/// from the field's idiom to do it.
	pub(crate) required_computed: HashSet<usize>,
	/// Field names that form a dependency cycle, when the graph is not a DAG.
	/// Reads are unaffected, so this is carried rather than raised here and
	/// the write path rejects the record.
	pub(crate) cycle: Option<Vec<String>>,
}

/// Derive the write-path field processing order for a table.
///
/// Computed once when a table context is built, for the same reason the field
/// definitions are loaded there: the result depends only on the field set, and
/// the alternative is rebuilding the graph for every record a statement
/// touches.
///
/// A dependency on a field this table does not define is ignored — a schemaless
/// record may or may not carry it, and either way there is no definition whose
/// processing could be ordered against. A clause whose dependencies could not
/// be fully determined (a subquery, a parameter, a graph traversal) contributes
/// only the dependencies that were found, and keeps its name-order position.
fn field_eval_order(fields: &[FieldDefinition]) -> FieldEvalOrder {
	/// Record that `to` must be processed after `from`. Self-edges and repeats
	/// are dropped so the in-degrees stay in step with the edge list.
	fn add_edge(from: usize, to: usize, edges: &mut [Vec<usize>], indegree: &mut [usize]) {
		if from != to && !edges[from].contains(&to) {
			edges[from].push(to);
			indegree[to] += 1;
		}
	}

	// Resolve dependency names against the fields this table defines
	let by_name: HashMap<String, usize> =
		fields.iter().enumerate().map(|(i, fd)| (fd.name.to_raw_string(), i)).collect();
	// edges[i] lists the fields that must be processed after field `i`
	let mut edges: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
	let mut indegree: Vec<usize> = vec![0; fields.len()];
	// A field's clauses read other fields: those are processed first
	let mut deps_by_field: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
	for (i, fd) in fields.iter().enumerate() {
		for dep in fd.production_dependencies() {
			if let Some(&j) = by_name.get(&dep) {
				deps_by_field[i].push(j);
				add_edge(j, i, &mut edges, &mut indegree);
			}
		}
	}
	// A nested field is processed after its parent, so a NONE optional parent
	// can still mark its children skippable
	for (i, fd) in fields.iter().enumerate() {
		for (j, other) in fields.iter().enumerate() {
			if i != j && other.name.len() > fd.name.len() && other.name.starts_with(&fd.name) {
				add_edge(i, j, &mut edges, &mut indegree);
			}
		}
	}
	// Kahn's algorithm, taking the lowest-numbered ready field each time so
	// that a table with no inter-field dependencies keeps name order
	let mut order = Vec::with_capacity(fields.len());
	let mut ready: BTreeSet<usize> =
		indegree.iter().enumerate().filter(|(_, d)| **d == 0).map(|(i, _)| i).collect();
	while let Some(i) = ready.iter().next().copied() {
		ready.remove(&i);
		order.push(i);
		for &next in &edges[i] {
			indegree[next] -= 1;
			if indegree[next] == 0 {
				ready.insert(next);
			}
		}
	}
	// Anything left is in a cycle. Emit it in name order so the field list is
	// still complete, and name the fields so the write path can report them.
	let cycle = if order.len() < fields.len() {
		let mut ordered = vec![false; fields.len()];
		for &i in &order {
			ordered[i] = true;
		}
		let remaining: Vec<usize> = (0..fields.len()).filter(|&i| !ordered[i]).collect();
		let names = remaining.iter().map(|&i| fields[i].name.to_raw_string()).collect();
		order.extend(remaining);
		Some(names)
	} else {
		None
	};
	// Transitively collect the computed fields some other field's clause reads.
	// `ASSERT` reads count here even though they contribute no edge above: the
	// validation pass still has to find the value materialised.
	let mut required_computed: HashSet<usize> = HashSet::new();
	let mut pending: Vec<usize> = Vec::new();
	for (i, fd) in fields.iter().enumerate() {
		if fd.computed.is_some() {
			continue;
		}
		pending.extend(deps_by_field[i].iter().copied());
		for dep in fd.assert_dependencies() {
			if let Some(&j) = by_name.get(&dep) {
				pending.push(j);
			}
		}
	}
	while let Some(i) = pending.pop() {
		if fields[i].computed.is_none() {
			continue;
		}
		if required_computed.insert(i) {
			pending.extend(deps_by_field[i].iter().copied());
		}
	}
	FieldEvalOrder {
		order,
		required_computed,
		cycle,
	}
}

/// Which same-table fields each computed field reads.
///
/// Derived once when a table context is built, for the same reason the field
/// definitions themselves are loaded here: the alternative is walking every
/// computed field's expression tree again for each record the statement
/// touches. The result depends only on the field set, which is fixed for the
/// life of the context.
fn computed_field_deps(fields: &[FieldDefinition]) -> HashMap<String, ComputedDeps> {
	fields
		.iter()
		.filter_map(|fd| {
			let computed = fd.computed.as_ref()?;
			Some((fd.name.to_raw_string(), extract_computed_deps(computed)))
		})
		.collect()
}

impl NsDbTbCtx {
	/// Build a read-only table-scoped catalog context. Fetches `fields`
	/// from the [`Datastore`](crate::kvs::Datastore) cache when one is
	/// attached to `ctx` and `version` is unset; versioned reads always
	/// bypass the cache and read directly from the transaction.
	pub(crate) async fn load(
		ctx: &FrozenContext,
		parent: &NsDbCtx,
		tb: Arc<TableDefinition>,
		table: &TableName,
		version: Option<u64>,
	) -> Result<Self> {
		use crate::kvs::cache;
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = parent.ns.namespace_id;
		// Get the database id
		let db = parent.db.database_id;
		// Fetch the cache if we can use it
		let cache = match version {
			None => ctx.get_cache(),
			Some(_) => None,
		};
		// Build the document context. Definitions arrive pre-compiled from
		// the transaction; the datastore cache holds them across
		// transactions, keyed by the table's cache stamps.
		if let Some(cache) = cache {
			let fields = {
				let key = cache::ds::Lookup::Fds(ns, db, table.as_str(), tb.cache_fields_ts);
				match cache.get(&key) {
					Some(val) => val.try_into_fds()?,
					None => {
						let val = txn.all_tb_fields(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Fds(Arc::clone(&val)));
						val
					}
				}
			};
			// Locate the id field once for the write path
			let id_field_idx = id_field_index(&fields);
			let computed_deps = computed_field_deps(&fields);
			let field_order = Arc::new(field_eval_order(&fields));
			// Return the document context
			Ok(Self {
				ns: Arc::clone(&parent.ns),
				db: Arc::clone(&parent.db),
				tb,
				fields,
				id_field_idx,
				computed_deps,
				field_order,
			})
		} else {
			// Fetch the definitions
			let fields = txn.all_tb_fields(ns, db, table, version).await?;
			// Locate the id field once for the write path
			let id_field_idx = id_field_index(&fields);
			let computed_deps = computed_field_deps(&fields);
			let field_order = Arc::new(field_eval_order(&fields));
			// Return the document context
			Ok(Self {
				ns: Arc::clone(&parent.ns),
				db: Arc::clone(&parent.db),
				tb,
				fields,
				id_field_idx,
				computed_deps,
				field_order,
			})
		}
	}
}

/// Context for a query which has a namespace, database and a table, and
/// which mutates records in that table. Used by CREATE / UPDATE / UPSERT /
/// DELETE / INSERT / RELATE statements, where the document processor
/// additionally needs the table's events, foreign views, indexes, and live
/// queries to maintain consistency with the rest of the catalog after a
/// write.
#[derive(Clone, Debug)]
pub(crate) struct NsDbTbMutCtx {
	/// The namespace this document belongs to.
	pub(crate) ns: Arc<NamespaceDefinition>,
	/// The database this document belongs to.
	pub(crate) db: Arc<DatabaseDefinition>,
	/// The compiled definition of the table this document belongs to.
	pub(crate) tb: Arc<TableDefinition>,
	/// The table's compiled field definitions, used to apply the schema
	/// (types, defaults, assertions, permissions) to the record being
	/// written.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// The table's compiled event definitions, triggered after the record is
	/// written.
	pub(crate) events: Arc<[EventDefinition]>,
	/// The table's compiled foreign (view) tables, recomputed after the
	/// record is written to keep their aggregates consistent.
	pub(crate) tables: Arc<[TableDefinition]>,
	/// The table's compiled index definitions, maintained after the record
	/// is written.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// The table's compiled live query subscriptions, notified after the
	/// record is written.
	pub(crate) lives: Arc<[SubscriptionDefinition]>,
	/// Index into `fields` of the `id` field definition, if one is defined.
	/// Precomputed once so the write path can read the id field's kind and
	/// default in O(1) without rescanning the field list per record.
	pub(crate) id_field_idx: Option<usize>,
	/// Which same-table fields each computed field reads, keyed by field name.
	/// See [`computed_field_deps`].
	pub(crate) computed_deps: HashMap<String, ComputedDeps>,
	/// The order the write path processes this table's fields in, derived from
	/// the dependencies between their clauses. See [`field_eval_order`].
	pub(crate) field_order: Arc<FieldEvalOrder>,
}

impl NsDbTbMutCtx {
	/// Build a mutating table-scoped catalog context. Fetches every
	/// per-table definition the document processor consults when applying
	/// a write — fields, events, foreign views, indexes, live queries —
	/// from the [`Datastore`](crate::kvs::Datastore) cache when one is
	/// attached and `version` is unset; versioned reads always bypass the
	/// cache and read directly from the transaction.
	pub(crate) async fn load(
		ctx: &FrozenContext,
		parent: &NsDbCtx,
		tb: Arc<TableDefinition>,
		table: &TableName,
		version: Option<u64>,
	) -> Result<Self> {
		use crate::kvs::cache;
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = parent.ns.namespace_id;
		// Get the database id
		let db = parent.db.database_id;
		// Fetch the cache if we can use it
		let cache = match version {
			None => ctx.get_cache(),
			Some(_) => None,
		};
		// Build the document context. Definitions arrive pre-compiled from
		// the transaction; the datastore cache holds them across
		// transactions, keyed by the table's cache stamps.
		if let Some(cache) = cache {
			// Fetch the fields
			let fields = async || -> Result<_> {
				let key = cache::ds::Lookup::Fds(ns, db, table.as_str(), tb.cache_fields_ts);
				match cache.get(&key) {
					Some(val) => Ok(val.try_into_fds()?),
					None => {
						let val = txn.all_tb_fields(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Fds(Arc::clone(&val)));
						Ok(val)
					}
				}
			};
			// Fetch the events
			let events = async || -> Result<_> {
				let key = cache::ds::Lookup::Evs(ns, db, table.as_str(), tb.cache_events_ts);
				match cache.get(&key) {
					Some(val) => Ok(val.try_into_evs()?),
					None => {
						let val = txn.all_tb_events(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Evs(Arc::clone(&val)));
						Ok(val)
					}
				}
			};
			// Fetch the foreign views
			let tables = async || -> Result<_> {
				let key = cache::ds::Lookup::Fts(ns, db, table.as_str(), tb.cache_tables_ts);
				match cache.get(&key) {
					Some(val) => Ok(val.try_into_fts()?),
					None => {
						let val = txn.all_tb_views(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Fts(Arc::clone(&val)));
						Ok(val)
					}
				}
			};
			// Fetch the indexes
			let indexes = async || -> Result<_> {
				let key = cache::ds::Lookup::Ixs(ns, db, table.as_str(), tb.cache_indexes_ts);
				match cache.get(&key) {
					Some(val) => Ok(val.try_into_ixs()?),
					None => {
						let val = txn.all_tb_indexes(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Ixs(Arc::clone(&val)));
						Ok(val)
					}
				}
			};
			// Fetch the live queries. Keyed on the table's committed
			// `cache_lives_ts` (bumped transactionally by LIVE/KILL), exactly
			// like fields/events/indexes above — so the cache key travels in the
			// same snapshot as the read and cannot be poisoned by a concurrent
			// writer holding a pre-commit snapshot.
			let lives = async || -> Result<_> {
				let key = cache::ds::Lookup::Lvs(ns, db, table.as_str(), tb.cache_lives_ts);
				match cache.get(&key) {
					Some(val) => Ok(val.try_into_lvs()?),
					None => {
						let val = txn.all_tb_lives(ns, db, table, None).await?;
						cache.insert(key, cache::ds::Entry::Lvs(Arc::clone(&val)));
						Ok(val)
					}
				}
			};
			// Fetch the definitions
			let (fields, events, tables, indexes, lives) =
				futures::try_join!(fields(), events(), tables(), indexes(), lives())?;
			// Locate the id field once for the write path
			let id_field_idx = id_field_index(&fields);
			let computed_deps = computed_field_deps(&fields);
			let field_order = Arc::new(field_eval_order(&fields));
			// Return the document context
			Ok(Self {
				ns: Arc::clone(&parent.ns),
				db: Arc::clone(&parent.db),
				tb,
				fields,
				events,
				tables,
				indexes,
				lives,
				id_field_idx,
				computed_deps,
				field_order,
			})
		} else {
			// Fetch the definitions
			let (fields, events, tables, indexes, lives) = futures::try_join!(
				txn.all_tb_fields(ns, db, table, version),
				txn.all_tb_events(ns, db, table, version),
				txn.all_tb_views(ns, db, table, version),
				txn.all_tb_indexes(ns, db, table, version),
				txn.all_tb_lives(ns, db, table, version),
			)?;
			// Locate the id field once for the write path
			let id_field_idx = id_field_index(&fields);
			let computed_deps = computed_field_deps(&fields);
			let field_order = Arc::new(field_eval_order(&fields));
			// Return the document context
			Ok(Self {
				ns: Arc::clone(&parent.ns),
				db: Arc::clone(&parent.db),
				tb,
				fields,
				events,
				tables,
				indexes,
				lives,
				id_field_idx,
				computed_deps,
				field_order,
			})
		}
	}
}

/// Catalog scope attached to a [`Document`] while a statement runs.
///
/// The planner picks the narrowest context that still has everything the
/// document processor needs:
/// - [`NsDbCtx`] — namespace + database only, for queries that do not resolve to a specific table
///   (e.g. `SELECT * FROM [...]`).
/// - [`NsDbTbCtx`] — read-only table context, with `fields` eagerly loaded so SELECTs can run
///   permission reduction and projection without async catalog calls.
/// - [`NsDbTbMutCtx`] — mutating table context, adds events / foreign views / indexes / live
///   queries so writes can maintain catalog consistency without per-row async calls.
///
/// The table variants are `Arc`-wrapped so that carrying the context
/// through the iterator → processor → document pipeline (and cloning it
/// once per record on multi-record statements) is a pointer move rather
/// than a 64-byte copy of every field.
#[derive(Clone, Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum DocumentContext {
	/// Context for a query which has a namespace, and database
	NsDbCtx(NsDbCtx),
	/// Context for a read-only query against a specific table
	NsDbTbCtx(Arc<NsDbTbCtx>),
	/// Context for a query that mutates records in a specific table
	NsDbTbMutCtx(Arc<NsDbTbMutCtx>),
}

impl DocumentContext {
	/// Build the per-table catalog context for a statement, picking the
	/// read-only or mutating variant based on `mutating`. Mutating
	/// statements (CREATE / UPSERT / UPDATE / RELATE / DELETE / INSERT)
	/// also need events, foreign views, indexes, and live queries;
	/// SELECT only needs fields.
	pub(crate) async fn initialise(
		ctx: &FrozenContext,
		parent: &NsDbCtx,
		tb: Arc<TableDefinition>,
		table: &TableName,
		version: Option<u64>,
		mutating: bool,
	) -> Result<Self> {
		// The load futures are boxed so their state (definition fetches plus
		// compile-at-fill) never inlines into the callers' async state
		// machines: `Expr::compute`'s future embeds every statement arm, and
		// reblessive briefly materialises that future on the call stack when
		// scheduling it, so an unboxed load here inflates stack use for every
		// computed expression. Loading runs once per statement, so the single
		// heap allocation is immaterial.
		if mutating {
			Ok(DocumentContext::NsDbTbMutCtx(Arc::new(
				// Load the required definitions
				Box::pin(NsDbTbMutCtx::load(ctx, parent, tb, table, version)).await?,
			)))
		} else {
			Ok(DocumentContext::NsDbTbCtx(Arc::new(
				// Load the required definitions
				Box::pin(NsDbTbCtx::load(ctx, parent, tb, table, version)).await?,
			)))
		}
	}

	/// Get the namespace that this document is associated with
	pub(crate) fn ns(&self) -> &Arc<NamespaceDefinition> {
		match self {
			DocumentContext::NsDbCtx(ctx) => &ctx.ns,
			DocumentContext::NsDbTbCtx(ctx) => &ctx.ns,
			DocumentContext::NsDbTbMutCtx(ctx) => &ctx.ns,
		}
	}

	/// Get the database that this document is associated with
	pub(crate) fn db(&self) -> &Arc<DatabaseDefinition> {
		match self {
			DocumentContext::NsDbCtx(ctx) => &ctx.db,
			DocumentContext::NsDbTbCtx(ctx) => &ctx.db,
			DocumentContext::NsDbTbMutCtx(ctx) => &ctx.db,
		}
	}

	/// Get the table that this document is associated with
	pub(crate) fn tb(&self) -> Result<&Arc<TableDefinition>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Table not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.tb),
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.tb),
		}
	}

	/// Get the fields to be processed for this document
	pub(crate) fn fd(&self) -> Result<&Arc<[FieldDefinition]>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Fields not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.fields),
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.fields),
		}
	}

	/// Which same-table fields each computed field reads. Derived once when the
	/// context was built; see [`computed_field_deps`].
	/// The write-path field processing order for this table.
	/// See [`field_eval_order`].
	pub(crate) fn field_order(&self) -> Result<&Arc<FieldEvalOrder>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Fields not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.field_order),
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.field_order),
		}
	}

	pub(crate) fn computed_deps(&self) -> Result<&HashMap<String, ComputedDeps>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Fields not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(&ctx.computed_deps),
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.computed_deps),
		}
	}

	/// Get the precomputed `id` field definition, if one is defined. Located
	/// once when the table context is built (see [`id_field_index`]), so the
	/// record write path reads the id field's kind and default in O(1) instead
	/// of rescanning the fields per record.
	pub(crate) fn id_field(&self) -> Result<Option<&FieldDefinition>> {
		match self {
			DocumentContext::NsDbCtx(_) => Err(anyhow::anyhow!(
				"Id field not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
			DocumentContext::NsDbTbCtx(ctx) => Ok(ctx.id_field_idx.map(|i| &ctx.fields[i])),
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(ctx.id_field_idx.map(|i| &ctx.fields[i])),
		}
	}

	/// Get the events to be processed for this document. Only available
	/// on the [`DocumentContext::NsDbTbMutCtx`] variant: read-only
	/// statements never consult events.
	pub(crate) fn ev(&self) -> Result<&Arc<[EventDefinition]>> {
		match self {
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.events),
			_ => Err(anyhow::anyhow!(
				"Events not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
		}
	}

	/// Get the foreign tables to be processed for this document. Only
	/// available on the [`DocumentContext::NsDbTbMutCtx`] variant.
	pub(crate) fn ft(&self) -> Result<&Arc<[TableDefinition]>> {
		match self {
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.tables),
			_ => Err(anyhow::anyhow!(
				"Foreign tables not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
		}
	}

	/// Get the indexes to be processed for this document. Only available
	/// on the [`DocumentContext::NsDbTbMutCtx`] variant.
	pub(crate) fn ix(&self) -> Result<&Arc<[IndexDefinition]>> {
		match self {
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.indexes),
			_ => Err(anyhow::anyhow!(
				"Indexes not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
		}
	}

	/// Get the live queries to be processed for this document. Only
	/// available on the [`DocumentContext::NsDbTbMutCtx`] variant.
	pub(crate) fn lv(&self) -> Result<&Arc<[SubscriptionDefinition]>> {
		match self {
			DocumentContext::NsDbTbMutCtx(ctx) => Ok(&ctx.lives),
			_ => Err(anyhow::anyhow!(
				"Live queries not defined in DocumentContext, this is certainly a bug and should be reported."
			)),
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct CursorDoc {
	pub(crate) rid: Option<Arc<RecordId>>,
	pub(crate) ir: Option<Arc<IteratorRecord>>,
	pub(crate) doc: CursorRecord,
	pub(crate) fields_computed: bool,
}

impl CursorDoc {
	/// Context with `$parent` bound to the enclosing row (same binding as
	/// [`Self::update_parent`] applies before running nested statement bodies).
	pub(crate) fn with_parent_ctx<'a>(
		ctx: &'a FrozenContext,
		doc: Option<&CursorDoc>,
	) -> Cow<'a, FrozenContext> {
		if let Some(doc) = doc {
			let mut new_ctx = Context::new_child(ctx);
			new_ctx.add_value("parent", Arc::new(doc.doc.as_ref().clone()));
			Cow::Owned(new_ctx.freeze())
		} else {
			Cow::Borrowed(ctx)
		}
	}

	/// Updates the `"parent"` doc field for statements with a meaning full
	/// document.
	pub async fn update_parent<'a, F, R>(ctx: &'a FrozenContext, doc: Option<&CursorDoc>, f: F) -> R
	where
		F: AsyncFnOnce(Cow<'a, FrozenContext>) -> R,
	{
		let ctx = Self::with_parent_ctx(ctx, doc);
		f(ctx).await
	}
}

/// Wrapper around a Record for cursor operations
///
/// Holds an `Arc<Record>` internally, providing copy-on-write semantics via
/// `Arc::make_mut` in `DerefMut`. This avoids deep clones when multiple
/// cursors share the same record (e.g. initial vs current document).
#[derive(Clone, Debug)]
pub(crate) struct CursorRecord {
	/// The underlying record, shared via Arc for copy-on-write
	record: Arc<Record>,
}

impl CursorRecord {
	/// Returns a mutable reference to the underlying value.
	///
	/// Uses copy-on-write: if other `Arc` references exist, the record
	/// is cloned first so mutations are isolated.
	pub(crate) fn to_mut(&mut self) -> &mut Value {
		&mut Arc::make_mut(&mut self.record).data
	}

	/// Returns a new `Arc<Value>` by cloning the underlying value.
	///
	/// Used for event/live-query contexts where `Arc<Value>` is needed.
	pub(crate) fn as_arc(&self) -> Arc<Value> {
		Arc::new(self.record.data.clone())
	}

	/// Returns the inner `Arc<Record>`.
	pub(crate) fn into_read_only(self) -> Arc<Record> {
		self.record
	}

	/// Returns a reference to the underlying value.
	pub(crate) fn as_ref(&self) -> &Value {
		&self.record.data
	}

	/// Consumes the cursor record and returns the owned `Value`.
	///
	/// If this is the last `Arc` reference, the value is moved out without
	/// cloning. Otherwise the value is cloned.
	pub(crate) fn into_owned(self) -> Value {
		match Arc::try_unwrap(self.record) {
			Ok(record) => record.data,
			Err(arc) => arc.data.clone(),
		}
	}

	/// Returns `true` if two `CursorRecord`s point to the same allocation.
	pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.record, &other.record)
	}
}

impl Deref for CursorRecord {
	type Target = Record;
	fn deref(&self) -> &Self::Target {
		&self.record
	}
}

impl DerefMut for CursorRecord {
	fn deref_mut(&mut self) -> &mut Self::Target {
		Arc::make_mut(&mut self.record)
	}
}

impl CursorDoc {
	pub(crate) fn new<T: Into<CursorRecord>>(
		rid: Option<Arc<RecordId>>,
		ir: Option<Arc<IteratorRecord>>,
		doc: T,
	) -> Self {
		Self {
			rid,
			ir,
			doc: doc.into(),
			fields_computed: false,
		}
	}
}

impl From<Record> for CursorRecord {
	fn from(record: Record) -> Self {
		Self {
			record: Arc::new(record),
		}
	}
}

impl From<Arc<Record>> for CursorRecord {
	fn from(arc: Arc<Record>) -> Self {
		Self {
			record: arc,
		}
	}
}

impl From<Value> for CursorRecord {
	fn from(value: Value) -> Self {
		Self {
			record: Arc::new(Record::new(value)),
		}
	}
}

impl From<Value> for CursorDoc {
	fn from(val: Value) -> Self {
		Self {
			rid: None,
			ir: None,
			doc: val.into(),
			fields_computed: false,
		}
	}
}

impl Debug for Document {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "Document - id: <{:?}>", self.id)
	}
}

impl Document {
	/// Initialise a new document
	pub fn new(pro: Processable) -> Self {
		// Get the record id if specified
		let id = pro.rid;
		// Get the iterator record if specified
		let ir = pro.ir;
		// Convert the operable to an extras
		let (val, extras) = match pro.val {
			Operable::Value(v) => (v, Extras::Normal),
			Operable::Insert(v, o) => (v, Extras::Insert(o)),
			Operable::Relate(v, f, w, o) => (v, Extras::Relate(f, w, o)),
			_ => unreachable!(),
		};
		//
		let initial = CursorDoc::new(id.clone(), ir, val);
		let current = initial.clone();
		// Create a new document
		Document {
			doc_ctx: pro.doc_ctx,
			id,
			r#gen: pro.generate,
			retry: false,
			extras,
			current,
			initial,
			current_reduced: None,
			initial_reduced: None,
			record_strategy: pro.record_strategy,
			input_data: None,
			mutated: false,
			modified: OnceCell::new(),
		}
	}

	/// Check if document is being created
	#[inline]
	pub(super) fn is_new(&self) -> bool {
		self.initial.doc.as_ref().is_none()
	}

	/// Check if this document has been modified.
	///
	/// Memoizes the answer in [`Self::modified`] on first call. All
	/// callers are post-mutation (`store_record_data`, `store_index_data`,
	/// `purge`, `process_table_views` / `events` / `lives`,
	/// `process_changefeeds`), so caching is sound — by the time any of
	/// them runs the mutation phases (`process_record_data`,
	/// `default_record_data`, `process_table_fields`,
	/// `cleanup_table_fields`) have finished and `initial` / `current`
	/// will not change again for this document.
	///
	/// Uses `tokio::sync::OnceCell` so `Document` remains `Sync`. The
	/// compute is deterministic and cheap, so a benign race (two threads
	/// both compute and try to `set`) is fine: only one set wins, both
	/// produce the same value.
	#[inline]
	pub(super) fn is_modified(&self) -> bool {
		if let Some(&v) = self.modified.get() {
			return v;
		}
		let v = if self.initial.doc.ptr_eq(&self.current.doc) {
			false
		} else {
			self.initial.doc.as_ref() != self.current.doc.as_ref()
		};
		let _ = self.modified.set(v);
		v
	}

	/// Check if the condition clause has already been checked
	#[inline]
	pub(crate) fn is_key_only_iteration(&self) -> bool {
		matches!(self.record_strategy, RecordStrategy::Count | RecordStrategy::KeysOnly)
	}

	/// Check if this is the first iteration. When
	/// running an UPSERT or INSERT statement we don't
	/// first fetch the value from the storage engine.
	/// If there is an error when attempting to set the
	/// value in the storage engine, then we retry the
	/// document processing, and this will return false.
	#[inline]
	pub(super) fn is_iteration_initial(&self) -> bool {
		!self.retry && self.initial.doc.as_ref().is_none()
	}

	/// Check if the record id for this document
	/// has been specifically set upfront. This is true
	/// in the following instances:
	///
	/// CREATE some:thing;
	/// CREATE some SET id = some:thing;
	/// CREATE some CONTENT { id: some:thing };
	/// UPSERT some:thing;
	/// UPSERT some SET id = some:thing;
	/// UPSERT some CONTENT { id: some:thing };
	/// INSERT some (id) VALUES (some:thing);
	/// INSERT { id: some:thing };
	/// INSERT [{ id: some:thing }];
	/// RELATE from->some:thing->to;
	/// RELATE from->some->to SET id = some:thing;
	/// RELATE from->some->to CONTENT { id: some:thing };
	///
	/// In addition, when iterating over tables or ranges
	/// the record id will also be specified before we
	/// process the document in this module. So therefore
	/// although this function is not used or checked in
	/// these scenarios, this function will also be true
	/// in the following instances:
	///
	/// UPDATE some;
	/// UPDATE some:thing;
	/// UPDATE some:from..to;
	/// DELETE some;
	/// DELETE some:thing;
	/// DELETE some:from..to;
	#[inline]
	pub(super) fn is_specific_record_id(&self) -> bool {
		match self.extras {
			Extras::Insert(ref v) => !v.rid().is_nullish(),
			Extras::Normal => self.r#gen.is_none(),
			_ => false,
		}
	}

	/// Update the document for a retry to update after an insert failed.
	pub fn modify_for_update_retry(&mut self, id: RecordId, record: Arc<Record>) {
		let retry = Arc::new(id);
		self.id = Some(Arc::clone(&retry));
		self.r#gen = None;
		self.retry = true;
		self.record_strategy = RecordStrategy::KeysAndValues;

		self.current = CursorDoc::new(Some(retry), None, record);
		self.initial = self.current.clone();
		// Recalculating ComputedData, depending on the existing record.
		self.input_data = None;
	}

	/// Retrieve the record id for this document
	pub(crate) fn id(&self) -> Result<Arc<RecordId>> {
		match &self.id {
			Some(id) => Ok(Arc::clone(id)),
			_ => fail!("Expected a document id to be present"),
		}
	}

	/// Retrieve the record id for this document
	pub fn inner_id(&self) -> Result<RecordId> {
		match self.id.clone() {
			Some(id) => Ok(Arc::unwrap_or_clone(id)),
			_ => fail!("Expected a document id to be present"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{FieldDefinition, computed_field_deps};
	use crate::syn;

	fn field(name: &str, computed: Option<&str>) -> FieldDefinition {
		FieldDefinition {
			name: syn::idiom(name).unwrap().into(),
			computed: computed.map(|c| syn::expr(c).unwrap().into()),
			..Default::default()
		}
	}

	/// Only computed fields get an entry, keyed by the same raw name the
	/// resolver looks them up by. A non-computed field must not appear, or the
	/// resolver would treat it as a computed field with no dependencies.
	#[test]
	fn derives_deps_for_computed_fields_only() {
		let fields = vec![
			field("plain", None),
			field("total", Some("price * qty")),
			field("nested.deep", Some("other")),
		];

		let deps = computed_field_deps(&fields);

		assert_eq!(deps.len(), 2, "only the two computed fields are keyed");
		assert!(!deps.contains_key("plain"));
		let total = deps.get("total").expect("keyed by raw name");
		assert!(total.fields.iter().any(|f| f == "price"));
		assert!(total.fields.iter().any(|f| f == "qty"));
		assert!(deps.contains_key("nested.deep"), "keyed by the full raw path");
	}
}
