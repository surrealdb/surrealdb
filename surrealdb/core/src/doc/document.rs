use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, EventDefinition, FieldDefinition, IndexDefinition, NamespaceDefinition,
	Record, SubscriptionDefinition, TableDefinition,
};
use crate::ctx::FrozenContext;
use crate::expr::computed_deps::{ComputedDeps, extract_computed_deps};
use crate::val::{RecordId, TableName, Value};

/// What a write row carries beyond its record, for specific statement types.
#[derive(Debug)]
pub(crate) enum Extras {
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

/// Find the index of the `id` field within a table's field set, if one is
/// defined. Computed once when a table context is built so the record write
/// path never has to rescan the field list per record.
pub(crate) fn id_field_index(fields: &[FieldDefinition]) -> Option<usize> {
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
	/// Ties are broken by putting a field derived from a named record id
	/// component first and then by field index, which is name order, so a
	/// table whose fields do not read each other and whose id names no
	/// component is processed in exactly the order the field list arrives in.
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
///
/// A definition derived from a named record id component
/// ([`FieldDefinition::is_key_part`]) reads the id and nothing the field loop
/// produces: the id is settled before any field is processed. It has no
/// dependency edges of its own, so an `id` clause that reads the name of a
/// component is not a cycle, and it is processed ahead of every field that is
/// ready beside it. A table that names no component is ordered by its
/// dependencies alone, taking the lowest-numbered ready field each time.
pub(crate) fn field_eval_order(fields: &[FieldDefinition]) -> FieldEvalOrder {
	/// Record that `to` must be processed after `from`. Self-edges and repeats
	/// are dropped so the in-degrees stay in step with the edge list.
	fn add_edge(from: usize, to: usize, edges: &mut [Vec<usize>], indegree: &mut [usize]) {
		if from != to && !edges[from].contains(&to) {
			edges[from].push(to);
			indegree[to] += 1;
		}
	}

	// Resolve dependency names against the fields this table defines. A clause
	// names a field it reads by the key a record holds it under, so a top-level
	// field is looked up by that, unescaped; a nested field is read by nobody
	// and has no entry.
	let by_name: HashMap<&str, usize> =
		fields.iter().enumerate().filter_map(|(i, fd)| Some((fd.name.field_name()?, i))).collect();
	// edges[i] lists the fields that must be processed after field `i`
	let mut edges: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
	let mut indegree: Vec<usize> = vec![0; fields.len()];
	// A field's clauses read other fields: those are processed first
	let mut deps_by_field: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
	for (i, fd) in fields.iter().enumerate() {
		if fd.is_key_part() {
			continue;
		}
		for dep in fd.production_dependencies() {
			if let Some(&j) = by_name.get(dep.as_str()) {
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
	// Kahn's algorithm, taking the ready field derived from a record id
	// component first and otherwise the lowest-numbered one, so that a table
	// with no inter-field dependencies keeps name order
	let rank = |i: usize| (!fields[i].is_key_part(), i);
	let mut order = Vec::with_capacity(fields.len());
	let mut ready: BTreeSet<(bool, usize)> =
		indegree.iter().enumerate().filter(|(_, d)| **d == 0).map(|(i, _)| rank(i)).collect();
	while let Some(first) = ready.iter().next().copied() {
		ready.remove(&first);
		let i = first.1;
		order.push(i);
		for &next in &edges[i] {
			indegree[next] -= 1;
			if indegree[next] == 0 {
				ready.insert(rank(next));
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
			if let Some(&j) = by_name.get(dep.as_str()) {
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
///
/// Keyed by the name a record holds the field under, which is how a clause
/// names it and how the resolver looks it up. A computed field is a top-level
/// one, so a definition with no such name has no entry.
fn computed_field_deps(fields: &[FieldDefinition]) -> HashMap<String, ComputedDeps> {
	fields
		.iter()
		.filter_map(|fd| {
			let computed = fd.computed.as_ref()?;
			Some((fd.name.field_name()?.to_owned(), extract_computed_deps(computed)))
		})
		.collect()
}

/// Context for a query which has a namespace, database and a table, and
/// which mutates records in that table. Used by CREATE / UPDATE / UPSERT /
/// DELETE / INSERT / RELATE statements, where the write path
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
	/// The VM's units of `lives`, one for each, at the catalog version this
	/// context read (`crate::exec::vm::live`).
	pub(crate) live_units: crate::exec::vm::live::LiveUnitList,
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
	/// per-table definition the write path consults when applying
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
		Self::load_with(ctx, parent, tb, table, version, true).await
	}

	/// [`Self::load`] with the table's live queries left unread: `lives` and
	/// `live_units` are empty, for a holder that does not notify them on the
	/// write path — a write under the router engine, which notifies them off
	/// it, and the router's replay, which reads them itself a page at a time.
	pub(crate) async fn load_without_lives(
		ctx: &FrozenContext,
		parent: &NsDbCtx,
		tb: Arc<TableDefinition>,
		table: &TableName,
		version: Option<u64>,
	) -> Result<Self> {
		Self::load_with(ctx, parent, tb, table, version, false).await
	}

	async fn load_with(
		ctx: &FrozenContext,
		parent: &NsDbCtx,
		tb: Arc<TableDefinition>,
		table: &TableName,
		version: Option<u64>,
		with_lives: bool,
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
			let lives = async || -> Result<Arc<[SubscriptionDefinition]>> {
				if !with_lives {
					return Ok(Arc::from(Vec::new()));
				}
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
			// The VM's units of the live queries at the catalog version read
			// here, one for each; a unit the node does not hold is compiled
			// in a fault.
			let live_units =
				crate::exec::vm::live::units_for(ctx, &parent.ns, &parent.db, table, &lives)
					.await?;
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
				live_units,
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
				async {
					if with_lives {
						txn.all_tb_lives(ns, db, table, version).await
					} else {
						Ok(Arc::from(Vec::new()))
					}
				},
			)?;
			// Locate the id field once for the write path
			let id_field_idx = id_field_index(&fields);
			let computed_deps = computed_field_deps(&fields);
			let field_order = Arc::new(field_eval_order(&fields));
			// The VM's units of the live queries at the catalog version read
			// here, one for each; a unit the node does not hold is compiled
			// in a fault.
			let live_units =
				crate::exec::vm::live::units_for(ctx, &parent.ns, &parent.db, table, &lives)
					.await?;
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
				live_units,
				id_field_idx,
				computed_deps,
				field_order,
			})
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct CursorDoc {
	pub(crate) rid: Option<Arc<RecordId>>,
	pub(crate) doc: CursorRecord,
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
	/// Returns a reference to the underlying value.
	pub(crate) fn as_ref(&self) -> &Value {
		&self.record.data
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
	pub(crate) fn new<T: Into<CursorRecord>>(rid: Option<Arc<RecordId>>, doc: T) -> Self {
		Self {
			rid,
			doc: doc.into(),
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
			doc: val.into(),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use surrealdb_strand::Strand;

	use super::{FieldDefinition, computed_field_deps, field_eval_order, id_field_index};
	use crate::catalog::DefineDefault;
	use crate::expr::computed_deps::topological_sort_computed_fields;
	use crate::expr::{KeyNames, Kind, KindLiteral};
	use crate::syn;
	use crate::val::TableName;

	fn field(name: &str, computed: Option<&str>) -> FieldDefinition {
		FieldDefinition {
			name: syn::idiom(name).unwrap(),
			computed: computed.map(|c| syn::expr(c).unwrap()),
			..Default::default()
		}
	}

	fn valued(name: &str, value: &str) -> FieldDefinition {
		FieldDefinition {
			name: syn::idiom(name).unwrap(),
			value: Some(syn::expr(value).unwrap()),
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

		assert_eq!(deps.len(), 1, "only the top-level computed field is keyed");
		assert!(!deps.contains_key("plain"));
		let total = deps.get("total").expect("keyed by raw name");
		assert!(total.fields.iter().any(|f| f == "price"));
		assert!(total.fields.iter().any(|f| f == "qty"));
		assert!(!deps.contains_key("nested.deep"), "a nested path is not a record key");
	}

	/// The key is the name the record holds the field under, so a name that has
	/// to be backticked when written is keyed without them.
	#[test]
	fn keys_deps_by_the_unescaped_name() {
		for name in ["the x", "a-b", "café", "1st", "a.b"] {
			let escaped = format!("`{name}`");
			let fields = vec![field(&escaped, Some("src"))];

			let deps = computed_field_deps(&fields);

			assert!(deps.contains_key(name), "`{name}` is keyed by itself");
			assert!(!deps.contains_key(&escaped), "`{name}` is not keyed by its rendering");
		}
	}

	/// A dependency on a field whose name has to be backticked when written
	/// orders the fields like any other, and marks the computed field read by
	/// another clause as required.
	#[test]
	fn orders_fields_by_dependencies_on_escaped_names() {
		// The names sort so that every reader precedes the field it reads
		let fields = vec![
			field("`a first`", Some("`z last` + 1")),
			valued("copied", "`z last`"),
			valued("`copied 2`", "`a first`"),
			field("`z last`", Some("src")),
		];
		let at = |name: &str| fields.iter().position(|fd| fd.name.to_raw_string() == name).unwrap();

		let eval = field_eval_order(&fields);

		let rank = |name: &str| eval.order.iter().position(|&i| i == at(name)).unwrap();
		assert!(eval.cycle.is_none());
		assert!(rank("`z last`") < rank("`a first`"), "a computed field follows what it reads");
		assert!(rank("`z last`") < rank("copied"), "a VALUE follows the computed field it reads");
		assert!(rank("`a first`") < rank("`copied 2`"));
		assert_eq!(
			eval.required_computed,
			[at("`a first`"), at("`z last`")].into_iter().collect::<std::collections::HashSet<_>>(),
			"both computed fields are read by another field's clause"
		);
	}

	/// A cycle through names that have to be backticked is found; a field
	/// reading its own name is not one.
	#[test]
	fn finds_cycles_through_escaped_names() {
		let cyclic = vec![valued("`m n`", "`o p`"), valued("`o p`", "`m n`")];
		let eval = field_eval_order(&cyclic);
		let mut cycle = eval.cycle.expect("a cycle through escaped names");
		cycle.sort();
		assert_eq!(cycle, ["`m n`", "`o p`"]);

		let own = vec![valued("`own n`", "`own n` ?? 'init'")];
		assert!(field_eval_order(&own).cycle.is_none(), "reading itself is not a cycle");
	}

	/// A computed field whose name has to be quoted is keyed and ordered by the
	/// name a read of it records, which is the unescaped identifier: the
	/// dependency map finds it, and a field reading `` `is-locked` `` is derived
	/// after it, both in the write path's field order and in the computed order
	/// the derivations sort by.
	#[test]
	fn a_quoted_computed_field_is_keyed_and_ordered_by_its_unescaped_name() {
		let fields = vec![
			field("can_edit", Some("!`is-locked`")),
			field("`is-locked`", Some("status = 'locked'")),
			field("ready", Some("`verrouillé` == false")),
			field("`verrouillé`", Some("can_edit == false")),
		];
		let deps = computed_field_deps(&fields);
		assert!(deps.contains_key("is-locked"), "{:?}", deps.keys());
		assert!(deps.contains_key("verrouillé"), "{:?}", deps.keys());

		let check = |order: &[&str]| {
			let at = |name: &str| order.iter().position(|n| *n == name).expect(name);
			assert!(at("is-locked") < at("can_edit"), "{order:?}");
			assert!(at("can_edit") < at("verrouillé"), "{order:?}");
			assert!(at("verrouillé") < at("ready"), "{order:?}");
		};
		let eval = field_eval_order(&fields);
		assert!(eval.cycle.is_none());
		let names: Vec<&str> =
			eval.order.iter().map(|&i| fields[i].name.field_name().unwrap()).collect();
		check(&names);

		let graph: Vec<(String, Vec<String>)> = fields
			.iter()
			.map(|fd| {
				let name = fd.name.field_name().unwrap().to_owned();
				let reads = deps[&name].fields.clone();
				(name, reads)
			})
			.collect();
		let names: Vec<&str> = topological_sort_computed_fields(&graph)
			.into_iter()
			.map(|i| graph[i].0.as_str())
			.collect();
		check(&names);
	}

	/// The write path's field graph knows a field whose name needs quoting by
	/// the name a read of it records: a clause reading one is ordered after it,
	/// a COMPUTED one an `ASSERT` reads is materialised for it, and a cycle
	/// among them is reported, which is what makes the write path refuse it.
	#[test]
	fn the_field_graph_matches_quoted_fields_by_their_unescaped_names() {
		let fields = vec![valued("`a-dep`", "`z-src` + 1"), valued("`z-src`", "10")];
		let order = field_eval_order(&fields);
		assert_eq!(order.order, vec![1, 0], "`z-src` is processed before `a-dep`");
		assert!(order.cycle.is_none());

		let mut guard = field("guard", None);
		guard.assert = Some(syn::expr("`is-locked` != true").unwrap());
		let fields = vec![guard, field("`is-locked`", Some("status = 'locked'"))];
		let order = field_eval_order(&fields);
		assert!(order.required_computed.contains(&1), "the ASSERT's computed read is materialised");

		let fields = vec![valued("`c-1`", "`c-2`"), valued("`c-2`", "`c-1`")];
		assert!(field_eval_order(&fields).cycle.is_some(), "a quoted cycle is reported");
	}

	/// A top-level field whose quoted name spells a path is not the nested field
	/// at that path. A read of the quoted field resolves to it whichever of the
	/// two the field list holds last, a nested field reading it is ordered after
	/// it rather than taken to be reading itself, and a cycle through it is
	/// reported.
	#[test]
	fn the_field_graph_tells_a_quoted_field_from_a_nested_path() {
		let quoted = || field("`is.locked`", Some("status = 'locked'"));
		let nested = || field("is.locked", None);
		let guard = || {
			let mut guard = field("guard", None);
			guard.assert = Some(syn::expr("`is.locked` != true").unwrap());
			guard
		};
		for fields in [vec![guard(), quoted(), nested()], vec![guard(), nested(), quoted()]] {
			let computed = fields.iter().position(|fd| fd.computed.is_some()).unwrap();
			let order = field_eval_order(&fields);
			assert!(order.required_computed.contains(&computed), "{:?}", order.required_computed);
			assert_eq!(order.required_computed.len(), 1, "{:?}", order.required_computed);
		}

		let fields = vec![valued("is.locked", "`is.locked`"), quoted()];
		let order = field_eval_order(&fields);
		assert_eq!(order.order, vec![1, 0], "the quoted field is processed first");
		assert!(order.required_computed.contains(&1), "and derived for the nested one");

		let fields =
			vec![valued("`is.locked`", "x"), valued("is.locked", "1"), valued("x", "`is.locked`")];
		assert!(field_eval_order(&fields).cycle.is_some(), "the cycle through it is reported");
	}

	/// The `id` of `reading` naming `city` and `time`, then the fields in
	/// `rest`, as the catalog lists them: the two virtual definitions of the
	/// components straight after `id`.
	fn keyed(id: FieldDefinition, rest: Vec<FieldDefinition>) -> Vec<FieldDefinition> {
		let mut id = id;
		id.set_type_strict(
			Some(Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Datetime]))),
			Some(KeyNames::new(vec![Strand::new("city"), Strand::new("time")], 2).unwrap()),
		)
		.unwrap();
		let derived = id.live_key().unwrap().derived_fields(&TableName::from("reading"));
		let mut all = vec![id];
		all.extend(derived);
		all.extend(rest);
		all
	}

	fn stored_value(name: &str, value: &str) -> FieldDefinition {
		FieldDefinition {
			name: syn::idiom(name).unwrap(),
			value: Some(syn::expr(value).unwrap()),
			..Default::default()
		}
	}

	fn names(fields: &[FieldDefinition], order: &[usize]) -> Vec<String> {
		order.iter().map(|&i| fields[i].name.to_raw_string()).collect()
	}

	/// A component derives from the id alone, so it is processed ahead of the
	/// fields that are ready beside it, and a field that reads it comes after
	/// it. A component nothing reads is not required of the validation pass.
	#[test]
	fn a_component_is_ordered_before_the_fields_that_read_it() {
		let fields =
			keyed(field("id", None), vec![stored_value("label", "city"), field("temp", None)]);
		let order = field_eval_order(&fields);

		assert!(order.cycle.is_none());
		assert_eq!(names(&fields, &order.order), ["city", "time", "id", "label", "temp"]);
		assert_eq!(order.order.len(), fields.len(), "every field is ordered exactly once");
		assert_eq!(order.required_computed, HashSet::from([1]), "only the component `label` reads");
	}

	/// A clause of the `id` that reads a component is not a dependency cycle:
	/// the component derives from the id, and it has no dependency of its own
	/// for the order to follow back to the id.
	#[test]
	fn an_id_clause_reading_a_component_is_no_cycle() {
		let mut id = field("id", None);
		id.default = DefineDefault::Set(syn::expr("city").unwrap());
		let fields = keyed(id, vec![field("temp", None)]);
		let order = field_eval_order(&fields);

		assert!(order.cycle.is_none(), "{:?}", order.cycle);
		assert_eq!(names(&fields, &order.order), ["city", "time", "id", "temp"]);
		assert!(order.required_computed.contains(&1));
	}

	/// A genuine cycle between ordinary fields is still reported, beside
	/// components.
	#[test]
	fn a_real_cycle_is_still_reported_beside_components() {
		let fields = keyed(
			field("id", None),
			vec![stored_value("a", "b"), stored_value("b", "a"), field("temp", None)],
		);
		let order = field_eval_order(&fields);

		let mut cycle = order.cycle.clone().expect("a and b read each other");
		cycle.sort();
		assert_eq!(cycle, ["a", "b"]);
		assert_eq!(order.order.len(), fields.len());
	}

	/// A table that names no component is ordered by its dependencies alone,
	/// taking the lowest-numbered ready field each time.
	#[test]
	fn a_table_without_components_is_ordered_by_dependency_alone() {
		let fields =
			vec![field("id", None), stored_value("b", "c"), field("c", None), field("d", None)];
		let order = field_eval_order(&fields);

		assert!(order.cycle.is_none());
		assert_eq!(order.order, [0, 2, 1, 3]);
		assert!(order.required_computed.is_empty());
		assert!(fields.iter().all(|f| !f.is_key_part()));
	}

	/// The plan names the id and every field by their place in the one list the
	/// order is derived from, so the place of the id is the same whether the
	/// components are in the list or not, and every place the order names is a
	/// field of the list.
	#[test]
	fn the_places_a_plan_names_are_the_places_in_the_list() {
		let fields = keyed(field("id", None), vec![field("temp", None)]);
		let id = id_field_index(&fields).unwrap();
		assert_eq!(fields[id].name.to_raw_string(), "id");
		assert!(fields[id].key_part.is_none());

		let order = field_eval_order(&fields);
		let mut seen = order.order;
		seen.sort_unstable();
		assert_eq!(seen, (0..fields.len()).collect::<Vec<usize>>());
	}

	/// The order a table whose `id` names no component is processed in: Kahn's
	/// algorithm over every field's dependency edges and the parent edges,
	/// taking the lowest-numbered ready field each time. Kept here as the
	/// reference [`field_eval_order`] is held to for such a table.
	fn dependency_order(
		fields: &[FieldDefinition],
	) -> (Vec<usize>, HashSet<usize>, Option<Vec<String>>) {
		use std::collections::{BTreeSet, HashMap};
		fn add_edge(from: usize, to: usize, edges: &mut [Vec<usize>], indegree: &mut [usize]) {
			if from != to && !edges[from].contains(&to) {
				edges[from].push(to);
				indegree[to] += 1;
			}
		}
		let by_name: HashMap<String, usize> =
			fields.iter().enumerate().map(|(i, fd)| (fd.name.to_raw_string(), i)).collect();
		let mut edges: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
		let mut indegree: Vec<usize> = vec![0; fields.len()];
		let mut deps_by_field: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];
		for (i, fd) in fields.iter().enumerate() {
			for dep in fd.production_dependencies() {
				if let Some(&j) = by_name.get(&dep) {
					deps_by_field[i].push(j);
					add_edge(j, i, &mut edges, &mut indegree);
				}
			}
		}
		for (i, fd) in fields.iter().enumerate() {
			for (j, other) in fields.iter().enumerate() {
				if i != j && other.name.len() > fd.name.len() && other.name.starts_with(&fd.name) {
					add_edge(i, j, &mut edges, &mut indegree);
				}
			}
		}
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
		(order, required_computed, cycle)
	}

	/// A deterministic source of choices (xorshift64*), so a failing case is
	/// reproduced by its seed.
	struct Choices(u64);

	impl Choices {
		fn below(&mut self, n: usize) -> usize {
			self.0 ^= self.0 >> 12;
			self.0 ^= self.0 << 25;
			self.0 ^= self.0 >> 27;
			(self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 33) as usize % n
		}
	}

	/// A field list drawn from `pool`, in an arbitrary order, each field with
	/// clauses that read other names of the pool, itself, or a name no field
	/// has.
	fn random_fields(choices: &mut Choices, pool: &[&str]) -> Vec<FieldDefinition> {
		let mut names: Vec<&str> = pool.to_vec();
		let mut fields = Vec::new();
		while !names.is_empty() && fields.len() < 9 {
			let name = names.swap_remove(choices.below(names.len()));
			if choices.below(4) == 0 {
				continue;
			}
			let reads = |choices: &mut Choices| -> Option<crate::expr::Expr> {
				if choices.below(3) != 0 {
					return None;
				}
				let read: Vec<&str> = (0..choices.below(4))
					.map(|_| match choices.below(pool.len() + 1) {
						n if n == pool.len() => "absent",
						n => pool[n],
					})
					.collect();
				let text = if read.is_empty() {
					"1".to_owned()
				} else {
					read.join(" + ")
				};
				Some(syn::expr(&text).unwrap())
			};
			let mut fd = FieldDefinition {
				name: syn::idiom(name).unwrap(),
				..Default::default()
			};
			fd.value = reads(choices);
			fd.assert = reads(choices);
			fd.computed = reads(choices);
			fd.default = match (reads(choices), choices.below(2)) {
				(Some(expr), 0) => DefineDefault::Set(expr),
				(Some(expr), _) => DefineDefault::Always(expr),
				(None, _) => DefineDefault::None,
			};
			fields.push(fd);
		}
		fields
	}

	/// For a table whose `id` names no component, the order, the computed
	/// fields required and the cycle reported are exactly those of the
	/// dependency order alone, over every shape of dependency graph: parents
	/// and children, reads of absent fields, self reads, cycles and `ASSERT`
	/// reads.
	#[test]
	fn a_table_without_components_keeps_the_dependency_order_exactly() {
		const POOL: &[&str] =
			&["id", "a", "b", "c", "d", "e", "a.x", "a.y", "a.x.q", "b.z", "temp", "in"];
		// How many lists were reordered, had a computed field required and had a
		// cycle: each shape has to be drawn for the comparison to cover it.
		let (mut reordered, mut requiring, mut cyclic) = (0, 0, 0);
		for seed in 1..=4000_u64 {
			let mut choices = Choices(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
			let fields = random_fields(&mut choices, POOL);
			assert!(fields.iter().all(|f| !f.is_key_part()));
			let found = field_eval_order(&fields);
			let (order, required, cycle) = dependency_order(&fields);
			reordered += usize::from(order.windows(2).any(|pair| pair[0] > pair[1]));
			requiring += usize::from(!required.is_empty());
			cyclic += usize::from(cycle.is_some());
			let listed: Vec<String> = fields.iter().map(|f| f.name.to_raw_string()).collect();
			assert_eq!(found.order, order, "seed {seed}: order of {listed:?}");
			assert_eq!(found.required_computed, required, "seed {seed}: required of {listed:?}");
			assert_eq!(found.cycle, cycle, "seed {seed}: cycle of {listed:?}");
		}
		assert!(
			reordered > 100 && requiring > 100 && cyclic > 100,
			"{reordered} {requiring} {cyclic}"
		);
	}

	/// For a table whose `id` names components that no field reads, the
	/// components come first and the other fields keep the order, required
	/// computed fields and cycle they would have without the components.
	#[test]
	fn components_nothing_reads_leave_the_other_fields_in_dependency_order() {
		const POOL: &[&str] = &["a", "b", "c", "d", "a.x", "a.y", "b.z", "temp"];
		for seed in 1..=2000_u64 {
			let mut choices = Choices(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
			let rest = random_fields(&mut choices, POOL);
			let mut plain = vec![field("id", None)];
			plain.extend(rest.iter().cloned());
			let (order, required, cycle) = dependency_order(&plain);

			let fields = keyed(field("id", None), rest);
			let found = field_eval_order(&fields);
			// `id` keeps its place; every other field moves past the two components.
			let shifted = |i: usize| {
				if i == 0 {
					0
				} else {
					i + 2
				}
			};
			let mut expected: Vec<usize> = vec![1, 2];
			expected.extend(order.iter().map(|&i| shifted(i)));
			assert_eq!(found.order, expected, "seed {seed}");
			assert_eq!(
				found.required_computed,
				required.iter().map(|&i| shifted(i)).collect::<HashSet<usize>>(),
				"seed {seed}"
			);
			assert_eq!(found.cycle, cycle, "seed {seed}");
		}
	}
}
