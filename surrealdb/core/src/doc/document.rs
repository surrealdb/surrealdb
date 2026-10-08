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
pub(crate) fn field_eval_order(fields: &[FieldDefinition]) -> FieldEvalOrder {
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
				txn.all_tb_lives(ns, db, table, version),
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
