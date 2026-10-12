//! What a write resolves once per statement per table, and every record's
//! stages then share.
//!
//! Resolution reads the catalog and compiles physical expressions, so it must
//! not happen per record. A statement that writes to more than one table — an
//! `INSERT` whose rows name their own — holds one of these per table.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use surrealdb_datastore::index_shapes::TableIndexShapes;

use super::scope::WriteScope;
use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, DatabaseId, Error as CatalogError, EventDefinition, FieldDefinition,
	Generation, IndexDefinition, KeyStructure, NamespaceDefinition, NamespaceId, RecordFormat,
	RecordWriter, SubscriptionDefinition, TableDefinition, TableLayouts,
};
use crate::doc::{FieldEvalOrder, NsDbCtx, NsDbTbMutCtx};
use crate::exec::ExecutionContext;
use crate::expr::computed_deps::ComputedDeps;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::kvs::Transaction;
use crate::val::TableName;

/// Whether the statement's table must already exist.
///
/// `UPDATE` and `DELETE` require it, and name the missing table. The four
/// creating forms do not: the table is defined on demand, as writing the first
/// record of a schemaless table has always done.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableExistence {
	/// The table must exist; a missing one is the statement's failure.
	Required,
	/// A missing table is defined by this write.
	DefinedOnDemand,
}

/// The format a statement's record writes give the rows of its table, as resolved at one layout
/// epoch of its transaction ([`Transaction::layout_epoch`]), with the layouts a packed format
/// reads its descriptor from.
///
/// Filled by this statement's record writes, one record at a time, for this context's table in
/// this context's transaction. A slot answers only for the epoch it was filled at: a mint, a
/// range delete over held layouts or a savepoint rollback that undid one of those or the
/// stored-format ledger entry moves the epoch and so empties it, because each can change which
/// format is current or whether the entry is part of the transaction. A hit costs two atomic
/// loads and no lock, hash lookup, allocation or reference count.
///
/// One word holds both the epoch and the format, so a reader never sees an epoch with a format
/// written for another, and any number of writers may fill it. The high 32 bits hold the epoch
/// and the low 32 bits the format's code: 0 for [`RecordFormat::Revision`], `stamp + 1` for
/// [`RecordFormat::Document`], and [`Self::PACKED`] with `stamp + 1` for
/// [`RecordFormat::Packed`], where the stamp is the generation's number, or 0 for a document with
/// no generation. A format whose code does not fit, or an epoch above `u32::MAX`, is
/// not cached, and every record of such a statement resolves the format again.
///
/// The layouts a packed format needs are set once, by the first packed fill. The word claims a
/// packed format at an epoch only when the layouts resolved at that epoch are the very ones held
/// (`Arc::ptr_eq`), so a hit lends layouts resolved at the epoch it answers for. A fill whose
/// layouts are others, which happens when a mid-statement mint or a rollback replaced them, is not
/// cached: the rows of such a statement resolve through the transaction.
#[derive(Debug)]
pub(crate) struct RecordFormatSlot {
	/// `(epoch << 32) | code` of the last fill; [`Self::EMPTY`] when it never was.
	word: AtomicU64,
	/// The layouts the slot's packed format was resolved from.
	layouts: OnceLock<Arc<TableLayouts>>,
}

/// A format a [`RecordFormatSlot`] answered, with the layouts it lends for a packed one.
pub(crate) struct SlotHit<'a> {
	pub(crate) format: RecordFormat,
	pub(crate) layouts: Option<&'a TableLayouts>,
}

/// The format of one record write: lent by a statement's slot, or resolved by the transaction.
pub(crate) enum ResolvedFormat<'a> {
	Slot(SlotHit<'a>),
	Writer(RecordWriter),
}

impl ResolvedFormat<'_> {
	/// The format the record is written in.
	pub(crate) fn format(&self) -> RecordFormat {
		match self {
			Self::Slot(hit) => hit.format,
			Self::Writer(writer) => writer.format(),
		}
	}

	/// The layouts a packed format reads its descriptor from, and a write that names its record is
	/// checked against.
	pub(crate) fn layouts(&self) -> Option<&TableLayouts> {
		match self {
			Self::Slot(hit) => hit.layouts,
			Self::Writer(writer) => writer.layouts().map(|layouts| &**layouts),
		}
	}
}

impl RecordFormatSlot {
	/// The word of a slot nothing has filled. No fill produces it: a fill's code is below
	/// `u32::MAX`.
	const EMPTY: u64 = u64::MAX;

	/// The bit of a code that marks a packed format.
	const PACKED: u32 = 0x8000_0000;

	/// A slot that answers for no epoch.
	pub(crate) fn empty() -> Self {
		Self {
			word: AtomicU64::new(Self::EMPTY),
			layouts: OnceLock::new(),
		}
	}

	/// The format resolved at `epoch`, or `None` when the slot was filled at another epoch or
	/// never, or holds a word that is no fill's: a code is a fill's only when it is 0 or names a
	/// generation, the codes [`Self::set`] writes, and a packed code is a fill's only when the slot
	/// holds layouts.
	pub(crate) fn get(&self, epoch: u64) -> Option<SlotHit<'_>> {
		if epoch > u64::from(u32::MAX) {
			return None;
		}
		let word = self.word.load(Ordering::Acquire);
		if word == Self::EMPTY || word >> 32 != epoch {
			return None;
		}
		let code = u32::try_from(word & u64::from(u32::MAX)).ok()?;
		if code == 0 {
			return Some(SlotHit {
				format: RecordFormat::Revision,
				layouts: None,
			});
		}
		let stamp = u16::try_from((code & !Self::PACKED).checked_sub(1)?).ok()?;
		let generation = Generation::new(stamp);
		if code & Self::PACKED == 0 {
			return Some(SlotHit {
				format: RecordFormat::Document {
					generation,
				},
				layouts: None,
			});
		}
		let generation = generation?;
		let layouts = self.layouts.get()?;
		Some(SlotHit {
			format: RecordFormat::Packed {
				generation,
			},
			layouts: Some(&**layouts),
		})
	}

	/// Records that `writer`'s format is the table's current one at `epoch`, unless the epoch or
	/// the format's code does not fit the word, or the writer is packed and its layouts are not
	/// the ones the slot holds.
	pub(crate) fn set(&self, epoch: u64, writer: &RecordWriter) {
		if epoch > u64::from(u32::MAX) {
			return;
		}
		let code_of = |stamp: u16| u32::from(stamp) + 1;
		let code = match writer.format() {
			RecordFormat::Revision => 0,
			RecordFormat::Document {
				generation,
			} => code_of(generation.map_or(0, Generation::get)),
			RecordFormat::Packed {
				generation,
			} => {
				let Some(layouts) = writer.layouts() else {
					return;
				};
				let held = self.layouts.get_or_init(|| Arc::clone(layouts));
				if !Arc::ptr_eq(held, layouts) {
					return;
				}
				code_of(generation.get()) | Self::PACKED
			}
		};
		self.word.store((epoch << 32) | u64::from(code), Ordering::Release);
	}

	/// The format the rows of `tb` are written in, answered from the slot while it stands.
	///
	/// The slot answers when it was filled at the transaction's current layout epoch, with no
	/// lock, hash lookup or allocation. Otherwise the transaction resolves the writer
	/// ([`Transaction::record_writer`]), which commits the stored-format ledger entry with the
	/// first record that needs it, and the slot is filled at the epoch read *before* that call,
	/// so an event that moves the epoch during it leaves the slot stale and the next record
	/// resolves again.
	///
	/// The slot describes one table: a caller passes the same `tb` every time.
	pub(crate) async fn resolve(
		&self,
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> anyhow::Result<ResolvedFormat<'_>> {
		let epoch = txn.layout_epoch();
		if let Some(hit) = self.get(epoch) {
			return Ok(ResolvedFormat::Slot(hit));
		}
		let writer = txn.record_writer(ns, db, tb).await?;
		self.set(epoch, &writer);
		Ok(ResolvedFormat::Writer(writer))
	}
}

/// Resolved once per statement per table, shared by every record.
#[derive(Debug)]
pub(crate) struct WriteCtx {
	/// The namespace the record belongs to. Held as the definition rather than
	/// the name because every storage key the stages write is keyed on its id.
	pub(crate) ns: Arc<NamespaceDefinition>,
	/// The database the record belongs to, for the same reason.
	pub(crate) db: Arc<DatabaseDefinition>,
	/// The table being written.
	pub(crate) table: Arc<TableDefinition>,
	/// The table's field definitions, in the order the catalog holds them.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// The event bodies this write triggers.
	pub(crate) events: Arc<[EventDefinition]>,
	/// The foreign materialised views this write maintains.
	pub(crate) views: Arc<[TableDefinition]>,
	/// The indexes this write maintains.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// The live-query subscriptions this write notifies.
	pub(crate) lives: Arc<[SubscriptionDefinition]>,
	/// The VM's units of `lives`, one for each.
	pub(crate) live_units: crate::exec::vm::live::LiveUnitList,

	/// The order the field clauses are applied in, so a clause that reads
	/// another field of the same record sees the written value rather than the
	/// prior one. Carries the cycle a table's clauses form, when they form one;
	/// the record is rejected rather than the table.
	pub(crate) field_order: Arc<FieldEvalOrder>,
	/// Which same-table fields each computed field reads, keyed by field name.
	pub(crate) computed_deps: HashMap<String, ComputedDeps>,
	/// Index of the `id` field in `fields`, so id coercion reads its kind and
	/// default without rescanning the list per record.
	pub(crate) id_field: Option<usize>,
	/// What the array keys of the table are made of, when the `id` field's type
	/// names their components (`TYPE [city: string, time: datetime]`); `None`
	/// for every other table. The record id stages consult it to build the key
	/// from the components a record is written with, to hold every element to
	/// its component's type, and to verify the components a record restates
	/// against the key. It is read from the same field list as `fields`, so it
	/// describes exactly the schema the stages apply.
	pub(crate) key: Option<Arc<KeyStructure>>,

	/// The context the stages evaluate against, and the only handle to the
	/// transaction. No operator begins, commits or cancels one; a write
	/// obtains it from here and from nowhere else.
	pub(crate) exec: ExecutionContext,

	/// The statement scope that resolved this context, which is how a stage
	/// reaches what one statement owns across tables and nesting levels — the
	/// other tables' contexts, and the compiled plans of the statements the
	/// write performs on its own behalf.
	///
	/// Weak because the scope owns the contexts. Set by
	/// [`WriteScope::write_ctx_for`], the only production construction site;
	/// [`Self::resolve`] leaves it empty, so a context a test builds directly
	/// has no scope and a stage that needs one says so rather than appearing
	/// to work.
	pub(crate) scope: Weak<WriteScope>,
	/// The table's catalog version when the context was resolved, and how
	/// many catalog stamps the transaction had made then: a context resolved
	/// before a change to its table's catalog no longer stands
	/// ([`Self::stands`]).
	pub(crate) resolved_at: (Option<uuid::Uuid>, u64),
	/// The format this statement's record writes give this table's rows, cached per layout epoch
	/// ([`RecordFormatSlot`]).
	pub(crate) format: RecordFormatSlot,
	/// The layout epoch of the transaction ([`Transaction::layout_epoch`]) read after this
	/// statement's last forfeit of its own table's conformance record, which an `OPTION IMPORT`
	/// row stamped with a generation makes before it lands; `u64::MAX` before any. A row skips
	/// the forfeit only while the epoch still equals it: a save-point rollback that undoes the
	/// forfeit moves the epoch, so the forfeit is made again.
	pub(crate) conformance_forfeit: AtomicU64,
	/// The value shapes the table's B-tree indexes have stored, as the
	/// transaction saw them when the context was resolved: a write of a shape
	/// one has not recorded records it ([`surrealdb_datastore::index_shapes`]).
	pub(crate) index_shapes: TableIndexShapes,
}

impl WriteCtx {
	/// Resolve the write context for one table.
	///
	/// Reads the per-table definitions a write needs: fields, events, foreign
	/// views, indexes and subscriptions, plus the field evaluation order
	/// derived from them.
	///
	/// It does **not** apply the statement-level table permission gate, which
	/// drops a target silently when the caller holds no permission on it —
	/// before any record is built, and therefore before any
	/// stage list runs. Nothing here covers it and no `StageId` can, because it
	/// is not a per-record step: it is owed by whatever builds the source, and
	/// is owed before a write operator is first planned.
	pub(crate) async fn resolve(
		exec: &ExecutionContext,
		table: &TableName,
		existence: TableExistence,
	) -> Result<Self, ControlFlow> {
		let db_ctx = exec.database().context("a write requires a database context")?;
		let parent = NsDbCtx {
			ns: Arc::clone(&db_ctx.ns_ctx.ns),
			db: Arc::clone(&db_ctx.db),
		};
		let version = exec.version_stamp();
		let tb = match existence {
			TableExistence::Required => {
				db_ctx.get_table_def(table, version).await.map_err(ControlFlow::Err)?.ok_or_else(
					|| {
						ControlFlow::Err(anyhow::anyhow!(CatalogError::TbNotFound {
							name: table.to_owned()
						}))
					},
				)?
			}
			TableExistence::DefinedOnDemand => exec
				.txn()
				.get_or_add_tb(Some(exec.ctx()), &parent.ns.name, &parent.db.name, table, version)
				.await
				.map_err(ControlFlow::Err)?,
		};
		let txn = exec.txn();
		let stamps = txn.catalog_stamps();
		let catalog_version = txn
			.catalog_version(parent.ns.namespace_id, parent.db.database_id, table)
			.await
			.map_err(ControlFlow::Err)?;
		let catalog = NsDbTbMutCtx::load(exec.ctx(), &parent, tb, table, version)
			.await
			.map_err(ControlFlow::Err)?;
		let key = catalog
			.id_field_idx
			.and_then(|index| catalog.fields.get(index))
			.and_then(FieldDefinition::live_key)
			.cloned();
		let index_shapes = txn
			.table_index_shapes(
				parent.ns.namespace_id,
				parent.db.database_id,
				table,
				&catalog.indexes,
			)
			.await
			.map_err(ControlFlow::Err)?;
		Ok(Self {
			ns: catalog.ns,
			db: catalog.db,
			table: catalog.tb,
			fields: catalog.fields,
			events: catalog.events,
			views: catalog.tables,
			indexes: catalog.indexes,
			lives: catalog.lives,
			live_units: catalog.live_units,
			field_order: catalog.field_order,
			computed_deps: catalog.computed_deps,
			id_field: catalog.id_field_idx,
			key,
			exec: exec.clone(),
			scope: Weak::new(),
			resolved_at: (catalog_version, stamps),
			format: RecordFormatSlot::empty(),
			conformance_forfeit: AtomicU64::new(u64::MAX),
			index_shapes,
		})
	}

	/// This context's catalog reads, with `exec` in place of its own.
	///
	/// Sound only while the catalog this context read is the one `exec`
	/// reads: the caller guarantees that nothing has changed it since, and
	/// that `exec` reads it at the same version, namespace and database.
	pub(crate) fn rebound(&self, exec: &ExecutionContext) -> Self {
		Self {
			ns: Arc::clone(&self.ns),
			db: Arc::clone(&self.db),
			table: Arc::clone(&self.table),
			fields: Arc::clone(&self.fields),
			events: Arc::clone(&self.events),
			views: Arc::clone(&self.views),
			indexes: Arc::clone(&self.indexes),
			lives: Arc::clone(&self.lives),
			live_units: Arc::clone(&self.live_units),
			field_order: Arc::clone(&self.field_order),
			computed_deps: self.computed_deps.clone(),
			id_field: self.id_field,
			key: self.key.clone(),
			exec: exec.clone(),
			scope: Weak::new(),
			resolved_at: self.resolved_at,
			format: RecordFormatSlot::empty(),
			conformance_forfeit: AtomicU64::new(u64::MAX),
			index_shapes: self.index_shapes.clone(),
		}
	}

	/// Whether the table's catalog is the one this context read: the
	/// transaction has stamped no catalog version since, or none of this
	/// table's. The version is read through the transaction's cache, a seam
	/// of the run-time seal ([`crate::exec::compiles::seam`]).
	pub(crate) async fn stands(&self) -> Result<bool, ControlFlow> {
		let txn = self.exec.txn();
		let (version, stamps) = self.resolved_at;
		if txn.catalog_stamps() == stamps {
			return Ok(true);
		}
		let now = crate::exec::vm::descriptor::object(txn.catalog_version(
			self.ns.namespace_id,
			self.db.database_id,
			&self.table.name,
		))
		.await
		.map_err(ControlFlow::Err)?;
		Ok(now == version)
	}

	/// The transaction the stages write through.
	///
	/// Read from the execution context on every call rather than cached beside
	/// it, so there is one handle and no way for the two to disagree.
	pub(crate) fn txn(&self) -> Arc<Transaction> {
		self.exec.txn()
	}

	/// The context the stages evaluate expressions against.
	pub(crate) fn exec(&self) -> &ExecutionContext {
		&self.exec
	}

	/// The statement scope this context was resolved by.
	///
	/// Infallible while a stage runs: the operator's stream holds the scope for
	/// its whole life, and every context a record's stages are handed came from
	/// it. An empty one therefore means a context built outside a scope — a
	/// test harness — has reached a stage that needs one, which is a broken
	/// invariant rather than a condition to recover from.
	pub(crate) fn scope(&self) -> Result<Arc<WriteScope>, ControlFlow> {
		self.scope.upgrade().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(crate::err::Error::unreachable(
				"a write stage asked for its statement scope, which is not set"
			)))
		})
	}
}
