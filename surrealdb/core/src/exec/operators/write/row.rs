//! The per-record state a write's stages thread through one record.
//!
//! One [`WriteRow`] per record. Stages mutate it in sequence and never overlap,
//! and at most one record is in flight per transaction, so nothing here is
//! shared or synchronised. Everything a stage needs that is *not* per record —
//! the table's schema, the statement's clauses, the transaction — is on
//! [`WriteCtx`](super::ctx::WriteCtx) instead.

use std::cell::OnceCell;

use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Metadata, Record};
use crate::exec::Error as ExecError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::ControlFlow;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// The evaluated data clause a record's stages apply.
///
/// It is the `doc` module's own shape, under the name the write operator uses
/// for it, rather than a second enum kept in step with it: two declarations of
/// one shape drift.
///
/// The helpers on it are not crate-visible and must not become so: they
/// evaluate through the surface `Value` mutators rather than the compiled
/// places the write stages use.
pub(crate) type ComputedInput = crate::doc::alter::ComputedData;

/// The statement-specific payload a record carries: `INSERT`'s own row value,
/// or `RELATE`'s endpoints and value.
///
/// As with [`ComputedInput`], this is the `doc` module's own shape under the
/// write operator's name for it, not a copy.
pub(crate) type WriteExtras = crate::doc::Extras;

/// Per-record state threaded through a stage list.
#[derive(Debug)]
pub(crate) struct WriteRow {
	/// Identity of the record being written. `None` until `generate_record_id`
	/// derives one, which happens only on a creating order whose target named a
	/// table rather than a record.
	pub(crate) id: Option<RecordId>,
	/// The table to derive an id in. Set by a creating order whose target named
	/// a table, `None` on every order whose subject already has an id.
	///
	/// **It stays set once `generate_record_id` has derived the id.** Two later
	/// readers depend on that, and clearing it is silent in both:
	///
	/// - `is_specific_record_id` is `generate_in.is_none()`, read *after* the create attempt to
	///   decide whether a unique-index conflict is retryable as an update. Cleared, every
	///   generated-id conflict starts raising instead of updating.
	/// - `check_data_fields` stands down entirely when it is set, which is what lets `CREATE foo
	///   CONTENT { id: bar:123 }` take its key from the content. Cleared, that becomes an
	///   `IdMismatch`.
	///
	/// The single place it is cleared is
	/// [`WriteRow::reset_for_update_retry`]. `WriteRow::table()` reads `id` first, so
	/// leaving it set costs nothing.
	pub(crate) generate_in: Option<TableName>,

	/// Before-image, as the record stood when the statement read it.
	/// `Value::None` for a record being created. `DELETE`'s select gate reads
	/// it, as do `RETURN BEFORE`, `RETURN DIFF` and `RETURN <fields>`.
	///
	/// Whether the record is stored anew or overwritten is keyed on this and
	/// not on which order was selected: an order chosen as creating may still
	/// have had a record id put into it by the data clause, and overwriting one
	/// under a create permission is the write the two-sided test refuses.
	pub(crate) initial: Value,
	/// Post-image the stages build and `store_record_data` writes verbatim.
	/// Starts as a clone of `initial`.
	pub(crate) current: Value,
	/// Permission-reduced counterparts, derived on demand. The data clause
	/// reads the reduced pre-image, so a caller's data clause cannot observe a
	/// field the caller may not select. `None` until a stage that needs one
	/// derives it; a caller whose reads are unchecked never needs either.
	pub(crate) initial_reduced: Option<Value>,
	pub(crate) current_reduced: Option<Value>,

	/// The record-type marker the post-image carries into storage. `RELATE`
	/// stamps the current adjacency-key generation here before the record is
	/// written, so the stored marker always matches the edge keys the later
	/// stages emit. `None` for a plain record.
	///
	/// Carried beside the image rather than inside it because the images are
	/// `Value`, which is what the expression layer evaluates against;
	/// a stored record is a `Value` plus this marker.
	pub(crate) metadata: Option<Metadata>,
	/// The marker the before-image carried. An update of an existing edge
	/// re-stamps nothing and must carry this through to the new write.
	pub(crate) initial_metadata: Option<Metadata>,

	/// Whether `current` currently carries its `COMPUTED` field values.
	///
	/// Load-bearing, not a cache hint. The derivation short-circuits on it, and
	/// every stage that changes the record's field values must clear it:
	/// applying the data clause, and stripping the computed values before
	/// storage. A stale `true` makes the scratch copy a write-permission
	/// predicate is judged against inherit values that are no longer there, so
	/// a predicate naming a `COMPUTED` field reads `NONE` and the guard opens
	/// (GHSA-f6h4-5xf2-86q9).
	///
	/// Cleared by [`WriteRow::reset_for_update_retry`] along with the images.
	pub(crate) fields_computed: bool,

	/// Whether `initial` carries its `COMPUTED` values.
	///
	/// The pre-image's counterpart to [`WriteRow::fields_computed`], and set by
	/// the same stage. A reader whose subject is the pre-image — a queued
	/// `ASYNC` event on a delete — records it alongside the snapshot, so the
	/// drain knows whether the bodies still have to be derived. The table
	/// declaring a computed field does not answer this: an order that reaches an
	/// observer without materialising, as view maintenance does, leaves the
	/// pre-image without the values the schema names.
	///
	/// Cleared by [`WriteRow::reset_for_update_retry`] along with the images.
	pub(crate) initial_fields_computed: bool,

	/// Product of `compute_input_data`, consumed by `process_record_data`,
	/// `process_merge_data` and `check_data_fields`. Memoised because
	/// recomputing it re-runs the user expressions in the data clause; cleared
	/// by [`WriteRow::reset_for_update_retry`], which changes the record the
	/// clause is evaluated against.
	pub(crate) input: Option<ComputedInput>,

	/// `INSERT`'s row value, or `RELATE`'s `(from, to, value)`. `Normal` for
	/// every other form.
	pub(crate) extras: WriteExtras,

	/// Whether the record is being processed for the second time, after a
	/// creating attempt was rolled back. Set by
	/// [`WriteRow::reset_for_update_retry`] and read by
	/// [`WriteRow::is_iteration_initial`], which is how `UPSERT` and `INSERT`
	/// decide whether to attempt the creating order at all.
	pub(crate) retry: bool,

	/// Set true only after a real KV write completes. Read once per record by
	/// `counts_as_affected` in [`super`], which is what drives the statement's
	/// affected-row total — independently of what the statement returns — and
	/// is reset when a creating attempt is rolled back so the retry does not
	/// count twice.
	///
	/// Setting it anywhere but after a successful storage write inflates a
	/// number the executor reports as fact.
	pub(crate) mutated: bool,

	/// Whether an in-flight index build deferred this record's doc-id removal.
	/// `store_index_data` reports it; `reclaim_doc_id`, the next stage of
	/// `DELETE`'s order, acts on it. Carried on the row rather than returned,
	/// because a stage's outcome is [`StageOutcome`](super::stage::StageOutcome)
	/// and the two stages are deliberately separate: folding the decision into
	/// the index stage, or moving it past the purge, breaks a concurrent build's
	/// replay.
	pub(crate) doc_id_removal_deferred: bool,

	/// Whether the statement's `WHERE` condition accepted this record. Set by
	/// `check_where_condition` on every accepting return — the condition
	/// evaluated truthy, or the statement carries no condition — and read once
	/// per record by the write operator, which needs to know whether the
	/// statement reached a record: an `UPSERT <table> ... WHERE` writes its
	/// guaranteed record only when none did.
	///
	/// Independent of what the record showed the caller. A record the caller may
	/// write and may not select is matched and emits nothing, and so is one
	/// under `RETURN NONE`; the gates ahead of the condition leave it `false`,
	/// so a record the caller may not update is a record the statement did not
	/// match.
	pub(crate) matched: bool,

	/// The row the statement emits for this record, produced by `output_write`.
	/// `None` when the `RETURN` clause produces none — `RETURN NONE`, and a
	/// bare `DELETE` — which is why the `ONLY` cardinality rule counts rows the
	/// statement returned rather than records it wrote.
	pub(crate) output: Option<Value>,

	/// Memoised deep compare of `initial` against `current`, valid only once
	/// every mutation phase has run. Read by the store, the indexes, the views,
	/// the events, the live queries and the change feed, so each record
	/// compares at most once.
	pub(crate) modified: OnceCell<bool>,
}

impl WriteRow {
	/// The row for a subject that already exists: a record read by the write's
	/// row source.
	///
	/// The value is the record as read, so it carries its own `id`; a value
	/// that does not is not a record and cannot be the subject of a write. The
	/// refusal names the value the source yielded rather than the `id` it
	/// looked for there, which is the one the statement was written with.
	///
	/// # The metadata slots come out empty and are filled before any stage runs
	///
	/// A batch carries `Vec<Value>`, and a stored record is
	/// `{ metadata, data }`, so the record-type marker is not in what this
	/// constructor is handed. [`WriteRow::load_stored_metadata`] fills both
	/// slots from storage once the write context has resolved, which is the
	/// first moment the table's ids and the transaction are in scope, and
	/// every row a row source produces goes through it.
	pub(crate) fn for_record(value: Value) -> Result<Self, ControlFlow> {
		let Value::RecordId(id) = value.rid() else {
			return Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatementTarget {
				value: value.to_sql(),
			})));
		};
		Ok(Self {
			initial: value.clone(),
			current: value,
			..Self::empty(Some(id), None)
		})
	}

	/// The row for a subject a creating order names: a record id to write at,
	/// or a table to derive one in. Nothing is read, because the subject of a
	/// creating statement does not exist yet.
	/// A target that names more than one subject — a record-id range, an array
	/// of ids, an object carrying one, a `|table:n|` generator — is refused
	/// rather than accepted as one. Each of those is expanded before a row
	/// exists; this shape is one expression to one subject, so taking a range
	/// here would
	/// write a single record whose id *is* the range instead of the records it
	/// spans. Failing is the only safe reading.
	pub(crate) fn for_target(target: Value) -> Result<Self, ControlFlow> {
		let unsupported = |value: Value| {
			Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatementTarget {
				value: value.to_sql(),
			})))
		};
		match target {
			Value::RecordId(id) if id.key.is_range() => unsupported(Value::RecordId(id)),
			Value::RecordId(id) => Ok(Self::empty(Some(id), None)),
			Value::Table(table) => Ok(Self::empty(None, Some(table))),
			other => unsupported(other),
		}
	}

	/// The row one `INSERT` row is written through, with its table and key
	/// already settled from the row's own `id` field and the statement's
	/// `INTO`.
	///
	/// It arrives split rather than as one value because `INSERT` decides the
	/// two separately: `INTO` supplies the table, and the row's `id` field
	/// supplies only the key when there is one. A row that named no key leaves
	/// the table for `generate_record_id` to derive one in, exactly as a target
	/// naming a table does.
	///
	/// A key that is a range is carried rather than refused here. The `id` a
	/// row carries is a claim the statement makes about the record, and
	/// `check_data_fields` judges it with the rest of them.
	///
	/// The row's value is not the record image: it is the statement's payload,
	/// carried on [`WriteRow::extras`] and merged onto the record by
	/// `process_merge_data` once the id is settled.
	pub(crate) fn for_insert(table: TableName, key: Option<RecordIdKey>) -> Self {
		match key {
			Some(key) => Self::empty(Some(RecordId::new(table, key)), None),
			None => Self::empty(None, Some(table)),
		}
	}

	/// The row a view-maintenance order runs over: a row this write
	/// synthesised on a view table, rather than a subject the statement named.
	///
	/// Both images are handed in whole, because a view row is not read the way
	/// a statement's subject is: the maintenance has just written or deleted
	/// it and holds the before and after itself. A view row's stored `data`
	/// carries no `id` — the row's identity is its key, and splicing it back in
	/// would change what the view table's own observers are shown — so the id
	/// comes from the caller and the metadata from the records.
	pub(crate) fn for_view_row(
		id: RecordId,
		initial: Option<&Record>,
		current: Option<&Record>,
	) -> Self {
		Self {
			initial: initial.map(|record| record.data.clone()).unwrap_or(Value::None),
			current: current.map(|record| record.data.clone()).unwrap_or(Value::None),
			initial_metadata: initial.and_then(|record| record.metadata.clone()),
			metadata: current.and_then(|record| record.metadata.clone()),
			..Self::empty(Some(id), None)
		}
	}

	/// A row with no images, for one of the two constructors above.
	fn empty(id: Option<RecordId>, generate_in: Option<TableName>) -> Self {
		Self {
			id,
			generate_in,
			initial: Value::None,
			current: Value::None,
			initial_reduced: None,
			current_reduced: None,
			metadata: None,
			initial_metadata: None,
			fields_computed: false,
			initial_fields_computed: false,
			input: None,
			extras: WriteExtras::Normal,
			retry: false,
			mutated: false,
			doc_id_removal_deferred: false,
			matched: false,
			output: None,
			modified: OnceCell::new(),
		}
	}

	/// The table this record belongs to: the id's table once there is an id,
	/// and the table an id is to be derived in before that.
	pub(crate) fn table(&self) -> Option<&TableName> {
		match &self.id {
			Some(id) => Some(&id.table),
			None => self.generate_in.as_ref(),
		}
	}

	/// Whether a form that selects its order up front selects the creating one.
	///
	/// Read from the post-image, which is what the selection sees: it runs
	/// before any order-specific stage, so the image is still the record as
	/// read. It is not a substitute for the before-image test the store makes —
	/// see [`WriteRow::initial`].
	pub(crate) fn is_new(&self) -> bool {
		self.current.is_nullish()
	}

	/// Whether the creating order may still be attempted. False once a rolled
	/// back attempt has handed the record to the updating order, and false for
	/// a subject the row source read rather than named.
	pub(crate) fn is_iteration_initial(&self) -> bool {
		!self.retry && self.initial.is_none()
	}

	/// Whether the statement named this record's id itself rather than leaving
	/// one to be derived. A conflict against an id the caller named is not a
	/// conflict the creating order may retry as an update.
	pub(crate) fn is_specific_record_id(&self) -> bool {
		match &self.extras {
			WriteExtras::Insert(value) => !value.rid().is_nullish(),
			WriteExtras::Normal => self.generate_in.is_none(),
			WriteExtras::Relate(_, _, _) => false,
		}
	}

	/// Whether the post-image differs from the before-image. Valid only after
	/// every mutation phase has run; memoised, so each record compares once.
	///
	/// A stored record is its marker and its value, and both count: an edge
	/// whose adjacency-key layout this write advanced carries the same value
	/// under a different marker, and an aggregated view row's stats move while
	/// its projected fields stand still. Comparing the images alone would
	/// leave both looking untouched, and every stage keyed on this answer
	/// would stand down over a record that did change.
	pub(crate) fn is_modified(&self) -> bool {
		*self
			.modified
			.get_or_init(|| self.initial != self.current || self.initial_metadata != self.metadata)
	}

	/// Fill both record-type markers from storage, for a row a row source
	/// produced.
	///
	/// A row source yields `Value`s, and the marker lives beside the data on
	/// the stored `Record`, so it cannot travel in the batch. Three stages
	/// dispatch on it and are silently wrong without it: `store_edges_data`
	/// reads the before-image's marker to decide which superseded layout to
	/// clean up, `store_inline_adjacency_data` stands down unless the
	/// before-image names an edge in the current layout, and
	/// `purge_record_data` deletes a record's four graph keys only when it
	/// does — so an unfilled marker leaves every edge deleted through a
	/// row-sourced `DELETE` with its adjacency keys still on disk, pointing at
	/// a record that is gone.
	///
	/// A row that read nothing has no marker to load: a creating order's
	/// subject does not exist yet, and the retry path fills both slots from
	/// the `Record` it read itself
	/// ([`WriteRow::reset_for_update_retry`]).
	///
	/// The read is served from the transaction's record cache in the ordinary
	/// case, the row source having just read the same record through it.
	pub(crate) async fn load_stored_metadata(&mut self, ctx: &WriteCtx) -> Result<(), ControlFlow> {
		if self.initial.is_none() {
			return Ok(());
		}
		let Some(id) = self.id.clone() else {
			return Ok(());
		};
		let stored = ctx
			.txn()
			.get_record(
				ctx.ns.namespace_id,
				ctx.db.database_id,
				&id.table,
				&id.key,
				ctx.exec().version_stamp(),
			)
			.await
			.map_err(ControlFlow::Err)?;
		self.initial_metadata.clone_from(&stored.metadata);
		self.metadata.clone_from(&stored.metadata);
		Ok(())
	}

	/// Read the record this row's subject named, so a caller choosing between
	/// a creating and an updating order sees what is there.
	///
	/// Both images and both markers come from the stored record, as
	/// [`WriteRow::reset_for_update_retry`] takes them from the record a
	/// rolled-back attempt conflicted with. A subject that named no id — a
	/// target naming a table, whose id `generate_record_id` derives — has
	/// nothing to read and is left as it is, so it reads as new.
	///
	/// The row must be untouched: this is the subject as the source produced
	/// it, before any stage of either order has run.
	pub(crate) async fn load_named_subject(&mut self, ctx: &WriteCtx) -> Result<(), ControlFlow> {
		let Some(id) = self.id.clone() else {
			return Ok(());
		};
		let stored = ctx
			.txn()
			.get_record(
				ctx.ns.namespace_id,
				ctx.db.database_id,
				&id.table,
				&id.key,
				ctx.exec().version_stamp(),
			)
			.await
			.map_err(ControlFlow::Err)?;
		self.adopt_stored(&stored);
		Ok(())
	}

	/// Take both images and both markers from a record read from storage.
	///
	/// The before- and after-image start equal, as they do for every subject a
	/// row source produced: the stages that follow are what move them apart.
	pub(crate) fn adopt_stored(&mut self, stored: &Record) {
		self.initial.clone_from(&stored.data);
		self.current.clone_from(&stored.data);
		self.initial_metadata.clone_from(&stored.metadata);
		self.metadata.clone_from(&stored.metadata);
	}

	/// Hand a rolled-back creating attempt to the updating order, against the
	/// record the conflict named.
	///
	/// Everything the attempt derived is dropped: the id becomes the
	/// conflicting record's, the table an id would have been derived in is
	/// gone, and the evaluated data clause is invalidated because it is
	/// evaluated against the record, which has just changed. The mutation flag
	/// is cleared so a record the attempt half-wrote is not counted, and
	/// [`WriteRow::is_iteration_initial`] answers false from here on.
	pub(crate) fn reset_for_update_retry(&mut self, id: RecordId, record: &Record) {
		self.id = Some(id);
		self.generate_in = None;
		self.retry = true;
		self.mutated = false;
		self.doc_id_removal_deferred = false;
		self.output = None;
		self.initial.clone_from(&record.data);
		self.current.clone_from(&record.data);
		self.initial_metadata.clone_from(&record.metadata);
		self.metadata.clone_from(&record.metadata);
		self.initial_reduced = None;
		self.current_reduced = None;
		self.fields_computed = false;
		self.initial_fields_computed = false;
		self.input = None;
		self.modified = OnceCell::new();
	}
}
