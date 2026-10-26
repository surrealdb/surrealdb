//! The per-record state a write's stages thread through one record.
//!
//! One [`WriteRow`] per record. Stages mutate it in sequence and never overlap,
//! and at most one record is in flight per transaction, so nothing here is
//! shared or synchronised. Everything a stage needs that is *not* per record —
//! the table's schema, the statement's clauses, the transaction — is on
//! [`WriteCtx`](super::ctx::WriteCtx) instead.

use std::cell::OnceCell;

use surrealdb_datastore::tx::ProvisionalDraw;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Generation, Metadata, Record};
use crate::exec::Error as ExecError;
#[cfg(not(target_family = "wasm"))]
use crate::exec::memory::size::value_bytes;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::ControlFlow;
use crate::val::{Object, RecordId, RecordIdKey, TableName, Value};

/// The evaluated data clause a record's stages apply.
///
/// This is the `doc` module's type, under the name the write operator uses for it.
/// It is an alias, not a copy, so the two declarations cannot drift apart.
///
/// The helpers on it are not crate-visible and must not become so: they
/// evaluate through the surface `Value` mutators rather than the compiled
/// places the write stages use.
pub(crate) type ComputedInput = crate::doc::alter::ComputedData;

/// The statement-specific payload a record carries: `INSERT`'s own row value,
/// or `RELATE`'s endpoints and value.
///
/// Like [`ComputedInput`], this is an alias of the `doc` module's type.
pub(crate) type WriteExtras = crate::doc::Extras;

/// A row moves from a worker task to the coordinator, so it must be `Send`.
/// It is not `Sync` (`modified` is a `OnceCell`), and no row is shared.
const _: fn() = || {
	fn send<T: Send>() {}
	send::<WriteRow>();
};

/// The stored bytes of a record's post-image, encoded under the format the table's slot held at one
/// layout epoch of the transaction.
///
/// The bytes stand for the record as it was when they were made. The stage that stores the record
/// uses them only while the transaction's layout epoch is the one they were made at and the format
/// it resolves carries the generation they were stamped with; otherwise it encodes the image
/// itself.
#[derive(Debug)]
pub(crate) struct EncodedImage {
	pub(crate) bytes: Vec<u8>,
	pub(crate) epoch: u64,
	pub(crate) generation: Option<crate::catalog::Generation>,
}

/// The record-type marker stored with the record `id`, read through the transaction of `exec`
/// at its read version.
///
/// A read of stored data: the statement's task makes it, never a worker of a write.
pub(crate) async fn stored_metadata(
	exec: &crate::exec::ExecutionContext,
	id: &RecordId,
) -> Result<Option<Metadata>, ControlFlow> {
	#[cfg(test)]
	crate::exec::vm::note_txn_read_on_worker();
	let db = exec.database().map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
	let stored = exec
		.txn()
		.get_record(
			db.ns_ctx.ns.namespace_id,
			db.db.database_id,
			&id.table,
			&id.key,
			exec.version_stamp(),
		)
		.await
		.map_err(ControlFlow::Err)?;
	Ok(stored.metadata.clone())
}

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
	///
	/// `OutputWrite` is the last stage of every order. Once it has shown an
	/// image, the row may no longer hold that image.
	pub(crate) initial: Value,
	/// Post-image the stages build and `store_record_data` writes verbatim.
	/// Starts as a clone of `initial`.
	///
	/// `OutputWrite` is the last stage of every order. Once it has shown an
	/// image, the row may no longer hold that image.
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
	/// The generation stamp of the stored record this row adopted, until its
	/// migration steps have been replayed ([`WriteRow::replay_stored`]): the
	/// outer `None` is a row with nothing to replay — one that adopted nothing
	/// from storage, a creating order's subject or a row a row source produced,
	/// whose value arrives as that source read it, or one already replayed —
	/// and the inner `None` a record with no generation.
	pub(crate) stored_stamp: Option<Option<Generation>>,
	/// Whether the running statistics an aggregated view row is computed from
	/// moved with this write, for a row that does not carry them
	/// ([`WriteRow::for_view_row`]). Always false for any other row.
	pub(crate) state_moved: bool,

	/// Whether `current` currently carries its `COMPUTED` field values.
	///
	/// A correctness condition, not a cache hint. The derivation short-circuits on it, and
	/// every stage that changes the record's field values must clear it:
	/// applying the data clause, and stripping the computed values before
	/// storage. A stale `true` makes the scratch copy a write-permission
	/// predicate is judged against inherit values that are no longer there, so
	/// a predicate naming a `COMPUTED` field reads `NONE`, so the guard opens
	/// instead of holding.
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

	/// The post-image as stored, encoded ahead of the stage that stores it by the part of a write
	/// that runs apart from its apply ([`EncodedImage`]). `None` for every other record.
	pub(crate) encoded: Option<EncodedImage>,

	/// Whether the attempt in flight got as far as storing its record: past
	/// every gate that skips a record before it writes, whether or not the
	/// store then changed anything (an unchanged record, a `DROP` table). Set
	/// by the store stage, cleared by [`Self::reset_for_update_retry`].
	///
	/// A creating or updating attempt runs behind a savepoint, and a skip
	/// before this point means the attempt wrote nothing a record answers to,
	/// so its savepoint is rolled back. A skip after it only withholds output
	/// (`RETURN NONE`, a select permission, an import), and what the attempt
	/// wrote stands.
	pub(crate) reached_store: bool,

	/// The values the creating attempt drew from sequences the transaction
	/// defined while deriving this record's id: in a data clause evaluated
	/// before the record had an id, and in the `id` field's `DEFAULT`. A
	/// creating attempt's rollback returns them to their sequences, though the
	/// id may go on to name the record an update retry writes;
	/// [`Self::replay_id_draws`] moves the sequences past them again.
	pub(crate) id_draws: Vec<ProvisionalDraw>,

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
	/// here would make one record whose id *is* the range, not the records
	/// it spans. Failing is the only safe reading.
	/// Callers expand the target before this constructor is reached.
	pub(crate) fn for_target(target: Value) -> Result<Self, ControlFlow> {
		let unsupported = |value: Value| {
			Err(ControlFlow::Err(anyhow::anyhow!(ExecError::InvalidStatementTarget {
				value: value.to_sql(),
			})))
		};
		if let Value::RecordId(id) = target {
			if id.key.is_range() {
				unsupported(Value::RecordId(id))
			} else {
				Ok(Self::empty(Some(id), None))
			}
		} else if let Value::Table(table) = target {
			Ok(Self::empty(None, Some(table)))
		} else {
			unsupported(target)
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
	///
	/// `state_moved` says the statistics an aggregated view row is computed
	/// from changed, which a row that holds none of them cannot show by its
	/// images: the row then counts as modified though its images are equal.
	pub(crate) fn for_view_row(
		id: RecordId,
		initial: Option<&Record>,
		current: Option<&Record>,
		state_moved: bool,
	) -> Self {
		Self {
			initial: initial.map(|record| record.data.clone()).unwrap_or(Value::None),
			current: current.map(|record| record.data.clone()).unwrap_or(Value::None),
			initial_metadata: initial.and_then(|record| record.metadata.clone()),
			metadata: current.and_then(|record| record.metadata.clone()),
			state_moved,
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
			stored_stamp: None,
			state_moved: false,
			fields_computed: false,
			initial_fields_computed: false,
			input: None,
			extras: WriteExtras::Normal,
			retry: false,
			mutated: false,
			encoded: None,
			reached_store: false,
			id_draws: Vec::new(),
			doc_id_removal_deferred: false,
			matched: false,
			output: None,
			modified: OnceCell::new(),
		}
	}

	/// After a creating attempt was rolled back, draw again from the sequences
	/// the transaction defined the values deriving this record's id drew, when
	/// the record is skipped or updated under that id: `target` is the record
	/// the duplicate-id conflict or the retry names. Left returned, the next
	/// record would draw the same id, and skip or update this record again
	/// instead of creating the next one. A sequence committed earlier is never
	/// restored by a rollback, so it needs nothing. Where `target` is another
	/// record (a unique-index conflict), the id is discarded with the attempt
	/// and its values are free to be drawn again.
	///
	/// Only the allocators move; see `Sequences::replay_provisional_draws`.
	/// Nothing that derived the id is evaluated again.
	pub(crate) async fn replay_id_draws(
		&mut self,
		ctx: &WriteCtx,
		target: &RecordId,
	) -> Result<(), ControlFlow> {
		let draws = std::mem::take(&mut self.id_draws);
		if draws.is_empty() || self.id.as_ref() != Some(target) {
			return Ok(());
		}
		let frozen = ctx.exec().ctx();
		let sequences = frozen.try_get_sequences().map_err(ControlFlow::Err)?;
		sequences
			.replay_provisional_draws(Some(frozen), &ctx.txn(), &draws)
			.await
			.map_err(ControlFlow::Err)
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
		*self.modified.get_or_init(|| {
			self.state_moved
				|| self.initial != self.current
				|| self.initial_metadata != self.metadata
		})
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
	/// The marker is read through the statement's transaction by
	/// [`stored_metadata`]. The compute half of a write that fans out never
	/// reads it: the statement's task reads it ahead for each row it hands on
	/// and the half takes it with [`WriteRow::adopt_metadata`].
	pub(crate) async fn load_stored_metadata(&mut self, ctx: &WriteCtx) -> Result<(), ControlFlow> {
		if self.initial.is_none() {
			return Ok(());
		}
		let Some(id) = self.id.clone() else {
			return Ok(());
		};
		let stored = stored_metadata(ctx.exec(), &id).await?;
		self.adopt_metadata(stored);
		Ok(())
	}

	/// Take the record-type marker a read of the stored record answered, as
	/// [`WriteRow::load_stored_metadata`] does for a row that read nothing
	/// else.
	pub(crate) fn adopt_metadata(&mut self, stored: Option<Metadata>) {
		if self.initial.is_none() || self.id.is_none() {
			return;
		}
		self.initial_metadata.clone_from(&stored);
		self.metadata = stored;
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
		#[cfg(test)]
		crate::exec::vm::note_txn_read_on_worker();
		let (stored, stamp) = crate::exec::migrate::stamped_record(
			&ctx.txn(),
			ctx.ns.namespace_id,
			ctx.db.database_id,
			&id.table,
			&id.key,
			ctx.exec().version_stamp(),
		)
		.await
		.map_err(ControlFlow::Err)?;
		self.adopt_stored(&stored, stamp);
		self.replay_stored(ctx.exec()).await
	}

	/// Bring both images of a row that adopted a stored record through the
	/// migration steps of its table that the record's stamp has not passed.
	///
	/// Every stage reads the pre-image, so replaying where it is adopted gives
	/// every stage — the data clauses, the schema checks, `READONLY`, the
	/// permission gates, events and notifications — the migrated values. The
	/// store stamps what it writes with the table's head, which is after every
	/// step. A row that adopted nothing from storage is left as it is, and a
	/// row is replayed once per adoption.
	pub(crate) async fn replay_stored(
		&mut self,
		exec: &crate::exec::ExecutionContext,
	) -> Result<(), ControlFlow> {
		let Some(stamp) = self.stored_stamp.take() else {
			return Ok(());
		};
		let Some(id) = self.id.as_ref() else {
			return Ok(());
		};
		if self.initial.is_none() {
			return Ok(());
		}
		let Some(replay) = crate::exec::migrate::replay_for(exec, &id.table, None).await? else {
			return Ok(());
		};
		if !replay.behind(stamp) {
			return Ok(());
		}
		replay.apply(exec, &mut self.initial, stamp).await?;
		self.current.clone_from(&self.initial);
		Ok(())
	}

	/// Start the after-image of a record whose stored content is not an object
	/// from the empty object of its id, the content a `CREATE` of that id starts
	/// from. A data clause that edits content runs on that object, so it
	/// replaces the stored scalar with an object rather than failing or doing
	/// nothing. The before-image is left as stored.
	///
	/// Does nothing for a row that read no record (`initial` is `NONE`) or whose
	/// content is already an object.
	pub(crate) fn content_from_id(&mut self) {
		if self.initial.is_none() || self.current.is_object() {
			return;
		}
		self.object_of_id();
	}

	/// Set the after-image to the empty object of the row's id, or to an empty
	/// object when the row names no id yet (a table target whose id is generated
	/// later).
	pub(crate) fn object_of_id(&mut self) {
		let mut object = Object::default();
		if let Some(id) = self.id.clone() {
			object.insert("id", Value::RecordId(id));
		}
		self.current = object.into();
	}

	/// Take both images and both markers from a record read from storage,
	/// stamped with `stamp`.
	///
	/// The before- and after-image start equal, as they do for every subject a
	/// row source produced: the stages that follow are what move them apart.
	pub(crate) fn adopt_stored(&mut self, stored: &Record, stamp: Option<Generation>) {
		self.initial.clone_from(&stored.data);
		self.current.clone_from(&stored.data);
		self.initial_metadata.clone_from(&stored.metadata);
		self.metadata.clone_from(&stored.metadata);
		self.stored_stamp = Some(stamp);
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
	///
	/// `stamp` is the stamp of `record`, as [`WriteRow::adopt_stored`] takes
	/// it.
	pub(crate) fn reset_for_update_retry(
		&mut self,
		id: RecordId,
		record: &Record,
		stamp: Option<Generation>,
	) {
		self.id = Some(id);
		self.generate_in = None;
		self.retry = true;
		self.mutated = false;
		self.encoded = None;
		self.reached_store = false;
		self.doc_id_removal_deferred = false;
		self.output = None;
		self.initial.clone_from(&record.data);
		self.current.clone_from(&record.data);
		self.initial_metadata.clone_from(&record.metadata);
		self.metadata.clone_from(&record.metadata);
		self.stored_stamp = Some(stamp);
		self.initial_reduced = None;
		self.current_reduced = None;
		self.fields_computed = false;
		self.initial_fields_computed = false;
		self.input = None;
		self.modified = OnceCell::new();
	}

	/// The bytes this record in flight holds.
	///
	/// The row's own size plus the values behind `initial`, `current`, the two reduced images,
	/// `output` and the evaluated data clause in `input`, counted by
	/// [`value_bytes`]. A `Set` clause counts its assignment values; an `Unset` clause counts its
	/// idiom slots by `size_of` only, as the parts of an idiom are not walked. `extras` and the
	/// metadata slots are counted by the struct's size alone.
	///
	/// An estimate for a memory account: close enough to bound the candidates held at once, and
	/// cheap enough to take once per record. It walks borrowed values and never clones.
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn held_bytes(&self) -> usize {
		let images = [
			Some(&self.initial),
			Some(&self.current),
			self.initial_reduced.as_ref(),
			self.current_reduced.as_ref(),
			self.output.as_ref(),
		];
		let input = match &self.input {
			None => 0,
			Some(
				ComputedInput::Patch(value)
				| ComputedInput::Merge(value)
				| ComputedInput::Replace(value)
				| ComputedInput::Content(value),
			) => value_bytes(value),
			Some(ComputedInput::Unset(idioms)) => std::mem::size_of_val(idioms.as_slice()),
			Some(ComputedInput::Set(assignments)) => {
				assignments.iter().map(|assignment| value_bytes(&assignment.value)).sum()
			}
		};
		let encoded = self.encoded.as_ref().map_or(0, |encoded| encoded.bytes.capacity());
		images.into_iter().flatten().map(value_bytes).fold(
			std::mem::size_of::<Self>().saturating_add(input).saturating_add(encoded),
			usize::saturating_add,
		)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn held_bytes_grows_with_the_images() {
		let light = WriteRow::for_insert(TableName::from("t"), None);
		let mut heavy = WriteRow::for_insert(TableName::from("t"), None);
		heavy.current = Value::String("x".repeat(1024).into());
		assert!(heavy.held_bytes() >= light.held_bytes() + 1024);
	}
}
