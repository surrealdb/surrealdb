//! Shared utilities for streaming execution operators.
//!
//! Contains helpers that are used by multiple scan operators (reference scan,
//! graph edge scan, etc.) to avoid code duplication.

// Not every helper in this module has a caller in every build, so dead code is allowed.
#![allow(dead_code)]

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

use surrealdb_strand::Strand;

use super::pipeline::{FieldState, build_field_state, materialise_fields_with_permissions};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Record, StoredRecord, TableLayouts};
use crate::err::EngineError;
use crate::exec::migrate::{Replay, stamp_of};
use crate::exec::object_extract::{
	PathSegment, project_packed_record_bytes, project_record_bytes, project_record_bytes_excluding,
};
use crate::exec::permission::PhysicalPermission;
use crate::exec::{Clause, ControlFlowExt, EvalContext, ExecutionContext};
use crate::expr::{ControlFlow, FlowResult};
use crate::key::schema::RecordKey;
use crate::kvs::{CachePolicy, DatastoreError, LayoutSet, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Fail the query when the process is past `SURREAL_MEMORY_THRESHOLD`.
///
/// Scans that materialise records call this once per fetched batch, and
/// again after computed fields are materialised into their rows and after
/// cache-shared records are cloned out of the transaction's cache. A query
/// that keeps decoding records fails with
/// [`DatastoreError::QueryBeyondMemoryThreshold`] rather than growing the
/// process until the operating system kills it. Without allocation tracking
/// the threshold is never reached and this always returns `Ok`.
pub(crate) fn ensure_below_memory_threshold() -> FlowResult<()> {
	if crate::mem::ALLOC.is_beyond_threshold() {
		crate::mem::record_refusal(crate::mem::RefusalSite::Query);
		return Err(ControlFlow::Err(anyhow::Error::new(
			DatastoreError::QueryBeyondMemoryThreshold,
		)));
	}
	Ok(())
}

/// The most rows in a scan's first record batch, before any row size is known.
///
/// Small enough that many concurrent queries do not each start with a full
/// row-capped batch; [`probe_batch_len`] takes fewer when `scan_batch_bytes`
/// is below the default.
pub(crate) const RECORD_PROBE_ROWS: usize = 32;

/// Decoded bytes a probe batch assumes per row, before any row size is known.
const PROBE_ROW_BYTES: usize = 64 << 10;

/// The number of rows in the first batch of a scan sized to `budget` bytes.
///
/// No row size is known yet, so the probe assumes rows of
/// `PROBE_ROW_BYTES`, about one 1024-float embedding decoded: a probe of such
/// rows stays within `budget`, and a smaller budget takes a smaller probe.
/// The result is never below 1 and never above `max_rows`.
pub(crate) fn probe_batch_len(budget: usize, max_rows: usize) -> usize {
	(budget / PROBE_ROW_BYTES).clamp(1, max_rows)
}

/// The number of records the next read may take so that it stays near
/// `budget` bytes.
///
/// A record's size is the average of `read_bytes` over `read_rows`, as
/// [`ReadAhead::read`] reports them: the bytes the read held, before any
/// permission check.
///
/// The estimate describes the read just made, so the next read is at most
/// twice `current`, the length of that read. A run of narrow records grows
/// the read gradually, and wide records are read in a chunk no larger than
/// twice the last one before the size shrinks. The result is never below 1
/// and never above `max_rows`. A read that found no record keeps `current`.
pub(crate) fn next_batch_len(
	read_bytes: usize,
	read_rows: usize,
	current: usize,
	budget: usize,
	max_rows: usize,
) -> usize {
	if read_rows == 0 {
		return current;
	}
	let per_row = (read_bytes / read_rows).max(1);
	(budget / per_row).min(current.saturating_mul(2)).clamp(1, max_rows)
}

/// Approximate in-memory size of a decoded [`Value`], in bytes.
///
/// Counts one inline `Value` per node, plus the bytes of strings and byte
/// buffers, a key slot and the key's bytes per object entry, a coordinate per
/// geometry point, and the contents of record-id keys and ranges; any other
/// variant counts only its inline size. It sizes
/// batches, so it trades precision for a single pass with no allocation.
/// Recursion is bounded by the value nesting limit enforced at decode.
pub(crate) fn approx_value_size(value: &Value) -> usize {
	let inline = std::mem::size_of::<Value>();
	match value {
		Value::String(s) => inline + s.len(),
		Value::Bytes(b) => inline + b.0.len(),
		Value::Array(a) => inline + a.iter().map(approx_value_size).sum::<usize>(),
		Value::Set(s) => inline + s.0.iter().map(approx_value_size).sum::<usize>(),
		Value::Object(o) => {
			let entry = std::mem::size_of::<surrealdb_strand::Strand>();
			inline + o.iter().map(|(k, v)| entry + k.len() + approx_value_size(v)).sum::<usize>()
		}
		Value::Geometry(g) => inline + geometry_heap_size(g),
		Value::RecordId(rid) => inline + rid.table.as_str().len() + approx_key_size(&rid.key),
		Value::Range(r) => {
			inline
				+ std::mem::size_of::<crate::val::Range>()
				+ approx_bound_size(&r.start, approx_value_size)
				+ approx_bound_size(&r.end, approx_value_size)
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => inline,
	}
}

/// The top-level fields a scan decodes from each record it fetches.
///
/// Built at plan time from the fields the statement reads off its rows:
/// either the only fields to decode or, for a `*` projection, the fields
/// not to decode (`OMIT` targets nothing else reads). A scan decodes this
/// way when nothing else in the plan reads the stored record (see
/// [`projected_decode_applies`]), and decodes the whole record otherwise.
/// `id` is never listed: a fetched row's id is set from its storage key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodeFields {
	/// The listed root field names, strictly ascending.
	keys: Arc<[PathSegment]>,
	/// Whether `keys` are the fields not to decode rather than the only ones.
	excluding: bool,
}

impl DecodeFields {
	/// Decode only these root fields.
	pub(crate) fn new(fields: impl IntoIterator<Item = String>) -> Self {
		Self::with_mode(fields, false)
	}

	/// Decode every root field except these; `None` when that excludes
	/// nothing stored, since `id` is never decoded from the record (it is set
	/// from the storage key), so a whole-record decode is the cheaper read.
	pub(crate) fn excluding(fields: impl IntoIterator<Item = String>) -> Option<Self> {
		let fields = Self::with_mode(fields, true);
		(!fields.keys.is_empty()).then_some(fields)
	}

	fn with_mode(fields: impl IntoIterator<Item = String>, excluding: bool) -> Self {
		let mut keys: Vec<PathSegment> =
			fields.into_iter().filter(|f| f != "id").map(PathSegment::from).collect();
		keys.sort();
		keys.dedup();
		Self {
			keys: keys.into(),
			excluding,
		}
	}

	/// Whether only the record id (`id`) is needed and no stored fields need to be decoded.
	#[inline]
	pub(crate) fn is_id_only(&self) -> bool {
		!self.excluding && self.keys.is_empty()
	}

	/// Format the decoded field names for `EXPLAIN` output.
	pub(crate) fn describe(&self) -> String {
		let names: Vec<&str> = self.keys.iter().map(|k| k.as_str()).collect();
		format!("[{}]", names.join(", "))
	}

	/// The listed fields and whether they are excluded.
	#[cfg(test)]
	pub(crate) fn listed(&self) -> (Vec<String>, bool) {
		(self.keys.iter().map(|k| k.as_str().to_string()).collect(), self.excluding)
	}

	/// Decode this record's data as the set says, or `None` when its data
	/// cannot be projected and must be decoded in full.
	///
	/// The record's generation was admitted by [`ReadAhead::read`].
	fn project(
		&self,
		record_bytes: &[u8],
		layouts: Option<&TableLayouts>,
		table: &TableName,
	) -> Option<crate::val::Object> {
		if self.excluding {
			project_record_bytes_excluding(record_bytes, &self.keys)
		} else if StoredRecord::is_packed_bytes(record_bytes) {
			project_packed_record_bytes(record_bytes, layouts, table, &self.keys)
		} else {
			project_record_bytes(record_bytes, &self.keys)
		}
	}
}

/// The owned contents of a record-id key, beyond its inline size.
fn approx_key_size(key: &RecordIdKey) -> usize {
	match key {
		RecordIdKey::Number(_) | RecordIdKey::Uuid(_) => 0,
		RecordIdKey::String(s) => s.len(),
		RecordIdKey::Array(a) => a.iter().map(approx_value_size).sum(),
		RecordIdKey::Object(o) => o
			.iter()
			.map(|(k, v)| {
				std::mem::size_of::<surrealdb_strand::Strand>() + k.len() + approx_value_size(v)
			})
			.sum(),
		RecordIdKey::Range(r) => {
			std::mem::size_of::<crate::val::RecordIdKeyRange>()
				+ approx_bound_size(&r.start, approx_key_size)
				+ approx_bound_size(&r.end, approx_key_size)
		}
	}
}

/// A record [`ReadAhead`] holds until a batch decodes it, or until
/// [`ReadAhead::take_records`] hands it on undecoded.
pub(crate) enum ReadAheadRecord {
	/// The record's stored bytes, with the layouts that admitted them when the
	/// record is packed: its decode reads its descriptor from them. A document
	/// and a `revision` record hold none.
	Stored(Vec<u8>, Option<Arc<TableLayouts>>),
	/// The record [`Transaction::get_records`] resolved for a record id with
	/// no stored value (a lightweight edge, or the empty record of a missing
	/// one).
	Resolved(Arc<Record>),
	/// A record id with no stored value, until [`ReadAhead::read`] resolves
	/// it; never left in place once `read` returns.
	Pending,
}

/// Records read from storage ahead of decoding, so a scan decodes each batch
/// only up to its byte budget.
///
/// [`ReadAhead::read`] holds a chunk of records as their stored bytes, a
/// fraction of their decoded size, and [`ReadAhead::decode_batch`] decodes
/// them in read order until the batch reaches the budget. A batch therefore
/// holds at most the budget plus one record of decoded values, however much
/// wider its records are than the ones the chunk was sized from; the records
/// a batch does not decode wait for the next one.
///
/// The budget counts decoded records, so when the scan's pipeline then
/// materialises computed fields that widen them, [`ReadAhead::processed`]
/// caps the rows of the next batch from how wide the processed rows were.
///
/// Stored bytes are read rather than going through the transaction's record
/// cache, so a partially decoded record never enters that cache; the
/// key-value transaction holds the transaction's own writes, so they are read
/// all the same.
pub(crate) struct ReadAhead {
	records: VecDeque<(RecordId, ReadAheadRecord)>,
	select_permission: PhysicalPermission,
	check_perms: bool,
	fields: Option<DecodeFields>,
	budget: usize,
	/// The most records a batch decodes; see [`ReadAhead::processed`].
	row_cap: usize,
	/// The decoded size and record count of the last batch decoded.
	last_bytes: usize,
	last_rows: usize,
}

impl ReadAhead {
	/// An empty read-ahead whose batches decode up to `budget` bytes each.
	///
	/// # Arguments
	///
	/// * `fields` - When set, decode only these fields of each record whose data is an object, and
	///   check no permission: callers establish [`projected_decode_applies`] first. Otherwise
	///   decode whole records and, when `check_perms`, drop those `select_permission` denies.
	pub(crate) fn new(
		select_permission: PhysicalPermission,
		check_perms: bool,
		fields: Option<DecodeFields>,
		budget: usize,
	) -> Self {
		Self {
			records: VecDeque::new(),
			select_permission,
			check_perms,
			fields,
			budget,
			row_cap: usize::MAX,
			last_bytes: 0,
			last_rows: 0,
		}
	}

	/// Whether every record read has been decoded.
	pub(crate) fn is_empty(&self) -> bool {
		self.records.is_empty()
	}

	/// Read the records `rids` names, after any records already held.
	///
	/// Each stored record's generation is admitted against its table's layouts
	/// as it is read, so [`Self::decode_batch`] decodes only admitted bytes. A
	/// record that is refused fails the read.
	///
	/// # Returns
	///
	/// The bytes read and the number of records found, for sizing the next
	/// read with [`next_batch_len`]: the stored bytes of each record with a
	/// stored value, plus the decoded size of each record resolved through
	/// [`Transaction::get_records`].
	pub(crate) async fn read(
		&mut self,
		txn: &Transaction,
		ns_id: NamespaceId,
		db_id: DatabaseId,
		rids: Vec<RecordId>,
		version: Option<u64>,
	) -> Result<(usize, usize), ControlFlow> {
		let keys: Vec<RecordKey<'_>> = rids
			.iter()
			.map(|rid| RecordKey {
				ns: ns_id,
				db: db_id,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
			})
			.collect();
		let stored = txn.getm_raw(keys, version).await.context("Failed to fetch records")?;
		// Each stored row's generation is admitted against its table's layouts
		// before any row is decoded; layouts are loaded for a table only when
		// one of its rows names a generation above 0.
		let mut layouts = LayoutSet::new(ns_id, db_id, version);
		let mut read_bytes = 0;
		let mut read_rows = 0;
		let mut miss_at = Vec::new();
		let mut miss_rids = Vec::new();
		for (rid, bytes) in rids.into_iter().zip(stored) {
			let record = match bytes {
				Some(bytes) => {
					let packed = StoredRecord::is_packed_bytes(&bytes);
					let (_, admitted) = layouts
						.admit_with_layouts(txn, &rid.table, &bytes)
						.await
						.map_err(ControlFlow::Err)?;
					let held = if packed {
						admitted.map(Arc::clone)
					} else {
						None
					};
					read_bytes += bytes.len();
					read_rows += 1;
					ReadAheadRecord::Stored(bytes, held)
				}
				None => {
					miss_at.push(self.records.len());
					miss_rids.push(rid.clone());
					ReadAheadRecord::Pending
				}
			};
			self.records.push_back((rid, record));
		}
		if !miss_rids.is_empty() {
			let records = txn
				.get_records(ns_id, db_id, &miss_rids, version, CachePolicy::ReadOnly)
				.await
				.context("Failed to fetch records")?;
			for (i, record) in miss_at.into_iter().zip(records) {
				if !record.data.is_none() {
					read_rows += 1;
					read_bytes += approx_value_size(&record.data);
				}
				self.records[i].1 = ReadAheadRecord::Resolved(record);
			}
		}
		ensure_below_memory_threshold()?;
		Ok((read_bytes, read_rows))
	}

	/// Take every held record, in read order, undecoded: a stored record as
	/// its bytes, admitted as [`Self::read`] admits them, and a record with no
	/// stored value as the record [`Transaction::get_records`] resolved.
	///
	/// The records are neither permission-checked nor brought through their
	/// table's migration steps: the caller reads them as stored, and owns both.
	/// No [`ReadAheadRecord::Pending`] record is ever handed on.
	pub(crate) fn take_records(&mut self) -> VecDeque<(RecordId, ReadAheadRecord)> {
		std::mem::take(&mut self.records)
	}

	/// Decode held records, in read order, until the batch holds the budget
	/// in decoded values.
	///
	/// Decodes at least one record while any is held, and stops after the
	/// record that brings the decoded size, denied records included, to the
	/// budget, or at the row cap [`ReadAhead::processed`] sets. A record with
	/// no data is skipped.
	///
	/// # Returns
	///
	/// The decoded records the permission check passed, in read order.
	pub(crate) async fn decode_batch(
		&mut self,
		ctx: &ExecutionContext,
	) -> Result<Vec<Value>, ControlFlow> {
		let fields = self.fields.as_ref();
		let mut values = Vec::new();
		let mut decoded_bytes = 0;
		let mut decoded_rows = 0;
		// The steps of the table the previous record belongs to, which the
		// next record of that table reuses.
		let mut steps: Option<(TableName, Option<Replay>)> = None;
		// The first decoded record is taken whatever the budget, so a batch
		// always makes progress while records are held.
		while decoded_rows == 0 || (decoded_bytes < self.budget && decoded_rows < self.row_cap) {
			let Some((rid, record)) = self.records.pop_front() else {
				break;
			};
			let replay = match &steps {
				Some((table, replay)) if *table == rid.table => replay.clone(),
				_ => {
					let replay =
						crate::exec::migrate::replay_for(ctx, &rid.table, ctx.version_stamp())
							.await?;
					steps = Some((rid.table.clone(), replay.clone()));
					replay
				}
			};
			// The stamp of a record behind a migration step of its table, which
			// is decoded whole and brought through the steps it has not passed:
			// its stored bytes are not what a read of it sees.
			let mut replaying = None;
			let mut data = match record {
				ReadAheadRecord::Stored(bytes, layouts) => {
					let stamp = replay.as_ref().map(|_| stamp_of(&bytes));
					if let (Some(replay), Some(stamp)) = (&replay, stamp)
						&& replay.behind(stamp)
					{
						replaying = Some(stamp);
					}
					match fields
						.filter(|_| replaying.is_none())
						.and_then(|fields| fields.project(&bytes, layouts.as_deref(), &rid.table))
					{
						Some(mut object) => {
							object.0.insert(Strand::new_static("id"), Value::RecordId(rid));
							Value::Object(object)
						}
						None => {
							Record::decode_admitted(&bytes, layouts.as_deref(), rid)
								.context("Failed to deserialize record")?
								.data
						}
					}
				}
				ReadAheadRecord::Resolved(record) => match Arc::try_unwrap(record) {
					Ok(record) => record.data,
					Err(record) => record.data.clone(),
				},
				ReadAheadRecord::Pending => {
					return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::Internal(
						"a read-ahead record was left unresolved".into()
					))));
				}
			};
			if data.is_none() {
				continue;
			}
			if let (Some(replay), Some(stamp)) = (&replay, replaying) {
				replay.apply(ctx, &mut data, stamp).await?;
			}
			decoded_bytes += approx_value_size(&data);
			decoded_rows += 1;
			if fields.is_none()
				&& self.check_perms
				&& !crate::exec::permission::check_permission_for_value(
					&self.select_permission,
					&data,
					None,
					ctx,
				)
				.await
				.context("Failed to check permission")?
			{
				continue;
			}
			values.push(data);
		}
		ensure_below_memory_threshold()?;
		self.last_bytes = decoded_bytes;
		self.last_rows = decoded_rows;
		Ok(values)
	}
}

/// The size of a range bound's value, by `size`; 0 for an unbounded end.
fn approx_bound_size<T>(bound: &std::ops::Bound<T>, size: fn(&T) -> usize) -> usize {
	match bound {
		std::ops::Bound::Included(v) | std::ops::Bound::Excluded(v) => size(v),
		std::ops::Bound::Unbounded => 0,
	}
}

/// The heap bytes a geometry owns beyond its inline size: its coordinate
/// buffers and the containers holding its rings, lines, polygons and members,
/// so a geometry of many empty parts is not taken for a narrow one.
fn geometry_heap_size(geometry: &crate::val::Geometry) -> usize {
	use std::mem::size_of;

	use crate::val::Geometry;
	fn line(line: &geo_types::LineString<f64>) -> usize {
		size_of::<geo_types::Coord<f64>>() * line.0.len()
	}
	fn polygon(polygon: &geo_types::Polygon<f64>) -> usize {
		line(polygon.exterior())
			+ polygon
				.interiors()
				.iter()
				.map(|ring| size_of::<geo_types::LineString<f64>>() + line(ring))
				.sum::<usize>()
	}
	match geometry {
		Geometry::Point(_) => 0,
		Geometry::Line(l) => line(l),
		Geometry::Polygon(p) => polygon(p),
		Geometry::MultiPoint(m) => size_of::<geo_types::Point<f64>>() * m.0.len(),
		Geometry::MultiLine(m) => {
			m.0.iter().map(|l| size_of::<geo_types::LineString<f64>>() + line(l)).sum()
		}
		Geometry::MultiPolygon(m) => {
			m.0.iter().map(|p| size_of::<geo_types::Polygon<f64>>() + polygon(p)).sum()
		}
		Geometry::Collection(c) => {
			c.iter().map(|g| size_of::<Geometry>() + geometry_heap_size(g)).sum()
		}
	}
}

/// Convert a [`Value`] to a [`RecordIdKey`] for use in key range construction.
///
/// Used by operators that need to evaluate bound expressions and convert
/// the result into a key suitable for datastore range scans.
pub(crate) fn value_to_record_id_key(val: Value) -> RecordIdKey {
	match val {
		Value::Number(n) => RecordIdKey::Number(n.as_int()),
		Value::String(s) => RecordIdKey::String(s),
		Value::Uuid(u) => RecordIdKey::Uuid(u),
		Value::Array(a) => RecordIdKey::Array(a),
		Value::Object(o) => RecordIdKey::Object(o),
		// For other types, convert to string representation
		other @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => RecordIdKey::String(other.to_raw_string().into()),
	}
}

/// Extract [`RecordId`]s from a [`Value`] into an existing vec.
///
/// Handles single `RecordId` values, arrays of `RecordId`s, and Objects
/// by extracting the `id` field. The extracted `id` is recursively
/// processed, so objects whose `id` is an array of `RecordId`s (or a
/// nested object with its own `id`) are fully traversed, matching
/// SurrealQL semantics where graph traversal on an object uses its `id`.
pub(crate) fn extract_record_ids_into(val: Value, rids: &mut Vec<RecordId>) {
	match val {
		Value::RecordId(rid) => rids.push(rid),
		Value::Object(mut obj) => {
			if let Some(id_val) = obj.remove("id") {
				extract_record_ids_into(id_val, rids);
			}
		}
		Value::Array(arr) => {
			for v in arr {
				extract_record_ids_into(v, rids);
			}
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => {}
	}
}

/// Evaluate a bound expression and convert the result to a [`RecordIdKey`].
///
/// Used by range-bounded scans to turn a bound [`Clause`] into a
/// key that can be encoded into datastore prefix/suffix bytes.
pub(crate) async fn evaluate_bound_key(
	expr: &Clause,
	ctx: &ExecutionContext,
) -> Result<RecordIdKey, ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let val = expr.evaluate(eval_ctx).await?;
	Ok(value_to_record_id_key(val))
}

/// Resolve the VERSION timestamp for a scan operator.
///
/// `version_expr` on a scan operator is the SELECT statement's VERSION
/// clause (propagated by the planner). The planner also wraps every
/// VERSION-bearing SELECT in a [`crate::exec::operators::version_scope::VersionScope`]
/// that evaluates the expression once in the *unversioned* outer
/// context and sets the resulting timestamp on
/// [`ExecutionContext::version_stamp`] before delegating to the inner
/// operator.
///
/// This helper prefers that already-resolved stamp and only falls back
/// to evaluating `version_expr` when no enclosing `VersionScope` was
/// inserted. Re-evaluating the expression inside the scan would run
/// under the just-set `version_stamp`, which can change how nested
/// lookups resolve. A param defined with `VALUE time::now()` is stored at a
/// revision after the snapshot it holds, so a lookup of it at that stamp finds
/// nothing, yields `NONE`, and the cast to `datetime` fails. The
/// `VersionScope` evaluation already produces the right timestamp.
pub(crate) async fn resolve_version_stamp(
	ctx: &ExecutionContext,
	version_expr: Option<&Clause>,
) -> Result<Option<u64>, ControlFlow> {
	if let Some(stamp) = ctx.version_stamp() {
		return Ok(Some(stamp));
	}
	let Some(expr) = version_expr else {
		return Ok(None);
	};
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let v = expr.evaluate(eval_ctx).await?;
	let stamp = v
		.cast_to::<crate::val::Datetime>()
		.map_err(|e| anyhow::anyhow!("{e}"))?
		.to_version_stamp(ctx.txn().version_impl().as_ref())?;
	Ok(Some(stamp))
}

/// Resolve a batch of [`RecordId`]s into output values, applying each
/// record's table-level SELECT permission.
///
/// Records whose table permission denies them are skipped, so neither the
/// existence of the record nor its contents leak to a caller without view
/// access. Compiled permissions are cached in `perm_cache` keyed by table
/// name so that a single graph/reference scan over many edges only resolves
/// each table's permission once.
///
/// When `fetch_full` is `false` and `check_perms` is `false`, this avoids
/// fetching records and just wraps each id as `Value::RecordId`. Any other
/// combination requires reading the record so the permission predicate (if
/// any) can be evaluated against the actual data.
///
/// SECURITY: when `fetch_full` is `true` the materialised record must go
/// through the *same* field-level processing as an ordinary table scan
/// ([`super::pipeline::filter_and_process_batch`]) and [`fetch_record`]
/// ([`crate::exec::operators::fetch::process_fetched_record`]): read-time
/// `COMPUTED` fields are always evaluated, and field-level SELECT
/// permissions are applied whenever `check_perms` is set. Resolving only the
/// table-level permission here would let a graph or reference traversal that
/// yields full edge/record objects expose field-restricted data to callers
/// whose field permissions should hide it, and drop computed fields —
/// diverging from a direct `SELECT *` on the same table.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_record_batch(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	fetch_full: bool,
	check_perms: bool,
	version: Option<u64>,
	cache_policy: CachePolicy,
	perm_cache: &mut std::collections::HashMap<
		surrealdb_strand::TableName,
		crate::exec::permission::PhysicalPermission,
	>,
) -> Result<Vec<Value>, ControlFlow> {
	use crate::exec::permission::{PhysicalPermission, check_permission_for_value};

	if !check_perms && !fetch_full {
		// Fast path: no permissions to check and no data to fetch. Wrap each
		// id as `Value::RecordId` directly.
		return Ok(rids.iter().map(|rid| Value::RecordId(rid.clone())).collect());
	}

	// Compile the SELECT permission for each distinct table referenced in
	// this batch. The cache survives across batches so we only resolve a
	// given table's permission once per scan.
	if check_perms {
		for rid in rids {
			if perm_cache.contains_key(&rid.table) {
				continue;
			}
			let perm = crate::exec::vm::descriptor::select_permission(ctx, &rid.table).await?;
			perm_cache.insert(rid.table.clone(), perm);
		}
	}

	let records =
		crate::exec::migrate::get_records(ctx, txn, ns_id, db_id, rids, version, cache_policy)
			.await?;
	ensure_below_memory_threshold()?;

	// Per-table field state (computed fields + field-level SELECT
	// permissions), built lazily and reused across the records of this batch.
	// `build_field_state` is itself cached per `(table, check_perms)` on the
	// database context, so the cross-batch cost is just a lookup + clone; the
	// local map avoids even that for the common single-table batch.
	let mut field_state_cache: std::collections::HashMap<TableName, FieldState> =
		std::collections::HashMap::new();
	// Computed-field record dereferences must reuse the ambient
	// `skip_fetch_perms` so that, when this runs inside a permission predicate,
	// they don't recurse back into permission checks on cyclic links — matching
	// `fetch_record` (false) vs `fetch_record_no_perms` (true). `check_perms`
	// is always false in the `skip_fetch_perms` case (see `should_check_perms`).
	let skip_fetch_perms = ctx.root().skip_fetch_perms;

	let mut values = Vec::with_capacity(rids.len());
	for (rid, record) in rids.iter().zip(records) {
		// Missing records cannot disclose information.
		if record.data.is_none() {
			continue;
		}

		if check_perms {
			let perm = perm_cache.get(&rid.table).map_or(&PhysicalPermission::Deny, |p| p);
			let allowed = check_permission_for_value(perm, &record.data, None, ctx)
				.await
				.context("Failed to check permission")?;
			if !allowed {
				continue;
			}
		}

		if fetch_full {
			let mut value = match Arc::try_unwrap(record) {
				Ok(rec) => rec.data,
				Err(arc) => arc.data.clone(),
			};

			// Apply the same field-level processing as ordinary table scans
			// and `fetch_record`: evaluate read-time computed fields, then
			// (when permissions are enforced) cut field-restricted values.
			// Without this, a graph/reference traversal yielding full
			// records would bypass field-level SELECT permissions and omit
			// computed fields. See the SECURITY note on this function.
			if !field_state_cache.contains_key(&rid.table) {
				let fs = build_field_state(ctx, &rid.table, check_perms, None).await?;
				field_state_cache.insert(rid.table.clone(), fs);
			}
			let field_state = &field_state_cache[&rid.table];
			materialise_fields_with_permissions(
				ctx,
				field_state,
				&mut value,
				skip_fetch_perms,
				check_perms,
			)
			.await?;

			values.push(value);
		} else {
			values.push(Value::RecordId(rid.clone()));
		}
	}
	if fetch_full {
		// Computed fields can make the materialised rows far wider than the
		// records fetched above.
		ensure_below_memory_threshold()?;
	}
	Ok(values)
}

#[cfg(test)]
/// Whether a scan may decode only its [`DecodeFields`] of each record rather
/// than the whole record.
///
/// Holds when nothing evaluated against a row reads a stored field the
/// statement does not name: the table defines no computed field, whose body
/// may read any field, and, when permissions are checked, its SELECT
/// permission is `FULL` and no field carries a SELECT permission, both of
/// which are evaluated against the stored record. `table_state` must be the
/// table's whole field state, not one filtered to a projection, so that
/// computed fields and permissions evaluated above the scan count too.
pub(crate) fn projected_decode_applies(
	select_permission: &PhysicalPermission,
	check_perms: bool,
	table_state: &FieldState,
) -> bool {
	table_state.computed_fields.is_empty()
		&& (!check_perms
			|| (matches!(select_permission, PhysicalPermission::Allow)
				&& table_state.field_permissions.is_empty()))
}

#[cfg(test)]
mod tests {
	use super::*;

	const MAX_ROWS: usize = 1000;

	fn bytes(rows: &[Value]) -> usize {
		rows.iter().map(approx_value_size).sum()
	}

	#[test]
	fn a_record_id_counts_its_key() {
		let key = RecordIdKey::Array(vec![Value::from("x".repeat(4096))].into());
		let rid = Value::RecordId(RecordId {
			table: "t".into(),
			key,
		});
		assert!(approx_value_size(&rid) > 4096, "a wide compound key is sized by its contents");
	}

	fn row_with_floats(n: usize) -> Value {
		Value::from(vec![Value::from(0.5f64); n])
	}

	#[test]
	fn projected_decode_needs_a_full_select_permission_only_when_checked() {
		let state = FieldState::empty();
		assert!(projected_decode_applies(&PhysicalPermission::Allow, true, &state));
		assert!(projected_decode_applies(&PhysicalPermission::Deny, false, &state));
		assert!(!projected_decode_applies(&PhysicalPermission::Deny, true, &state));
	}

	/// A read-ahead holding one resolved record per value, keyed `t:0`, `t:1`, ….
	fn read_ahead_of(
		permission: &PhysicalPermission,
		budget: usize,
		values: Vec<Value>,
	) -> ReadAhead {
		let mut read_ahead = ReadAhead::new(permission.clone(), false, None, budget);
		for (i, value) in values.into_iter().enumerate() {
			let rid = RecordId {
				table: "t".into(),
				key: RecordIdKey::Number(i as i64),
			};
			read_ahead
				.records
				.push_back((rid, ReadAheadRecord::Resolved(Arc::new(Record::new(value)))));
		}
		read_ahead
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn a_batch_decodes_one_record_whatever_the_budget() -> anyhow::Result<()> {
		let ctx = crate::exec::operators::test_util::root_ctx();
		let mut read_ahead =
			read_ahead_of(&PhysicalPermission::Allow, 0, vec![Value::from(1), Value::from(2)]);
		for expected in [1, 2] {
			let values =
				read_ahead.decode_batch(&ctx).await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
			assert_eq!(values, vec![Value::from(expected)]);
		}
		assert!(read_ahead.is_empty());
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn a_batch_stops_at_the_budget_and_the_row_cap() -> anyhow::Result<()> {
		let ctx = crate::exec::operators::test_util::root_ctx();
		let row = row_with_floats(8);
		let rows = vec![row.clone(); 4];

		// The record that brings the batch to the budget ends it.
		let mut read_ahead =
			read_ahead_of(&PhysicalPermission::Allow, approx_value_size(&row) + 1, rows.clone());
		let values = read_ahead.decode_batch(&ctx).await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
		assert_eq!(values.len(), 2);

		// Under an ample budget, the row cap ends it.
		let mut read_ahead = read_ahead_of(&PhysicalPermission::Allow, usize::MAX, rows);
		read_ahead.row_cap = 3;
		let values = read_ahead.decode_batch(&ctx).await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
		assert_eq!(values.len(), 3);
		Ok(())
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn a_record_with_no_data_is_skipped_uncounted() -> anyhow::Result<()> {
		let ctx = crate::exec::operators::test_util::root_ctx();
		let mut read_ahead = read_ahead_of(
			&PhysicalPermission::Allow,
			usize::MAX,
			vec![Value::None, Value::from(1), Value::None, Value::from(2)],
		);
		read_ahead.row_cap = 2;
		let values = read_ahead.decode_batch(&ctx).await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
		assert_eq!(values, vec![Value::from(1), Value::from(2)]);
		assert!(read_ahead.is_empty());
		Ok(())
	}

	#[test]
	fn an_empty_previous_batch_keeps_the_row_cap() {
		assert_eq!(next_batch_len(0, 0, MAX_ROWS, 1 << 20, MAX_ROWS), MAX_ROWS);
	}

	#[test]
	fn narrow_rows_keep_the_row_cap() {
		let rows = vec![Value::from("narrow"); 10];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 600, 8 << 20, MAX_ROWS), MAX_ROWS);
	}

	#[test]
	fn the_probe_follows_the_budget() {
		assert_eq!(probe_batch_len(2 << 20, 32), 32);
		assert_eq!(probe_batch_len(256 << 10, 32), 4);
		assert_eq!(probe_batch_len(8 << 20, 32), 32);
		assert_eq!(probe_batch_len(1, 32), 1);
	}

	#[test]
	fn a_batch_at_most_doubles() {
		let rows = vec![Value::from("narrow"); 10];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 32, 8 << 20, MAX_ROWS), 64);
	}

	#[test]
	fn wide_rows_shrink_the_batch_to_the_budget() {
		let rows = vec![row_with_floats(1024); 4];
		let per_row = approx_value_size(&rows[0]);
		assert_eq!(per_row, std::mem::size_of::<Value>() * 1025);
		assert_eq!(
			next_batch_len(bytes(&rows), rows.len(), MAX_ROWS, 8 << 20, MAX_ROWS),
			(8 << 20) / per_row
		);
	}

	#[test]
	fn a_row_wider_than_the_budget_still_takes_one_row() {
		let rows = vec![row_with_floats(1024)];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 32, 1, MAX_ROWS), 1);
	}

	#[test]
	fn read_bytes_larger_than_the_decoded_rows_size_the_read() {
		// Ten narrow projected rows read from 100 KiB of stored records.
		assert_eq!(next_batch_len(100 << 10, 10, MAX_ROWS, 1 << 20, MAX_ROWS), 102);
	}

	#[test]
	fn an_empty_batch_keeps_the_current_length() {
		assert_eq!(next_batch_len(0, 0, 31, 2 << 20, MAX_ROWS), 31);
	}

	#[test]
	fn a_geometry_counts_its_containers() {
		let empty = || geo_types::Polygon::new(geo_types::LineString::<f64>::new(vec![]), vec![]);
		let members: Vec<crate::val::Geometry> =
			(0..5000).map(|_| crate::val::Geometry::Polygon(empty())).collect();
		let collection = Value::Geometry(crate::val::Geometry::Collection(members));
		assert!(
			approx_value_size(&collection) >= 5000 * std::mem::size_of::<crate::val::Geometry>(),
			"a collection of 5000 empty polygons is sized by its members"
		);
		let rings: Vec<geo_types::LineString<f64>> =
			(0..5000).map(|_| geo_types::LineString::new(vec![])).collect();
		let polygon = Value::Geometry(crate::val::Geometry::Polygon(geo_types::Polygon::new(
			geo_types::LineString::new(vec![]),
			rings,
		)));
		assert!(
			approx_value_size(&polygon) >= 5000 * std::mem::size_of::<geo_types::LineString<f64>>(),
			"a polygon of 5000 empty interior rings is sized by its rings"
		);
	}

	#[test]
	fn a_geometry_counts_its_coordinates() {
		let ring: Vec<(f64, f64)> = (0..5000).map(|i| (i as f64, 0.0)).collect();
		let polygon = Value::Geometry(crate::val::Geometry::Polygon(geo_types::Polygon::new(
			ring.into(),
			vec![],
		)));
		assert!(
			approx_value_size(&polygon) >= 5000 * std::mem::size_of::<geo_types::Coord<f64>>(),
			"a 5000-point polygon is sized by its coordinates"
		);
	}

	#[test]
	fn decode_fields_normalizes_root_keys_and_identifies_id_only() {
		let empty = DecodeFields::new(std::iter::empty::<String>());
		assert!(empty.is_id_only());
		assert_eq!(empty.listed(), (vec![], false));

		let id_only = DecodeFields::new(["id".to_owned()]);
		assert!(id_only.is_id_only());
		assert_eq!(id_only.listed(), (vec![], false));

		let listed = DecodeFields::new(["b", "id", "a", "b"].map(str::to_owned));
		assert!(!listed.is_id_only());
		assert_eq!(listed.listed(), (vec!["a".to_owned(), "b".to_owned()], false));

		assert_eq!(DecodeFields::excluding(["id".to_owned()]), None);
		let excluded =
			DecodeFields::excluding(["z", "id", "m", "z"].map(str::to_owned)).expect("non-empty");
		assert!(!excluded.is_id_only());
		assert_eq!(excluded.listed(), (vec!["m".to_owned(), "z".to_owned()], true));
	}
}
