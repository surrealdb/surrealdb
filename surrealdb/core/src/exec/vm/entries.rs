//! The read of a B-tree or `UNIQUE` index's entries, as a program runs it.
//!
//! A program's index source names the index, the access its predicate
//! admits and the direction to read in. Opening it applies the bounds kernel
//! (`btree.bounds`) to the access: a pure computation of the half-open key
//! range the access covers. Each step then reads the next batch of that
//! range through [`Services::read_range`], a read of the keyspace that knows
//! nothing of what the keys hold, and applies the entry kernel
//! (`btree.decode_entry`) to each stored value to find the record it names.
//!
//! No index-specific cursor is involved: the range is the program's, the read
//! is the storage seam's, and the computations are the kernels the index
//! write path shares (`idx::kernels::btree`), so a scan reads exactly the
//! entries a write stores for the values it admits.

use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::btree::INDEX_BATCH_SIZE;
use crate::exec::vm::services::Services;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::kernels::btree::{IndexKeyspace, decode_record_id};
use crate::key::RawRange;
use crate::kvs::Direction;
use crate::val::RecordId;

/// The key range B-tree index `index` holds the entries of `access` in,
/// from the bounds kernel. Fails when `access` is not one a B-tree index
/// answers.
pub(super) fn entry_range(
	ctx: &ExecutionContext,
	index: &IndexRef,
	access: &BTreeAccess,
) -> FlowResult<RawRange> {
	let db = ctx.database().context("the VM requires a database context")?;
	let ks = IndexKeyspace::of(db.ns_ctx.ns.namespace_id, db.db.database_id, index.definition());
	let unique = index.is_unique();
	let range = match access {
		BTreeAccess::Equality(value) => ks.equality(unique, value),
		BTreeAccess::Range {
			range,
		} => ks.range(unique, range.start.as_ref(), range.end.as_ref()),
		BTreeAccess::Compound {
			prefix,
			range,
		} => ks.compound(prefix, range.as_ref().map(|(op, value)| (op, value))),
		BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => {
			return Err(ControlFlow::Err(anyhow::anyhow!("an index source with no B-tree access")));
		}
	};
	Ok(range.map_err(ControlFlow::Err)?.into())
}

/// The fewest entries a batch asks for, so that a range whose entries name
/// few distinct records — repeats of a record that holds an array — is not
/// read a handful of entries at a time.
const MIN_ENTRY_READ: u32 = 32;

/// The entries of one index range not yet read, in key order along a
/// direction.
pub(super) struct EntryScan {
	range: RawRange,
	direction: Direction,
}

impl EntryScan {
	/// The entries of B-tree index `index` under `access`, read along
	/// `direction`.
	pub(super) fn open(
		ctx: &ExecutionContext,
		index: &IndexRef,
		access: &BTreeAccess,
		direction: Direction,
	) -> FlowResult<Self> {
		Ok(Self {
			range: entry_range(ctx, index, access)?,
			direction,
		})
	}

	/// The record ids the next batch of entries names; empty once the range
	/// is read.
	///
	/// A batch asks for about `wanted` entries — the records the reader takes
	/// next, so the scan reads no further ahead than its records are read —
	/// and no fewer than [`MIN_ENTRY_READ`], no more than
	/// [`INDEX_BATCH_SIZE`] or the fuel left ([`super::fuel::batch`]); and
	/// no more than `paged`, the rows the reading pipeline's own paging still
	/// takes, though never fewer than [`MIN_ENTRY_READ`] for it: a scan under
	/// a `LIMIT` reads at most that many entries past the last row the limit
	/// admits, and a filter that rejects most entries still reads them in
	/// batches rather than one at a time. It spends a unit per entry read, so entries that name no
	/// record the scan keeps — repeats of one record, or entries whose record is gone — are
	/// paid for as they are read.
	pub(super) async fn next_batch(
		&mut self,
		ctx: &ExecutionContext,
		services: &impl Services,
		wanted: usize,
		paged: usize,
	) -> FlowResult<Vec<RecordId>> {
		let wanted = u32::try_from(wanted).unwrap_or(u32::MAX);
		let paged = u32::try_from(paged).unwrap_or(u32::MAX).max(MIN_ENTRY_READ);
		let most = super::fuel::batch(wanted.clamp(MIN_ENTRY_READ, INDEX_BATCH_SIZE).min(paged));
		let entries = services.read_range(&mut self.range, self.direction, most).await?;
		super::fuel::tick(ctx, super::fuel::units(entries.len())).await?;
		entries.iter().map(|(_, value)| decode_record_id(value).map_err(ControlFlow::Err)).collect()
	}
}

/// The number of entries B-tree index `index` holds under `access`, over the
/// range the bounds kernel encodes, with no entry value read.
///
/// The first batch of keys is read, asking for no more than the fuel left
/// allows: a range that answers fewer keys than were asked for is counted by
/// it, at the cost of one keyspace read. A range that fills the batch has what
/// remains of it counted by the store in place (`count_keys`), in parts the
/// fuel bounds, so a large count copies no key out of storage.
pub(super) async fn count(
	ctx: &ExecutionContext,
	services: &impl Services,
	index: &IndexRef,
	access: &BTreeAccess,
) -> FlowResult<usize> {
	let mut range = entry_range(ctx, index, access)?;
	let most = super::fuel::batch(INDEX_BATCH_SIZE);
	let first = services.read_range_keys(&mut range, Direction::Forward, most).await?.len();
	super::fuel::tick(ctx, super::fuel::units(first)).await?;
	if first < most as usize {
		return Ok(first);
	}
	Ok(first + services.count_keys(range, None).await?)
}
