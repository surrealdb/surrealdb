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

use std::sync::Arc;

use crate::exec::index::access_path::{BTreeAccess, IndexRef, KeyAccess, SpatialPointFilter};
use crate::exec::index::iterator::btree::INDEX_BATCH_SIZE;
use crate::exec::vm::services::Services;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::entry::IndexEntryValue;
use crate::idx::kernels::btree::{IndexKeyspace, decode_record_id};
use crate::key::RawRange;
use crate::kvs::Direction;
use crate::val::RecordId;

/// The key range B-tree index `index` holds the entries of `access` in,
/// from the bounds kernel. Fails when `access` is not one a B-tree index
/// answers.
///
/// A bound that reads the statement clock resolves here, as the scan opens, to
/// the instant of the statement `ctx` runs in (the wall clock where none is
/// installed), so a program that outlives its statement reads the clock of the
/// statement that runs it.
pub(in crate::exec::vm) fn entry_range(
	ctx: &ExecutionContext,
	index: &IndexRef,
	access: &BTreeAccess,
) -> FlowResult<RawRange> {
	let db = ctx.database().context("the VM requires a database context")?;
	let ks = IndexKeyspace::of(db.ns_ctx.ns.namespace_id, db.db.database_id, index.definition());
	let unique = index.is_unique();
	let resolved = access.resolve(ctx.ctx().statement_clock()).map_err(ControlFlow::Err)?;
	let range = match &resolved {
		Some(KeyAccess::Equality(value)) => ks.equality(unique, value),
		Some(KeyAccess::Range(range)) => ks.range(range.start.as_ref(), range.end.as_ref()),
		Some(KeyAccess::Compound {
			prefix,
			range,
			upper,
		}) => ks.compound_between(
			prefix,
			range.as_ref().map(|(op, value)| (op, value)),
			upper.as_ref().map(|(op, value)| (op, value)),
		),
		None => {
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
	point_filter: Option<Arc<SpatialPointFilter>>,
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
			point_filter: access.point_filter().cloned(),
		})
	}

	/// The record ids the next batch of entries names; empty once the range
	/// is read.
	///
	/// A batch asks for about `wanted` entries, the records the reader takes
	/// next, so the scan never reads further ahead than those records need.
	/// The size is at least [`MIN_ENTRY_READ`], at most [`INDEX_BATCH_SIZE`]
	/// and at most the fuel left ([`super::fuel::batch`]). The initial probe is
	/// also capped by `paged`, the rows the reading pipeline's paging still
	/// takes, but never falls below [`MIN_ENTRY_READ`]: when `point_filter`
	/// rejects every entry in a probe, subsequent probes double up to
	/// [`INDEX_BATCH_SIZE`] so sparse boundary cells do not crawl at `paged`.
	/// Each entry read spends one unit,
	/// including entries that name no kept record (repeats of one record, or a
	/// record that is gone).
	pub(super) async fn next_batch(
		&mut self,
		ctx: &ExecutionContext,
		services: &impl Services,
		wanted: usize,
		paged: usize,
	) -> FlowResult<Vec<RecordId>> {
		let wanted = u32::try_from(wanted).unwrap_or(u32::MAX);
		let paged = u32::try_from(paged).unwrap_or(u32::MAX).max(MIN_ENTRY_READ);
		let mut most =
			super::fuel::batch(wanted.clamp(MIN_ENTRY_READ, INDEX_BATCH_SIZE).min(paged));
		loop {
			let entries = services.read_range(&mut self.range, self.direction, most).await?;
			super::fuel::tick(ctx, super::fuel::units(entries.len())).await?;
			if entries.is_empty() {
				return Ok(Vec::new());
			}
			let Some(pf) = self.point_filter.as_deref() else {
				return entries
					.iter()
					.map(|(_, value)| decode_record_id(value).map_err(ControlFlow::Err))
					.collect();
			};
			let mut out = Vec::with_capacity(entries.len());
			for (_, value) in &entries {
				let (_, point) =
					IndexEntryValue::decode_spatial_trailer(value).map_err(ControlFlow::Err)?;
				let Some((x, y)) = point else {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"Spatial point filter applied to index entry without embedded point coordinates"
					)));
				};
				if !pf.matches(x, y) {
					continue;
				}
				out.push(decode_record_id(value).map_err(ControlFlow::Err)?);
			}
			if !out.is_empty() {
				return Ok(out);
			}
			most = super::fuel::batch(most.saturating_mul(2).min(INDEX_BATCH_SIZE));
		}
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
///
/// With `at_most`, the count stops once it reaches that many entries, reading
/// keys batch by batch and no further than it counts.
pub(super) async fn count(
	ctx: &ExecutionContext,
	services: &impl Services,
	index: &IndexRef,
	access: &BTreeAccess,
	at_most: Option<usize>,
) -> FlowResult<usize> {
	let mut range = entry_range(ctx, index, access)?;
	let Some(at_most) = at_most else {
		let most = super::fuel::batch(INDEX_BATCH_SIZE);
		let first = services.read_range_keys(&mut range, Direction::Forward, most).await?.len();
		super::fuel::tick(ctx, super::fuel::units(first)).await?;
		if first < most as usize {
			return Ok(first);
		}
		return Ok(first + services.count_keys(range, None).await?);
	};
	let mut counted = 0;
	while counted < at_most {
		let left = u32::try_from(at_most - counted).unwrap_or(u32::MAX);
		let most = super::fuel::batch(INDEX_BATCH_SIZE).min(left);
		let read = services.read_range_keys(&mut range, Direction::Forward, most).await?.len();
		super::fuel::tick(ctx, super::fuel::units(read)).await?;
		counted += read;
		if read < most as usize {
			break;
		}
	}
	Ok(counted)
}
