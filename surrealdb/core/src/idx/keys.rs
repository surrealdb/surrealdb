//! Index key-range math.
//!
//! Pure encoding over a catalog [`IndexDefinition`] and a pair of value bounds:
//! no planner, executor or transaction state is involved. It lives beside the
//! index key types rather than in either query planner so both the streaming
//! and legacy iterators can share one definition of what a bound scans.

use std::borrow::Cow;
use std::ops::Bound;
use std::slice;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::idx::entry::IndexEntryValue;
use crate::key::schema::{EntryFdOpenPrefix, EntryPrefix};
use crate::key::{KVRange, KVSubspace, KeyRange, TypedRange};
use crate::val::Value;

/// Claim a range assembled from index-entry bounds for the value those entries
/// store.
///
/// A b-tree access can take its two ends from different bounds — the index-wide
/// prefix, an open prefix over a leading subset of the indexed fields, a unique
/// key — so the range is put together from bounds rather than produced by one of
/// them.  Every key under [`EntryPrefix`] stores an [`IndexEntryValue`], the
/// non-unique and the unique shape alike, so a scan over any range within it
/// decodes to that type.
pub(crate) fn entry_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	range: KeyRange<'static>,
) -> TypedRange<IndexEntryValue> {
	EntryPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&ix.table_name),
		ix: ix.index_id,
	}
	.typed(range)
}

/// Compute the range of entries covered by a non-unique index range scan.
///
/// Each end comes from a bound on the indexed values — the open field prefix,
/// which delimits every entry whose values start with the given one, or the
/// index-wide prefix where that end is unbounded:
/// - **inclusive start** (`>=`): the bound itself, so the scan starts at the first entry for the
///   given value.
/// - **exclusive start** (`>`): past the bound and every entry beneath it.
/// - **inclusive end** (`<=`): past the bound and every entry beneath it, so the entries for the
///   given value are the last included.
/// - **exclusive end** (`<`): the bound itself, so no entry for the given value is included.
pub(crate) fn compute_index_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Bound<&Value>,
	to: Bound<&Value>,
) -> Result<TypedRange<IndexEntryValue>> {
	/// The bound covering every entry whose values start with `x`.
	fn fd_prefix<'a>(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &'a IndexDefinition,
		x: &'a Value,
	) -> EntryFdOpenPrefix<'a> {
		EntryFdOpenPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(x)),
		}
	}

	/// The bound covering every entry in the index.
	fn ix_prefix<'a>(ns: NamespaceId, db: DatabaseId, ix: &'a IndexDefinition) -> EntryPrefix<'a> {
		EntryPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
	}

	let start = match from {
		Bound::Included(x) => fd_prefix(ns, db, ix, x).encode_bound()?,
		Bound::Excluded(x) => fd_prefix(ns, db, ix, x).skip_extensions()?,
		Bound::Unbounded => ix_prefix(ns, db, ix).encode_bound()?,
	};

	let end = match to {
		Bound::Included(x) => fd_prefix(ns, db, ix, x).skip_extensions()?,
		Bound::Excluded(x) => fd_prefix(ns, db, ix, x).encode_bound()?,
		Bound::Unbounded => ix_prefix(ns, db, ix).skip_extensions()?,
	};
	Ok(entry_range(ns, db, ix, (start..end).into()))
}
