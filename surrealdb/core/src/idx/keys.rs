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
use crate::key::database::all::DatabaseRoot;
use crate::key::index::{IndexPrefix, IndexPrefixUnterminated};
use crate::key::{KVRange, KeyRange};
use crate::val::Value;

/// Compute the half-open key range `[start, end)` scanned for a non-unique
/// (`Idx`) index over the value bounds `from`..`to`.
///
/// Each bound maps to the index prefix bracketing all entries for that value:
/// an inclusive lower bound starts at the first such entry, an exclusive one
/// steps past the last with `next_neighbour_expect`, and an unbounded one
/// widens to the index-wide prefix. Because the KV layer excludes `end`, every
/// upper bound is shifted one neighbour further than the matching lower bound:
/// an *inclusive* upper bound encodes the neighbour after its value's last
/// entry, an *exclusive* one the value's own prefix, and an *unbounded* one the
/// neighbour after the index-wide prefix so the whole index is covered.
pub(crate) fn compute_index_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Bound<&Value>,
	to: Bound<&Value>,
) -> Result<KeyRange<'static>> {
	let prefix = DatabaseRoot {
		ns,
		db,
	};

	let start = match from {
		Bound::Included(x) => IndexPrefixUnterminated {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(x)),
		}
		.encode_bound()?,
		Bound::Excluded(x) => IndexPrefixUnterminated {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(x)),
		}
		.encode_bound()?
		.next_neighbour_expect(),
		Bound::Unbounded => IndexPrefix {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.encode_bound()?,
	};

	let end = match to {
		Bound::Included(x) => IndexPrefixUnterminated {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(x)),
		}
		.encode_bound()?
		.next_neighbour_expect(),
		Bound::Excluded(x) => IndexPrefixUnterminated {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(x)),
		}
		.encode_bound()?,
		Bound::Unbounded => IndexPrefix {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.encode_bound()?
		.next_neighbour_expect(),
	};
	Ok((start..end).into())
}
