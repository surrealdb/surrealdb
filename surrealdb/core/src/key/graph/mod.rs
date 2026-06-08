//! Graph adjacency keys.
//!
//! Each [`RELATE`](crate::expr::statements::RelateStatement) writes four KV
//! keys that together model a single relation between an `in` vertex `l`,
//! an edge record `rid`, and an `out` vertex `r`:
//!
//! ```text
//!              ltr (target = r)         pointer
//!          ┌─────────────────────┬─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
//!          │                     ▼                  ▼
//!     ┌────┴─────┐   etl  ┌────────────┐  etr   ┌──────────┐
//!     │   left   │───────▶│ rid (edge) │───────▶│  right   │
//!     └──────────┘   in   └────────────┘  out   └────┬─────┘
//!           ▼                    ▼                   │
//!           └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┴───────────────────┘
//!                  pointer         rtl (source = l)
//! ```
//!
//! Two roles share the same wire format ([`Graph`] / [`GraphWithTarget`])
//! but carry different meaning:
//!
//! - **Pointer keys** (`ltr`, `rtl`) live on the IN / OUT vertex's adjacency. They embed the
//!   opposite endpoint vertex in `(tt, tk)` so a `->edge->vertex` (or `<-edge<-vertex`) range scan
//!   can resolve the far vertex without reading the edge record. Constructed with [`new_pointer`].
//! - **Inner keys** (`etl`, `etr`) live on the edge record's own adjacency. Their `(ft, fk)` slot
//!   already names the endpoint vertex, so they keep the legacy layout with no embedded target.
//!   Constructed with [`new`].
//!
//! Pointer keys are the only ones that benefit from the target embedding;
//! inner keys are unchanged from the pre-target-vertex layout. Pointer-key
//! bytes are a strict prefix of legacy-key bytes (legacy = `Graph`,
//! pointer = `Graph` + `(tt, tk)`), so a single range scan transparently
//! returns both formats and the decoder handles each per-row.
//!
//! Writers (`doc::edges::store_edges_data`) and the cascade delete path
//! (`doc::purge::purge_pointers`) both refer to the keys by these names.

use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, BorrowReader, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::dir::Dir;
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};
use crate::val::{RecordId, RecordIdKey, TableName};

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
}

impl_kv_key_storekey!(Prefix<'_> => Vec<u8>);

impl<'a> Prefix<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, id: &RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id: id.to_owned(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct PrefixEg<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
}

impl_kv_key_storekey!(PrefixEg<'_> => Vec<u8>);

impl<'a> PrefixEg<'a> {
	fn new(ns: NamespaceId, db: DatabaseId, tb: &'a TableName, id: &RecordIdKey, eg: Dir) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id: id.clone(),
			eg,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: Cow<'a, str>,
}

impl_kv_key_storekey!(PrefixFt<'_> => Vec<u8>);

impl<'a> PrefixFt<'a> {
	fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &RecordIdKey,
		eg: Dir,
		ft: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id: id.to_owned(),
			eg,
			ft: Cow::Borrowed(ft),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Graph<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: Cow<'a, TableName>,
	pub fk: Cow<'a, RecordIdKey>,
}

impl_kv_key_storekey!(Graph<'_> => ());

/// Pointer-key wire format: the [`Graph`] layout followed by the target
/// vertex `(tt, tk)`.
///
/// See the module-level docs for the role pointer keys play in a relation
/// and why their bytes are a strict superset of the legacy [`Graph`]
/// encoding. Construct via [`new_pointer`].
#[derive(Clone, Debug, Eq, PartialEq, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct GraphWithTarget<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub id: RecordIdKey,
	pub eg: Dir,
	pub ft: Cow<'a, TableName>,
	pub fk: Cow<'a, RecordIdKey>,
	pub tt: Cow<'a, TableName>,
	pub tk: Cow<'a, RecordIdKey>,
}

impl_kv_key_storekey!(GraphWithTarget<'_> => ());

/// Result of decoding a graph adjacency key.
///
/// `edge` is the record sitting in the key's `(ft, fk)` slot. Its meaning
/// depends on which side of the relation the key came from (see the
/// module-level diagram): for **inner keys** this is the endpoint vertex,
/// for **pointer keys** this is the edge record itself.
///
/// `target` is `Some` only for **pointer keys** written in the new layout
/// — those embed the far endpoint vertex in `(tt, tk)`. It is `None` for
/// inner keys and for any pointer key that pre-dates the new layout.
#[derive(Debug, Clone)]
pub(crate) struct DecodedGraph {
	pub edge: RecordId,
	pub target: Option<RecordId>,
}

impl Graph<'_> {
	/// Decode a graph adjacency key, transparently handling legacy and
	/// pointer layouts.
	///
	/// Forward-compat contract: once the pointer-key tail `(tt, tk)` has
	/// been consumed, any further trailing bytes are ignored. A future
	/// field appended after `(tt, tk)` therefore stays readable by this
	/// decoder, just without the extra information.
	///
	/// Trailing bytes appended directly after a legacy (no-target) key are
	/// **not** tolerated: the decoder eagerly tries to consume them as
	/// `(tt, tk)` and bubbles up whatever decode error storekey produces.
	/// This is intentional — new fields are always layered after the
	/// target tail (preserving the prefix property the range scans rely
	/// on), never appended to the legacy form. A legacy key with garbage
	/// trailing bytes is malformed data, not a forward-compat scenario.
	pub fn decode_key(k: &[u8]) -> Result<DecodedGraph> {
		let mut reader = BorrowReader::new(k);
		let g = <Graph as BorrowDecode>::borrow_decode(&mut reader)?;
		let edge = RecordId {
			table: g.ft.into_owned(),
			key: g.fk.into_owned(),
		};
		let target = if reader.is_empty() {
			None
		} else {
			let tt = <Cow<TableName> as BorrowDecode>::borrow_decode(&mut reader)?;
			let tk = <Cow<RecordIdKey> as BorrowDecode>::borrow_decode(&mut reader)?;
			Some(RecordId {
				table: tt.into_owned(),
				key: tk.into_owned(),
			})
		};
		Ok(DecodedGraph {
			edge,
			target,
		})
	}
}

pub fn new<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	id: &RecordIdKey,
	eg: Dir,
	fk: &'a RecordId,
) -> Graph<'a> {
	Graph::new(ns, db, tb, id.to_owned(), eg, fk)
}

/// Construct a **pointer key** (vertex-side adjacency) embedding the far
/// endpoint of the relation.
///
/// `fk` identifies the edge record; `target` is the vertex reached by
/// traversing through that edge in the given direction. Used by
/// `doc::edges::store_edges_data` to write the two pointer keys of a
/// `RELATE` so that subsequent `->edge->vertex` (and mirror) range scans
/// can resolve the far vertex from the adjacency alone, without reading
/// the edge record. See the module-level docs for the four-key layout.
pub fn new_pointer<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &'a TableName,
	id: &RecordIdKey,
	eg: Dir,
	fk: &'a RecordId,
	target: &'a RecordId,
) -> GraphWithTarget<'a> {
	GraphWithTarget {
		__: b'/',
		_a: b'*',
		ns,
		_b: b'*',
		db,
		_c: b'*',
		tb: Cow::Borrowed(tb),
		_d: b'~',
		id: id.to_owned(),
		eg,
		ft: Cow::Borrowed(&fk.table),
		fk: Cow::Borrowed(&fk.key),
		tt: Cow::Borrowed(&target.table),
		tk: Cow::Borrowed(&target.key),
	}
}

pub fn prefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn egprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
	eg: Dir,
) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn egsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
	eg: Dir,
) -> Result<Vec<u8>> {
	let mut k = PrefixEg::new(ns, db, tb, id, eg).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

pub fn ftprefix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
	eg: Dir,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode_key()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn ftsuffix(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	id: &RecordIdKey,
	eg: Dir,
	ft: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, eg, ft).encode_key()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

impl Categorise for Graph<'_> {
	fn categorise(&self) -> Category {
		Category::Graph
	}
}

impl Categorise for GraphWithTarget<'_> {
	fn categorise(&self) -> Category {
		Category::Graph
	}
}

impl<'a> Graph<'a> {
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: RecordIdKey,
		eg: Dir,
		fk: &'a RecordId,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'~',
			id,
			eg,
			ft: Cow::Borrowed(&fk.table),
			fk: Cow::Borrowed(&fk.key),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;
	use crate::types::PublicValue;

	#[test]
	fn key() {
		let Ok(PublicValue::RecordId(fk)) = syn::value("other:test") else {
			panic!()
		};
		let fk = fk.into();
		let tb: TableName = "testtb".into();
		let val = Graph::new(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			"testid".to_owned().into(),
			Dir::Out,
			&fk,
		);
		let enc = Graph::encode_key(&val).unwrap();
		assert_eq!(
			enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0~\x03testid\0\x03other\0\x03test\0"
		);
		// Legacy keys decode to no target.
		let dec = Graph::decode_key(&enc).unwrap();
		assert_eq!(dec.edge.table.as_str(), "other");
		assert!(dec.target.is_none());
	}

	#[test]
	fn key_with_target() {
		let Ok(PublicValue::RecordId(edge)) = syn::value("likes:abc") else {
			panic!()
		};
		let Ok(PublicValue::RecordId(target)) = syn::value("person:bob") else {
			panic!()
		};
		let edge: RecordId = edge.into();
		let target: RecordId = target.into();
		let tb: TableName = "person".into();
		let id: RecordIdKey = "alice".to_owned().into();
		// Build the new-format key.
		let new_key =
			new_pointer(NamespaceId(1), DatabaseId(2), &tb, &id, Dir::Out, &edge, &target);
		let new_bytes = GraphWithTarget::encode_key(&new_key).unwrap();
		// Build the legacy-format key on the same vertex/edge for comparison.
		let legacy = Graph::new(NamespaceId(1), DatabaseId(2), &tb, id.clone(), Dir::Out, &edge);
		let legacy_bytes = Graph::encode_key(&legacy).unwrap();
		// The legacy encoding must be a strict prefix of the new encoding so
		// the same range scan returns both formats.
		assert!(new_bytes.starts_with(&legacy_bytes));
		assert!(new_bytes.len() > legacy_bytes.len());
		// The byte immediately after the legacy prefix is the first byte of
		// the target table's storekey encoding. The range-bound construction
		// in `exec::operators::scan::graph::eval_graph_bound` and
		// `expr::lookup::ComputedLookupSubject::presuf` appends `0xff` to a
		// legacy key to "skip past every new-format variant of the same fk"
		// without spilling into the next fk's keyspace. That trick only
		// holds while this boundary byte is strictly less than `0xff`; if a
		// future storekey change makes it `0xff`, both call sites need to
		// be revisited.
		let boundary_byte = new_bytes[legacy_bytes.len()];
		assert!(
			boundary_byte < 0xff,
			"first byte after legacy graph-key prefix must be < 0xff but was 0x{:02x}; \
			 the `0xff` sentinel used by graph range bounds is no longer safe",
			boundary_byte,
		);
		// The unified decoder must round-trip the target on the new format.
		let dec = Graph::decode_key(&new_bytes).unwrap();
		assert_eq!(dec.edge.table.as_str(), "likes");
		assert_eq!(dec.edge.key, RecordIdKey::from("abc".to_owned()));
		let tgt = dec.target.expect("new-format key must carry a target");
		assert_eq!(tgt.table.as_str(), "person");
		assert_eq!(tgt.key, RecordIdKey::from("bob".to_owned()));
	}

	#[test]
	fn legacy_key_with_trailing_bytes_errors() {
		// Negative half of the forward-compat contract: trailing bytes on
		// a *legacy* (no-target) key are not tolerated. The decoder
		// eagerly tries to read them as the pointer-key `(tt, tk)` tail
		// and bubbles up the decode error. New fields always land after
		// the target tail, not directly after legacy bytes, so a legacy
		// key with garbage trailing bytes is malformed data and must
		// surface as an error rather than silently decode.
		let Ok(PublicValue::RecordId(fk)) = syn::value("other:test") else {
			panic!()
		};
		let fk: RecordId = fk.into();
		let tb: TableName = "person".into();
		let legacy = Graph::new(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			"alice".to_owned().into(),
			Dir::Out,
			&fk,
		);
		let mut bytes = Graph::encode_key(&legacy).unwrap();
		bytes.extend_from_slice(b"trailing-garbage");
		Graph::decode_key(&bytes)
			.expect_err("trailing bytes on a legacy key must surface as a decode error");
	}

	#[test]
	fn key_with_trailing_bytes_decodes() {
		// Positive half of the forward-compat contract: trailing bytes
		// appended *after* the pointer-key tail are silently ignored, so
		// a future field layered on top of the new format stays readable
		// by the current decoder.
		let Ok(PublicValue::RecordId(edge)) = syn::value("likes:abc") else {
			panic!()
		};
		let Ok(PublicValue::RecordId(target)) = syn::value("person:bob") else {
			panic!()
		};
		let edge: RecordId = edge.into();
		let target: RecordId = target.into();
		let tb: TableName = "person".into();
		let id: RecordIdKey = "alice".to_owned().into();
		let key = new_pointer(NamespaceId(1), DatabaseId(2), &tb, &id, Dir::Out, &edge, &target);
		let mut bytes = GraphWithTarget::encode_key(&key).unwrap();
		// Append arbitrary trailing bytes mimicking a future field.
		bytes.extend_from_slice(b"future-extension");
		let dec = Graph::decode_key(&bytes).expect("trailing bytes must not fail decode");
		assert_eq!(dec.edge.table.as_str(), "likes");
		let tgt = dec.target.expect("target still decoded from prefix");
		assert_eq!(tgt.table.as_str(), "person");
	}
}
