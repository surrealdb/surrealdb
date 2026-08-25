//! Stores a DEFINE SEQUENCE config definition
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId, SequenceDefinition};
use crate::key::category::{Categorise, Category};
use crate::kvs::{KVKey, impl_kv_key_storekey};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub(crate) struct Sq<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_d: u8,
	_e: u8,
	pub sq: Cow<'a, str>,
}

impl_kv_key_storekey!(Sq<'_> => SequenceDefinition);

pub fn prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"*sq\x00");
	Ok(k)
}

pub fn suffix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"*sq\xff");
	Ok(k)
}

/// The byte position at which a sequence name begins.
///
/// This is also where a *table* name begins: `*` introduces a table, so the
/// definition key for a sequence named `foo` is byte-for-byte the content root
/// of a table named `sqfoo`, and the range `prefix..suffix` therefore spans
/// every key of every table whose name starts with `sq`. [`is_definition`]
/// separates the two.
pub fn name_prefix(ns: NamespaceId, db: DatabaseId) -> Result<Vec<u8>> {
	let mut k = super::all::new(ns, db).encode_key()?;
	k.extend_from_slice(b"*sq");
	Ok(k)
}

/// Whether `key` is a sequence definition rather than a key belonging to a
/// table whose name begins with `sq`.
///
/// A definition is `{name_prefix}{name}` and *ends* at the name. A table
/// sharing the range always carries its own key material after the name:
/// nothing is stored at a bare content root, and a table's definition lives in
/// the `!tb` band, which this range does not reach. Ending exactly at the name
/// is therefore the whole test.
///
/// Callers scanning `prefix..suffix` must apply this to each key before
/// decoding the value, since a table's row will not decode as a
/// [`SequenceDefinition`] and would otherwise fail the entire scan.
pub fn is_definition(key: &[u8], name_prefix: &[u8]) -> bool {
	let Some(rest) = key.strip_prefix(name_prefix) else {
		return false;
	};
	name_end(rest) == Some(rest.len())
}

/// The least key sorting above everything held by the table that `key` belongs
/// to, or `None` if `key` is not a table's key.
///
/// A colliding table's content root is `{name_prefix}{name}` — byte-identical
/// to the definition key of the sequence named `{name}` — and every key the
/// table stores extends it. A scan that meets one of those keys can therefore
/// seek past the whole table: the definition sorts first and has already been
/// seen, and nothing further inside the subtree can be one.
///
/// The bound is the root with its trailing `storekey` terminator raised to
/// `0x01`: every key the table holds extends the root past that terminator, so
/// all of them sort below the bound, while the next definition sorts above it.
pub fn skip_table_subtree(key: &[u8], name_prefix: &[u8]) -> Option<Vec<u8>> {
	let rest = key.strip_prefix(name_prefix)?;
	let end = name_end(rest)?;
	if end == rest.len() {
		// A definition, which is the table root itself and holds nothing.
		return None;
	}
	let mut bound = key[..name_prefix.len() + end].to_vec();
	let last = bound.last_mut()?;
	// `name_end` stops at nothing but the terminator, so this always holds. A
	// byte that was not the terminator would raise to a bound sorting *below*
	// the key it came from, so the scan declines to skip rather than seek
	// backwards.
	if *last != 0x00 {
		return None;
	}
	*last = 0x01;
	Some(bound)
}

/// The offset just past the `storekey`-encoded name at the start of `rest`.
///
/// `storekey` escapes `0x00` and `0x01` by prefixing `0x01`, so the byte after
/// an escape belongs to the name whatever it is, and the first unescaped `0x00`
/// is the terminator.
fn name_end(rest: &[u8]) -> Option<usize> {
	let mut i = 0;
	while i < rest.len() {
		match rest[i] {
			0x01 => i += 2,
			0x00 => return Some(i + 1),
			_ => i += 1,
		}
	}
	None
}

impl Categorise for Sq<'_> {
	fn categorise(&self) -> Category {
		Category::DatabaseSequence
	}
}

impl<'a> Sq<'a> {
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, sq: &'a str) -> Self {
		Self {
			__: b'/', // /
			_a: b'*', // *
			ns,
			_b: b'*', // *
			db,
			_c: b'*', // *
			_d: b's', // s
			_e: b'q', // q
			sq: Cow::Borrowed(sq),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pre() -> Vec<u8> {
		name_prefix(NamespaceId(1), DatabaseId(2)).unwrap()
	}

	fn definition(name: &str) -> Vec<u8> {
		Sq::encode_key(&Sq::new(NamespaceId(1), DatabaseId(2), name)).unwrap()
	}

	#[test]
	fn key() {
		let val = Sq::new(NamespaceId(1), DatabaseId(2), "test");
		let enc = Sq::encode_key(&val).unwrap();
		assert_eq!(enc, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sqtest\0");
	}

	#[test]
	fn prefix() {
		let val = super::prefix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sq\0");
	}

	#[test]
	fn suffix() {
		let val = super::suffix(NamespaceId(1), DatabaseId(2)).unwrap();
		assert_eq!(val, b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sq\xff");
	}

	#[test]
	fn name_prefix_is_where_the_name_starts() {
		assert_eq!(pre(), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sq");
	}

	/// Every definition the encoder produces is recognised as one, including
	/// names carrying the bytes the surrounding key structure uses.
	#[test]
	fn a_definition_key_is_recognised() {
		let pre = pre();
		for name in ["", "a", "foo", "sqfoo", "a\0b", "a\x01b", "*sq", "!tb"] {
			assert!(
				is_definition(&definition(name), &pre),
				"the definition for {name:?} was not recognised"
			);
		}
	}

	/// A table whose name begins with `sq` shares this range. Its content root
	/// is byte-for-byte the definition key of the sequence named after the rest
	/// of the table name, and every key it actually stores extends that root —
	/// which is what marks those keys out.
	#[test]
	fn a_table_subtree_key_is_not_a_definition() {
		let pre = pre();
		let root = definition("foo");
		for tail in [b"*".as_slice(), b"\0", b"*id\0", b"!fdname\0", b"\x01\x01"] {
			let mut key = root.clone();
			key.extend_from_slice(tail);
			assert!(
				!is_definition(&key, &pre),
				"a key of table `sqfoo` was taken for a definition: {key:?}"
			);
		}
	}

	/// A name whose encoding contains an escaped terminator must not be cut
	/// short at that byte, which would read one sequence's key as another's
	/// followed by trailing material.
	#[test]
	fn an_escaped_name_is_not_truncated() {
		let pre = pre();
		let embedded = definition("a\0b");
		assert!(is_definition(&embedded, &pre));
		assert_ne!(embedded, definition("a"), "the escape collapsed two distinct names");
	}

	/// A subtree key yields a bound that sorts above every key the table holds
	/// and below the next sequence definition.
	#[test]
	fn a_subtree_key_yields_a_bound_past_the_whole_table() {
		let pre = pre();
		let root = definition("foo");
		let mut inner = root.clone();
		inner.extend_from_slice(b"*id\0");
		let bound = skip_table_subtree(&inner, &pre).expect("a table key yields a bound");
		assert!(bound > inner, "the bound must sort above the key that produced it");
		assert!(bound > root, "the bound must sort above the table root");
		// Every key the table can hold extends the root, so all of them precede
		// the bound.
		for tail in [b"\0".as_slice(), b"*", b"!fd", b"\xff\xff\xff"] {
			let mut key = root.clone();
			key.extend_from_slice(tail);
			assert!(key < bound, "a table key sorted at or above the bound: {key:?}");
		}
		// The next sequence definition is not skipped over.
		assert!(definition("fop") > bound, "the bound swallowed the following definition");
	}

	#[test]
	fn a_definition_yields_no_bound_to_skip() {
		let pre = pre();
		for name in ["", "a", "foo", "a\0b"] {
			assert_eq!(skip_table_subtree(&definition(name), &pre), None, "for {name:?}");
		}
	}

	#[test]
	fn a_key_outside_the_layout_is_not_a_definition() {
		let pre = pre();
		// Another database's keys, which the range never reaches.
		assert!(!is_definition(&name_prefix(NamespaceId(1), DatabaseId(3)).unwrap(), &pre));
		assert!(!is_definition(b"/", &pre));
		// Under the prefix but unterminated: not a whole key of either shape.
		let mut truncated = pre.clone();
		truncated.extend_from_slice(b"foo");
		assert!(!is_definition(&truncated, &pre));
	}
}
