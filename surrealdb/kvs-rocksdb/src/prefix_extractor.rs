//! Custom `SliceTransform` tailored for SurrealDB's key layout.
//!
//! SurrealDB keys follow a structured layout where all "hot path" keys
//! (records, index entries, graph edges, refs, per-table metadata) live under
//! a common, variable-length, table-level prefix:
//!
//! ```text
//! /*{ns:4}*{db:4}*{tb_name}\0<d>...
//! ```
//!
//! where `<d>` is a single-byte discriminator separating each per-table
//! namespace:
//!
//! * `*` — records (`/*{ns}*{db}*{tb}\0*{id}`)
//! * `+` — index entries (`/*{ns}*{db}*{tb}\0+{ix}...`)
//! * `!` — table metadata (events, fields, views, index defs, live queries)
//! * `~` — graph edges
//! * `&` — refs
//!
//! A fixed-length prefix extractor cannot describe this because `ns`/`db`
//! are fixed-width but `tb_name` is variable-width. This module provides a
//! custom extractor that parses through the structure and returns the prefix
//! up to — and including — the discriminator byte. Two keys that belong to
//! the same table AND the same category therefore produce identical prefixes,
//! which lets RocksDB:
//!
//! 1. Build per-SST bloom filters keyed on table+category, so a scan over one table can skip entire
//!    SSTs that belong to other tables.
//! 2. Short-circuit `FindNextUserEntry` at prefix boundaries when paired with
//!    `ReadOptions::set_prefix_same_as_start(true)`.
//!
//! Keys that don't match this layout (root/namespace/database-level metadata,
//! change feed entries, identifier-state keys, node-level keys, etc.) are
//! reported as out-of-domain so that RocksDB bypasses prefix bloom filter
//! lookups for them and falls back to its regular block-index path.
//!
//! See `crate::key` for the authoritative definition of every key layout.

use rocksdb::SliceTransform;

/// Stable identifier persisted into SST metadata. Bumping this name will
/// invalidate existing bloom filters (they will be rebuilt on next
/// compaction), which is desirable when the extractor's semantics change.
pub(super) const NAME: &str = "surrealdb.TablePrefix.v1";

/// Byte offset (inclusive) at which the null-terminated table name starts
/// in a well-formed table-level key: `/`, `*`, `ns:4`, `*`, `db:4`, `*`.
const TB_START: usize = 12;

/// Minimum number of bytes required for a table-level key. This is the
/// smallest possible in-domain key: a zero-length table name encoded as a
/// single `\0` terminator, followed by a single-byte discriminator.
///
/// `TB_START` + 1 (`\0`) + 1 (discriminator) = 14.
const MIN_LEN: usize = TB_START + 2;

/// Parse a SurrealDB key and, if it matches the table-level layout, return
/// the exclusive end offset of its table+category prefix.
///
/// Returns `None` for any key that is not in the table-level domain (root,
/// namespace, database metadata, change feed, identifier state, node, etc.).
fn parse_prefix_end(key: &[u8]) -> Option<usize> {
	// Fast-fail on keys too short to possibly be in our domain. This also
	// guarantees the subsequent indexed accesses (`key[0]`..`key[11]`) are
	// in-bounds.
	if key.len() < MIN_LEN {
		return None;
	}
	// Structural check: every in-domain key is of the form
	// `/`, `*`, ns:4, `*`, db:4, `*`, tb_name..., `\0`, <d>, ...
	// Any mismatch means the key lives in a different sub-tree (namespace
	// metadata, database metadata, change feed, refs-at-root, etc.) and
	// must be treated as out-of-domain.
	if key[0] != b'/' || key[1] != b'*' || key[6] != b'*' || key[11] != b'*' {
		return None;
	}
	// Locate the `\0` terminator of the (possibly empty) table name. The
	// null byte cannot appear earlier than `TB_START` because positions
	// 2..=10 carry the 4-byte `ns`/`db` ids which may legitimately contain
	// zero bytes.
	let null_pos = TB_START + key[TB_START..].iter().position(|&b| b == 0)?;
	// The discriminator byte must exist (and is what we include in the
	// returned prefix). Without it, the key is ambiguous (e.g. a bare
	// `TableRoot` key) and should not be used for prefix bloom lookups.
	if null_pos + 1 >= key.len() {
		return None;
	}
	// Exclusive end offset: everything up to and including the discriminator.
	Some(null_pos + 2)
}

/// Transform a key into its table+category prefix, or return the key
/// unchanged when it falls outside the table-level domain. The RocksDB
/// contract requires `Transform` to always return a sub-slice, even for
/// out-of-domain keys; such keys are filtered out separately by
/// `in_domain` and therefore never participate in prefix bloom filters.
fn transform(key: &[u8]) -> &[u8] {
	match parse_prefix_end(key) {
		Some(end) => &key[..end],
		None => key,
	}
}

/// Report whether this key participates in the prefix bloom filter.
fn in_domain(key: &[u8]) -> bool {
	parse_prefix_end(key).is_some()
}

/// Extract the table+category prefix of a key, if any. Used by the
/// datastore to decide whether a scan range stays within a single prefix
/// (and can therefore safely enable `prefix_same_as_start`).
pub(super) fn extract(key: &[u8]) -> Option<&[u8]> {
	parse_prefix_end(key).map(|end| &key[..end])
}

/// Build a fresh `SliceTransform` instance suitable for
/// `Options::set_prefix_extractor`.
pub(super) fn build() -> SliceTransform {
	SliceTransform::create(NAME, transform, Some(in_domain))
}

#[cfg(test)]
mod tests {
	use super::*;

	// Shorthand for the fixed header `/*{ns:4}*{db:4}*` with
	// ns=1, db=2 — matches the encoded test vectors in `crate::key`.
	const HEADER: &[u8] = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*";

	#[test]
	fn record_key_prefix() {
		// Matches `key::record` test vector: `/*{ns}*{db}*testtb\0*\x03testid\0`.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*\x03testid\0";
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0*");
	}

	#[test]
	fn index_root_prefix() {
		// Matches `key::index::all` test vector: `.../*testtb\0+\0\0\0\x03`.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\x00\x00\x00\x03";
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+");
	}

	#[test]
	fn index_definition_prefix() {
		// Matches `key::table::ix` test vector: `.../*testtb\0!ixtestix\0`.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!ixtestix\0";
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0!");
	}

	#[test]
	fn graph_key_prefix() {
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0~\x03id\0";
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0~");
	}

	#[test]
	fn ref_key_prefix() {
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0&\x03id\0";
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0&");
	}

	#[test]
	fn records_in_same_table_share_prefix() {
		let k1 = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\x03idA\0";
		let k2 = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\x03idBBB\0";
		assert_eq!(transform(k1), transform(k2));
	}

	#[test]
	fn records_in_different_tables_have_different_prefixes() {
		let k1 = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tbA\0*\x03id\0";
		let k2 = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tbB\0*\x03id\0";
		assert_ne!(transform(k1), transform(k2));
	}

	#[test]
	fn records_in_different_namespaces_have_different_prefixes() {
		let k1 = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\x03id\0";
		let k2 = b"/*\x00\x00\x00\x99*\x00\x00\x00\x02*tb\0*\x03id\0";
		assert_ne!(transform(k1), transform(k2));
	}

	#[test]
	fn records_and_indexes_have_different_prefixes() {
		// Same table, different category byte => different prefix so the
		// bloom filter can skip SSTs that contain only the other category.
		let records = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\x03id\0";
		let indexes = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0+\x00\x00\x00\x01";
		assert_ne!(transform(records), transform(indexes));
	}

	#[test]
	fn zero_length_table_name_still_in_domain() {
		// Minimum-length in-domain key: table name is just the `\0` terminator.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\0*";
		assert_eq!(key.len(), MIN_LEN);
		assert!(in_domain(key));
		assert_eq!(transform(key), b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*\0*");
	}

	#[test]
	fn namespace_metadata_is_out_of_domain() {
		// `/!ns{ns}` — root-level namespace definition.
		let key = b"/!ns\x00\x00\x00\x01";
		assert!(!in_domain(key));
		// Transform must still return a valid sub-slice.
		assert_eq!(transform(key), key);
	}

	#[test]
	fn database_metadata_is_out_of_domain() {
		// `/*{ns}*{db}!tb{tb_name}` — position 11 is `!`, not `*`.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02!tbhello\0";
		assert!(!in_domain(key));
	}

	#[test]
	fn database_identifier_state_is_out_of_domain() {
		// `/+{ns}!di` — position 1 is `+`, not `*`.
		let key = b"/+\x00\x00\x00\x01!di";
		assert!(!in_domain(key));
	}

	#[test]
	fn database_sequence_is_out_of_domain() {
		// `/*{ns}*{db}*sq{sq_name}\0` — no discriminator after the null, so
		// we refuse to compute a prefix (bare `TableRoot`-style key).
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*sqtest\0";
		assert!(!in_domain(key));
	}

	#[test]
	fn node_key_is_out_of_domain() {
		// `/${nd}` — position 1 is `$`.
		let key = b"/$\x00\x00\x00\x00\x01";
		assert!(!in_domain(key));
	}

	#[test]
	fn change_feed_is_out_of_domain() {
		// `/*{ns}*{db}#{ts}...` — position 11 is `#`.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02#\x00\x00\x00\x00\x00\x00\x00\x01";
		assert!(!in_domain(key));
	}

	#[test]
	fn table_root_without_discriminator_is_out_of_domain() {
		// `/*{ns}*{db}*{tb_name}\0` — bare table root has no discriminator
		// and therefore cannot be classified into a category.
		let key = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0";
		assert!(!in_domain(key));
	}

	#[test]
	fn too_short_keys_are_out_of_domain() {
		assert!(!in_domain(b""));
		assert!(!in_domain(b"/"));
		assert!(!in_domain(b"/*\x00\x00\x00\x01"));
		// Exactly MIN_LEN - 1 bytes: still rejected.
		assert!(!in_domain(b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*"));
	}

	#[test]
	fn seek_range_start_and_end_share_prefix() {
		// Every record scan uses bounds of the form `.../*\x00`..`.../*\xff`.
		// Both bounds must resolve to the same extracted prefix so that
		// `prefix_same_as_start` can safely be enabled by the datastore.
		let start = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\x00";
		let end = b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*tb\0*\xff";
		assert_eq!(extract(start), extract(end));
	}

	#[test]
	fn extract_returns_none_for_out_of_domain() {
		assert!(extract(b"/!ns\x00\x00\x00\x01").is_none());
	}

	#[test]
	fn header_literal_matches_expected_layout() {
		// Sanity check the test shorthand stays in sync with the real layout.
		assert_eq!(HEADER.len(), TB_START);
		assert_eq!(HEADER[0], b'/');
		assert_eq!(HEADER[1], b'*');
		assert_eq!(HEADER[6], b'*');
		assert_eq!(HEADER[11], b'*');
	}
}
