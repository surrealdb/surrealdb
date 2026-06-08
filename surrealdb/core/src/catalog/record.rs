//! Record module for SurrealDB
//!
//! This module provides the `Record` type which represents a database record with metadata.
//! Records can contain both data and metadata about the record type (e.g., whether it's an edge).

use std::sync::Arc;

use anyhow::Result;
use revision::revisioned;
use surrealdb_strand::Strand;

use crate::catalog::aggregation::AggregationStat;
use crate::kvs::KVValue;
use crate::val::{RecordId, Value};

/// Represents a record stored in the database
///
/// A `Record` contains both the actual data and optional metadata about the record.
/// The metadata can include information such as the record type (e.g., Edge for graph edges).
///
/// # Examples
///
/// ```no_compile
/// use surrealdb_core::catalog::Record;
/// use surrealdb_core::val::{Object, Value};
///
/// // Create a new record with data
/// let record = Record::new(Value::Object(Object::default()));
///
/// // Check if it's an edge record
/// assert!(!record.is_edge());
/// ```
/// `Record` serialises through the standard `#[revisioned]` derive — no
/// custom wire surgery. The `data` field carries the record's fields,
/// including the top-level `"id"` when present.
///
/// `id` redundancy with the storage key: a record's canonical id also
/// lives in its storage key (a `RecordId`). New writes store `id` in the
/// value too; [`Record::kv_decode_value`] only synthesises it from the
/// key for *legacy* data written before the id was stored (see the
/// `entry().or_insert_with(..)` splice there).
///
/// The pre-decode filter consumes the rev-2 wire layout via the
/// macro-emitted accessor `Record::walk_revisioned(...)?.into_data_bytes()?`,
/// which returns the `data` field's bytes as `Cow<'_, [u8]>` in O(1)
/// (offset-table slice) without decoding the field — see
/// [`crate::val::object_extract::extract_field_from_record_bytes`].
#[revisioned(revision(1), revision(2, optimised, indexed_struct))]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Record {
	/// Optional metadata about the record (e.g., record type)
	pub(crate) metadata: Option<Metadata>,
	/// The actual data stored in the record
	// TODO (DB-655): Switch to `Object`.
	pub(crate) data: Value,
}

/// Strand value for the `"id"` field name. Used by the post-decode id
/// normaliser so we don't allocate a fresh `Strand` per record.
const ID_KEY: Strand = Strand::new_static("id");

impl KVValue for Record {
	/// The storage key carries the canonical `RecordId`. The decoder uses
	/// it to set `data`'s `"id"` field — the storage key is the single
	/// source of truth for a record's identity.
	type KeyContext = RecordId;

	/// Encode a `Record` for storage via the derived `revisioned`
	/// serialiser. The top-level `"id"` (if present) is stored as-is.
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(revision::to_vec(self)?)
	}

	/// Decode a `Record` and force its `data`'s top-level `"id"` to the
	/// canonical id reconstructed from the storage key.
	///
	/// The storage key — not the stored bytes — is the source of truth
	/// for a record's identity, so we **overwrite** any `"id"` carried in
	/// the payload rather than preserving it. This matters for records
	/// whose stored `"id"` differs from their key: a materialised view
	/// such as `DEFINE TABLE v AS SELECT id, .. FROM src` projects the
	/// *source* row's id into the view record's data, but the view
	/// record's identity is its own key (`v:..`), which is what callers
	/// (and the pre-decode filter, which derives `id` from the key) must
	/// see. Overwriting also fixes up legacy rows written before the id
	/// was stored inline.
	fn kv_decode_value(bytes: &[u8], rid: RecordId) -> Result<Record> {
		let mut record: Record = revision::from_slice(bytes)?;
		if let Value::Object(obj) = &mut record.data {
			obj.0.insert(ID_KEY, Value::RecordId(rid));
		}
		Ok(record)
	}
}

impl Record {
	/// Creates a new record with the given data and no metadata
	pub(crate) fn new(data: Value) -> Self {
		Self {
			metadata: None,
			data,
		}
	}

	/// Checks if this record represents an edge in a graph
	pub const fn is_edge(&self) -> bool {
		matches!(
			&self.metadata,
			Some(Metadata {
				record_type: RecordType::Edge { .. },
				..
			})
		)
	}

	/// Returns the adjacency-key format variant of this edge record, if
	/// any. Returns `None` for non-edge records. Callers that need to
	/// preserve an existing edge's variant on UPDATE (rather than
	/// downgrading it to a different write-time default) should source
	/// the variant from this helper on the prior document.
	pub const fn edge_variant(&self) -> Option<u16> {
		match &self.metadata {
			Some(Metadata {
				record_type: RecordType::Edge {
					variant,
				},
				..
			}) => Some(*variant),
			_ => None,
		}
	}

	/// Wraps this record in an `Arc` for shared ownership.
	pub(crate) fn into_read_only(self) -> Arc<Self> {
		Arc::new(self)
	}

	/// Sets the record type in the metadata
	pub(crate) fn set_record_type(&mut self, rtype: RecordType) {
		match &mut self.metadata {
			Some(metadata) => {
				metadata.record_type = rtype;
			}
			metadata => {
				*metadata = Some(Metadata {
					record_type: rtype,
					aggregation_stats: Vec::new(),
				});
			}
		}
	}
}

/// The adjacency-key layout generation used when writing brand-new edges.
///
/// Existing edges keep whatever variant they were originally stamped with
/// so their on-disk keys stay valid — only first-write picks this value.
/// Bump when the on-disk adjacency-key layout for new edges advances; add
/// a matching dispatch arm in `doc/edges.rs::store_edges_data` (write) and
/// `doc/purge.rs::purge_pointers` (delete) at the same time.
pub(crate) const LATEST_EDGE_VARIANT: u16 = 2;

/// Types of records that can be stored in the database
///
/// This enum defines the different types of records that can be stored.
/// Edge records carry a `variant` discriminator so format-aware paths
/// (adjacency-key writes, edge purge) know which on-disk layout the
/// record was created against and can dispatch accordingly.
#[revisioned(revision = 2)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub(crate) enum RecordType {
	/// Represents a normal table record
	/// From 3.0.0
	#[default]
	Table,
	/// Revision-1 unit form of `Edge`. Records persisted before the
	/// variant marker existed deserialize through this variant and are
	/// upgraded to `Edge { variant: 1 }`, which corresponds to the
	/// original adjacency-key layout (no embedded target vertex).
	/// From v3.0.0 to 3.1.0
	#[revision(end = 2, convert_fn = "upgrade_edge_v1", fields_name = "EdgeV1")]
	Edge,
	/// Represents an edge in a graph. `variant` identifies which
	/// adjacency-key layout was used when the edge was written, so the
	/// purge path knows exactly which keys to delete without probing
	/// multiple formats. New edges get the current format generation;
	/// older edges keep whatever variant they were written with.
	/// From v3.1.0
	#[revision(start = 2)]
	Edge {
		variant: u16,
	},
}

impl RecordType {
	/// Legacy unit `Edge` records predate the variant marker and were
	/// always written with the original adjacency-key layout. Stamp
	/// them as variant 1 on decode so format-aware code paths can treat
	/// them uniformly with explicitly-stamped records going forward.
	fn upgrade_edge_v1(_fields: EdgeV1, _revision: u16) -> Result<Self, revision::Error> {
		Ok(Self::Edge {
			variant: 1,
		})
	}
}

/// Metadata associated with a record
///
/// This struct contains optional metadata about a record, such as its type and
/// aggregation statistics for materialized view records.
/// The metadata is revisioned to ensure compatibility across different versions
/// of the database.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Metadata {
	/// The type of the record (e.g., Edge for graph edges)
	pub(crate) record_type: RecordType,
	/// Statistics related to running aggregations for this record.
	/// These do not directly correspond to a field but must be used in conjunction with the table
	/// definition to calculate the final value for this record.
	pub(crate) aggregation_stats: Vec<AggregationStat>,
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Array, Object, RecordIdKey, TableName};

	fn make_rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table),
			key: RecordIdKey::String(Strand::new(key)),
		}
	}

	#[test]
	fn legacy_unit_edge_decodes_to_variant_one() {
		// Records persisted before the variant marker existed wrote
		// `RecordType::Edge` as a rev-1 unit variant. The rev-1 → rev-2
		// upgrade path must map them to `Edge { variant: 1 }` so the
		// purge layer (which dispatches on the variant) treats them as
		// the original adjacency-key layout.
		use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};

		#[revisioned(revision = 1)]
		#[derive(Debug, PartialEq)]
		enum LegacyRecordType {
			Table,
			Edge,
		}

		let mut bytes = Vec::new();
		LegacyRecordType::Edge.serialize_revisioned(&mut bytes).unwrap();

		let decoded =
			<RecordType as DeserializeRevisioned>::deserialize_revisioned(&mut bytes.as_slice())
				.unwrap();
		assert_eq!(
			decoded,
			RecordType::Edge {
				variant: 1,
			}
		);
	}

	#[test]
	fn current_edge_round_trips_through_metadata() {
		// A `Metadata` containing `Edge { variant: N }` must round-trip
		// through the revisioned encoder/decoder without losing the
		// variant marker. This guards against accidental regressions
		// where Metadata's encoding inadvertently strips struct-variant
		// fields on the RecordType field.
		use revision::{DeserializeRevisioned, SerializeRevisioned};

		let original = Metadata {
			record_type: RecordType::Edge {
				variant: 7,
			},
			aggregation_stats: Vec::new(),
		};
		let mut bytes = Vec::new();
		original.serialize_revisioned(&mut bytes).unwrap();
		let decoded =
			<Metadata as DeserializeRevisioned>::deserialize_revisioned(&mut bytes.as_slice())
				.unwrap();
		assert_eq!(decoded, original);
	}

	#[test]
	fn bool_record_encoded_size_is_stable() {
		// Pin the encoded byte length for a minimal `Record { data: Bool(true) }`
		// so accidental changes to the revisioned wire format (e.g. extra
		// metadata bytes, a Bool encoding change) trip a test. Previously
		// covered by the generic `test_serialize_deserialize` rstest, which
		// dropped its Record case when the `KeyContext = ()` bound was added.
		//
		// Wire breakdown under rev-2 `optimised, indexed_struct`:
		//   1 byte  u16 rev=2 (varint)
		//   4 bytes u32_le payload_length = 12
		//  12 bytes payload:
		//      8 bytes prologue (`u32_le metadata_off=8, u32_le data_off=9`)
		//      1 byte  metadata = Option::None tag
		//      3 bytes data = Value::Bool(true) (rev=2 || tag=0x22 || 0x01)
		//  ─────────
		//  17 bytes total.
		assert_eq!(Record::new(Value::Bool(true)).kv_encode_value().unwrap().len(), 17);
	}

	/// Build the on-disk bytes a *legacy* writer would have produced: a
	/// `Record` whose stored `Object` has had its top-level `"id"`
	/// removed (the pre-3.2 encoder stripped it). Used to exercise the
	/// `kv_decode_value` splice path.
	fn legacy_stripped_bytes(obj_without_id: Object) -> Vec<u8> {
		revision::to_vec(&Record::new(Value::Object(obj_without_id))).unwrap()
	}

	#[test]
	fn encode_decode_round_trips_with_id() {
		// Current writers store `id` inline. Encode → decode must return
		// the record unchanged, with `id` present at the top level.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let original = Record::new(Value::Object(obj));

		let bytes = original.kv_encode_value().unwrap();
		let decoded = Record::kv_decode_value(&bytes, rid.clone()).unwrap();
		assert_eq!(decoded, original);
		match &decoded.data {
			Value::Object(o) => assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid))),
			_ => panic!("expected Value::Object"),
		}
	}

	#[test]
	fn encode_stores_id_inline() {
		// The encoder no longer strips `id`: the encoded bytes must be
		// byte-identical to the plain derived `revision::to_vec`.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let record = Record::new(Value::Object(obj));

		assert_eq!(record.kv_encode_value().unwrap(), revision::to_vec(&record).unwrap());
	}

	#[test]
	fn decode_splices_id_for_legacy_stripped_data() {
		// Legacy data lacks the top-level `id`. Decode must synthesise it
		// from the storage key at the correct sorted position. Three
		// bracket cases: id in the middle, at the end, at the front.
		let rid = make_rid("user", "alice");

		// "age" < "id" < "name" → id lands in the middle.
		let mut middle = Object::default();
		middle.0.insert(Strand::new("age"), Value::from(30i64));
		middle.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let decoded = Record::kv_decode_value(&legacy_stripped_bytes(middle), rid.clone()).unwrap();
		match &decoded.data {
			Value::Object(o) => {
				let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
				assert_eq!(keys, vec!["age", "id", "name"]);
				assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid.clone())));
			}
			_ => panic!("expected Value::Object"),
		}

		// All keys sort before "id" → appended at the end.
		let mut tail = Object::default();
		tail.0.insert(Strand::new("address"), Value::String(Strand::new("123 main")));
		tail.0.insert(Strand::new("age"), Value::from(30i64));
		let decoded = Record::kv_decode_value(&legacy_stripped_bytes(tail), rid.clone()).unwrap();
		match &decoded.data {
			Value::Object(o) => {
				let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
				assert_eq!(keys, vec!["address", "age", "id"]);
			}
			_ => panic!("expected Value::Object"),
		}

		// All keys sort after "id" → prepended at the front.
		let mut head = Object::default();
		head.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		head.0.insert(Strand::new("zip"), Value::from(12345i64));
		let decoded = Record::kv_decode_value(&legacy_stripped_bytes(head), rid).unwrap();
		match &decoded.data {
			Value::Object(o) => {
				let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
				assert_eq!(keys, vec!["id", "name", "zip"]);
			}
			_ => panic!("expected Value::Object"),
		}
	}

	#[test]
	fn decode_round_trips_matching_id() {
		// When the stored `id` matches the storage key (the normal case),
		// decode returns the record unchanged.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		let record = Record::new(Value::Object(obj));

		let bytes = revision::to_vec(&record).unwrap();
		let decoded = Record::kv_decode_value(&bytes, rid).unwrap();
		assert_eq!(decoded, record);
	}

	#[test]
	fn decode_overwrites_id_with_storage_key() {
		// The storage key is the source of truth for identity. When the
		// stored `id` differs from the key — as for a materialised view
		// `DEFINE TABLE v AS SELECT id, .. FROM src`, which projects the
		// *source* row's id into the view record stored under `v:..` — the
		// decoded `id` must be the key, not the stored (foreign) id.
		let view_key = make_rid("high_scores", "2");
		let source_id = make_rid("src", "2");
		let mut obj = Object::default();
		// Stored data carries the projected source id, as the view writer
		// persists it.
		obj.0.insert(Strand::new("id"), Value::RecordId(source_id));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("b")));
		obj.0.insert(Strand::new("score"), Value::from(20i64));
		let stored = Record::new(Value::Object(obj));

		let bytes = revision::to_vec(&stored).unwrap();
		let decoded = Record::kv_decode_value(&bytes, view_key.clone()).unwrap();
		match &decoded.data {
			Value::Object(o) => {
				assert_eq!(o.0.get("id"), Some(&Value::RecordId(view_key)));
			}
			_ => panic!("expected Value::Object"),
		}
	}

	#[test]
	fn nested_id_is_preserved() {
		// Nested objects with their own `id` field must round-trip intact;
		// the top-level splice only touches the outermost object.
		let rid = make_rid("user", "alice");

		let mut profile = Object::default();
		profile.0.insert(Strand::new("id"), Value::String(Strand::new("profile:7")));
		profile.0.insert(Strand::new("bio"), Value::String(Strand::new("hello")));

		let mut friend1 = Object::default();
		friend1.0.insert(Strand::new("id"), Value::String(Strand::new("user:bob")));
		let mut friend2 = Object::default();
		friend2.0.insert(Strand::new("id"), Value::String(Strand::new("user:carol")));

		let friends = Array(vec![Value::Object(friend1.clone()), Value::Object(friend2.clone())]);

		let mut top = Object::default();
		top.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		top.0.insert(Strand::new("profile"), Value::Object(profile.clone()));
		top.0.insert(Strand::new("friends"), Value::Array(friends));

		let record = Record::new(Value::Object(top));

		let bytes = record.kv_encode_value().unwrap();
		let decoded = Record::kv_decode_value(&bytes, rid.clone()).unwrap();

		match &decoded.data {
			Value::Object(o) => {
				assert_eq!(o.0.get("id"), Some(&Value::RecordId(rid)));
				match o.0.get("profile") {
					Some(Value::Object(p)) => {
						assert_eq!(p.0.get("id"), Some(&Value::String(Strand::new("profile:7"))));
					}
					_ => panic!("expected nested profile object"),
				}
				match o.0.get("friends") {
					Some(Value::Array(arr)) => {
						let v: Vec<_> = arr.iter().collect();
						match v[0] {
							Value::Object(f) => {
								assert_eq!(
									f.0.get("id"),
									Some(&Value::String(Strand::new("user:bob")))
								);
							}
							_ => panic!("expected nested friend object"),
						}
						match v[1] {
							Value::Object(f) => {
								assert_eq!(
									f.0.get("id"),
									Some(&Value::String(Strand::new("user:carol")))
								);
							}
							_ => panic!("expected nested friend object"),
						}
					}
					_ => panic!("expected friends array"),
				}
			}
			_ => panic!("expected Value::Object"),
		}
	}

	#[test]
	fn encode_round_trips_with_divergent_key_orderings() {
		// Keys `"name"` (varint length 4) and `"zip"` (varint length 3)
		// have opposite K::Ord and wire-byte orderings: K::Ord puts
		// `"name"` before `"zip"`, while the rev-2 indexed-map body sorts
		// by wire bytes (length-prefixed), putting `"zip"` first. The
		// deserialiser re-sorts by K::Ord on read, so the round-trip
		// preserves the record's logical content and key order.
		let rid = make_rid("user", "alice");
		let mut obj = Object::default();
		obj.0.insert(Strand::new("id"), Value::RecordId(rid.clone()));
		obj.0.insert(Strand::new("name"), Value::String(Strand::new("Alice")));
		obj.0.insert(Strand::new("zip"), Value::from(12345i64));
		let original = Record::new(Value::Object(obj));

		let bytes = original.kv_encode_value().unwrap();
		let decoded = Record::kv_decode_value(&bytes, rid).unwrap();
		assert_eq!(decoded, original);
		match decoded.data {
			Value::Object(o) => {
				let keys: Vec<&str> = o.0.iter().map(|(k, _)| k.as_str()).collect();
				assert_eq!(keys, vec!["id", "name", "zip"]);
			}
			_ => panic!("expected Value::Object"),
		}
	}
}
