//! The pending-state guards of a vector index, whatever its kind.
//!
//! An index whose writes leave pending updates for compaction to fold keeps,
//! per shard of its pending range, a guard that reads `NonEmpty` whenever the
//! shard may hold an entry, and a summary that reads `NonEmpty` whenever any
//! guard may. A lookup reads the summary, then the guards, and scans only the
//! shards they leave armed. The protocol that keeps them truthful — the
//! covering of a guard before the writer's commit, the writer leases on a
//! store other datastores write to, the guard step that returns a drained
//! shard to `Empty` — lives on [`crate::tx::Transaction`], and is the same for
//! every kind. What differs is only where each kind keeps its guards, summary,
//! leases and pending entries: [`GuardedIndex`] names an index together with
//! its [`PendingGuardFamily`] and builds those keys.

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_expr::val::RecordIdKey;
use surrealdb_kvs::key::{KVKey, KVKeyDecode, KVSubspace, RawRange, TypedRange};
use uuid::Uuid;

#[cfg(diskann)]
use crate::key::schema::{
	DiskannPendingKey, DiskannPendingPrefix, DiskannPendingSummaryKey,
	DiskannRecordPendingShardShardPrefix, DiskannWriterLeaseKey, DiskannWriterLeaseShardPrefix,
};
use crate::key::schema::{
	HnswPendingKey, HnswPendingPrefix, HnswPendingSummaryKey, HnswRecordPendingShardShardPrefix,
	HnswWriterLeaseKey, HnswWriterLeaseShardPrefix, IdxRoot,
};
use crate::values::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnWriterLease,
};
use crate::values::index_delta::BufferedIndex;

/// How many shards an index's pending range is divided into, each under a
/// guard of its own. Every guard bit set (`u64`) has a bit per shard.
pub const PENDING_GUARD_SHARDS: u16 = DISKANN_PENDING_STATE_SHARDS;

/// The shard of `id`'s pending entry, and so of the guard its writer covers.
///
/// Two ids the pending key spells alike share a key, so they must share a
/// shard: an integer id is its own value modulo the shard count, and any other
/// id is hashed by its value, whose hash agrees with `==` — which compares
/// numbers by value, as the key's `IndexFormat` spelling does.
///
/// A record's pending entry is found under the shard its writer computed, so
/// the mapping is persisted state: every release that wrote DiskANN's `!dw`
/// computed it with `std`'s `DefaultHasher`, which `rustc` may change. The hash
/// is [`ZeroKeySip13`], the algorithm that hasher runs, kept here so a
/// toolchain cannot move a record to another shard.
pub fn pending_guard_shard(id: &RecordIdKey) -> u16 {
	use std::hash::{Hash, Hasher};
	if let RecordIdKey::Number(id) = id {
		return id.rem_euclid(i64::from(PENDING_GUARD_SHARDS)) as u16;
	}
	let mut hasher = ZeroKeySip13::default();
	id.hash(&mut hasher);
	(hasher.finish() % u64::from(PENDING_GUARD_SHARDS)) as u16
}

/// SipHash-1-3 keyed with zeros: what `std::hash::DefaultHasher::new()`
/// computes, for every write sequence, on the toolchains every release that
/// sharded a pending range was built with.
#[derive(Clone, Copy, Debug)]
pub struct ZeroKeySip13 {
	v0: u64,
	v1: u64,
	v2: u64,
	v3: u64,
	/// Bytes written that do not yet fill a word, in their low bytes.
	tail: u64,
	/// How many bytes `tail` holds.
	ntail: usize,
	/// Every byte written.
	length: usize,
}

impl Default for ZeroKeySip13 {
	fn default() -> Self {
		Self {
			v0: 0x736f_6d65_7073_6575,
			v1: 0x646f_7261_6e64_6f6d,
			v2: 0x6c79_6765_6e65_7261,
			v3: 0x7465_6462_7974_6573,
			tail: 0,
			ntail: 0,
			length: 0,
		}
	}
}

impl ZeroKeySip13 {
	#[inline]
	fn round(&mut self) {
		self.v0 = self.v0.wrapping_add(self.v1);
		self.v1 = self.v1.rotate_left(13);
		self.v1 ^= self.v0;
		self.v0 = self.v0.rotate_left(32);
		self.v2 = self.v2.wrapping_add(self.v3);
		self.v3 = self.v3.rotate_left(16);
		self.v3 ^= self.v2;
		self.v0 = self.v0.wrapping_add(self.v3);
		self.v3 = self.v3.rotate_left(21);
		self.v3 ^= self.v0;
		self.v2 = self.v2.wrapping_add(self.v1);
		self.v1 = self.v1.rotate_left(17);
		self.v1 ^= self.v2;
		self.v2 = self.v2.rotate_left(32);
	}

	/// Compresses one little-endian message word, with one round.
	#[inline]
	fn compress(&mut self, m: u64) {
		self.v3 ^= m;
		self.round();
		self.v0 ^= m;
	}
}

impl std::hash::Hasher for ZeroKeySip13 {
	fn write(&mut self, bytes: &[u8]) {
		self.length += bytes.len();
		for &byte in bytes {
			self.tail |= u64::from(byte) << (8 * self.ntail);
			self.ntail += 1;
			if self.ntail == 8 {
				self.compress(self.tail);
				self.tail = 0;
				self.ntail = 0;
			}
		}
	}

	fn finish(&self) -> u64 {
		let mut state = *self;
		state.compress(((self.length as u64 & 0xff) << 56) | self.tail);
		state.v2 ^= 0xff;
		state.round();
		state.round();
		state.round();
		state.v0 ^ state.v1 ^ state.v2 ^ state.v3
	}
}

/// The kind of index whose pending-state guards a guard belongs to, which
/// decides where its guards, summary, writer leases and pending entries are
/// kept.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PendingGuardFamily {
	/// An HNSW index: guards `!hy`, summary `!hY`, leases `!hz`, entries `!hw`.
	Hnsw,
	/// A DiskANN index: guards `!dy`, summary `!dY`, leases `!dz`, entries `!dw`.
	#[cfg(diskann)]
	DiskAnn,
}

/// An index together with the kind of its pending-state guards.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GuardedIndex {
	pub family: PendingGuardFamily,
	pub index: BufferedIndex,
}

impl GuardedIndex {
	pub fn new(
		family: PendingGuardFamily,
		ns: surrealdb_catalog::NamespaceId,
		db: surrealdb_catalog::DatabaseId,
		tb: surrealdb_expr::val::TableName,
		ix: surrealdb_catalog::IndexId,
	) -> Self {
		Self {
			family,
			index: BufferedIndex {
				ns,
				db,
				tb,
				ix,
			},
		}
	}

	/// The guard of `shard`.
	pub fn guard(&self, shard: u16) -> GuardKey<'_> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => GuardKey::Hnsw(HnswPendingKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => GuardKey::DiskAnn(DiskannPendingKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}),
		}
	}

	/// The summary of the index's guards.
	pub fn summary(&self) -> SummaryKey<'_> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => SummaryKey::Hnsw(HnswPendingSummaryKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
			}),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => SummaryKey::DiskAnn(DiskannPendingSummaryKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
			}),
		}
	}

	/// The writer lease node `nid` takes on `shard`.
	pub fn lease(&self, shard: u16, nid: Uuid) -> LeaseKey<'_> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => LeaseKey::Hnsw(HnswWriterLeaseKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
				nid,
			}),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => LeaseKey::DiskAnn(DiskannWriterLeaseKey {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
				nid,
			}),
		}
	}

	/// Every node's writer lease on `shard`.
	pub fn shard_leases(&self, shard: u16) -> Result<TypedRange<DiskAnnWriterLease>> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => HnswWriterLeaseShardPrefix {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}
			.range(),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => DiskannWriterLeaseShardPrefix {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}
			.range(),
		}
	}

	/// The lease whose encoded key, read from [`Self::shard_leases`], is `raw`.
	pub fn decode_lease<'k>(&self, raw: &'k [u8]) -> Result<LeaseKey<'k>> {
		match self.family {
			PendingGuardFamily::Hnsw => Ok(LeaseKey::Hnsw(HnswWriterLeaseKey::decode_key(raw)?)),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => Ok(LeaseKey::DiskAnn(DiskannWriterLeaseKey::decode_key(raw)?)),
		}
	}

	/// Every guard of the index.
	pub fn guards(&self) -> Result<TypedRange<DiskAnnPendingState>> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => {
				HnswPendingPrefix::new(i.ns, i.db, Cow::Borrowed(&i.tb), i.ix).range()
			}
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => {
				DiskannPendingPrefix::new(i.ns, i.db, Cow::Borrowed(&i.tb), i.ix).range()
			}
		}
	}

	/// The shard of the guard whose encoded key, read from [`Self::guards`], is
	/// `raw`.
	pub fn decode_guard_shard(&self, raw: &[u8]) -> Result<u16> {
		match self.family {
			PendingGuardFamily::Hnsw => Ok(HnswPendingKey::decode_key(raw)?.shard),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => Ok(DiskannPendingKey::decode_key(raw)?.shard),
		}
	}

	/// The pending entries of `shard`, which its guard covers.
	pub fn shard_entries(&self, shard: u16) -> Result<RawRange> {
		let i = &self.index;
		match self.family {
			PendingGuardFamily::Hnsw => Ok(HnswRecordPendingShardShardPrefix {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}
			.range()?
			.into()),
			#[cfg(diskann)]
			PendingGuardFamily::DiskAnn => Ok(DiskannRecordPendingShardShardPrefix {
				ns: i.ns,
				db: i.db,
				tb: Cow::Borrowed(&i.tb),
				ix: i.ix,
				shard,
			}
			.range()?
			.into()),
		}
	}

	/// Everything the index keeps, which bounds a probe of its pending entries.
	pub fn root(&self) -> Result<RawRange> {
		let i = &self.index;
		IdxRoot {
			ns: i.ns,
			db: i.db,
			tb: Cow::Borrowed(&i.tb),
			ix: i.ix,
		}
		.raw_range()
	}

	/// Names the index's guard fence: the start of its guard range, which is
	/// distinct per index and orders indexes the same way for every caller.
	pub fn fence_id(&self) -> Result<Vec<u8>> {
		Ok(self.guards()?.start().to_vec())
	}
}

/// The guard of one shard of an index, of whichever family.
#[derive(Clone, Debug, PartialEq)]
pub enum GuardKey<'a> {
	Hnsw(HnswPendingKey<'a>),
	#[cfg(diskann)]
	DiskAnn(DiskannPendingKey<'a>),
}

impl GuardKey<'_> {
	/// The shard this guard covers.
	pub fn shard(&self) -> u16 {
		match self {
			Self::Hnsw(k) => k.shard,
			#[cfg(diskann)]
			Self::DiskAnn(k) => k.shard,
		}
	}
}

impl KVKey for GuardKey<'_> {
	type Value = DiskAnnPendingState;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(k) => k.encode_buffer(buffer),
			#[cfg(diskann)]
			Self::DiskAnn(k) => k.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

/// The summary of an index's guards, of whichever family.
#[derive(Clone, Debug, PartialEq)]
pub enum SummaryKey<'a> {
	Hnsw(HnswPendingSummaryKey<'a>),
	#[cfg(diskann)]
	DiskAnn(DiskannPendingSummaryKey<'a>),
}

impl KVKey for SummaryKey<'_> {
	type Value = DiskAnnPendingState;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(k) => k.encode_buffer(buffer),
			#[cfg(diskann)]
			Self::DiskAnn(k) => k.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

/// A node's writer lease on one shard of an index, of whichever family.
#[derive(Clone, Debug, PartialEq)]
pub enum LeaseKey<'a> {
	Hnsw(HnswWriterLeaseKey<'a>),
	#[cfg(diskann)]
	DiskAnn(DiskannWriterLeaseKey<'a>),
}

impl LeaseKey<'_> {
	/// The node holding the lease.
	pub fn nid(&self) -> Uuid {
		match self {
			Self::Hnsw(k) => k.nid,
			#[cfg(diskann)]
			Self::DiskAnn(k) => k.nid,
		}
	}
}

impl KVKey for LeaseKey<'_> {
	type Value = DiskAnnWriterLease;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Hnsw(k) => k.encode_buffer(buffer),
			#[cfg(diskann)]
			Self::DiskAnn(k) => k.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

#[cfg(test)]
mod tests {
	use std::hash::{DefaultHasher, Hash, Hasher};

	use super::{ZeroKeySip13, pending_guard_shard};
	use crate::val::{Array, Number, Object, RecordIdKey, Value};

	/// The shard function keeps the mapping every release that sharded a
	/// pending range computed with `DefaultHasher`: the two hashers agree on
	/// every id shape and on raw writes of every length around a word.
	#[test]
	fn the_shard_hash_is_the_default_hasher() {
		let ids = [
			RecordIdKey::String("a".into()),
			RecordIdKey::String("a longer string id that spans several words".into()),
			RecordIdKey::Uuid(uuid::Uuid::from_u128(0x0123_4567_89ab_cdef).into()),
			RecordIdKey::Array(Array::from(vec![
				Value::Number(Number::Int(1)),
				Value::from("b"),
				Value::Number(Number::Float(2.5)),
			])),
			RecordIdKey::Object(Object::from_iter([("k".to_string(), Value::from(7i64))])),
		];
		for id in &ids {
			let mut std = DefaultHasher::new();
			let mut ours = ZeroKeySip13::default();
			id.hash(&mut std);
			id.hash(&mut ours);
			assert_eq!(ours.finish(), std.finish(), "{id:?}");
		}
		for len in 0..40usize {
			let bytes: Vec<u8> = (0..len as u8).collect();
			let (mut std, mut ours) = (DefaultHasher::new(), ZeroKeySip13::default());
			// Split across writes, as a hash of several fields writes it.
			let (head, rest) = bytes.split_at(len / 3);
			for part in [head, rest] {
				std.write(part);
				ours.write(part);
			}
			std.write_u32(0xdead_beef);
			ours.write_u32(0xdead_beef);
			assert_eq!(ours.finish(), std.finish(), "{len} bytes");
		}
		assert_eq!(pending_guard_shard(&RecordIdKey::Number(-1)), 31);
	}
}
