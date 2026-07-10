//! This crate defines the key structure for the key value store.
//!
//! Key struct naming conventions:
//! `XxKey` - A specific key type. eg. `/*{ns}*{db}*{tb_name}*{id}`
//! `XxRoot` - A root key which prefixes other keys. eg. `/*{ns}*{db}`
//! `XxRange` - Represents a start and end key. eg. `/*{ns}*{db}#\x00` or
//! `/*{ns}*{db}#\xff`
//!
//!
//!
//! Terminology:
//! - `/`: Root identifier
//! - `*`: Path separator
//! - `!`: Catalog identifier
//!
//! - {ns}: NamespaceId
//! - {db}: DatabaseId
//! - {ns_name}: &str
//! - {db_name}: &str
//!
//! crate::key::version                  !v -> Version
//!
//! crate::key::root::all                /
//! crate::key::root::ac                 /!ac{ac}
//! crate::key::root::nd                 /!nd{nd}
//! crate::key::root::ni                 /!ni
//! crate::key::root::ns                 /!ns{ns} -> NamespaceDefinition
//! crate::key::root::us                 /!us{us}
//! crate::key::root::tl                 /!tl{tl}
//! crate::key::root::cg                 /!cg{ty}
//!
//! crate::key::node::all                /${nd}
//! crate::key::node::lq                 /${nd}!lq{lq}{ns}{db}
//!
//! crate::key::root::access::all        /&{ac}
//! crate::key::root::access::gr         /&{ac}!gr{gr}
//!
//! crate::key::namespace::all           /*{ns}
//! crate::key::namespace::ac            /*{ns}!ac{ac}
//! crate::key::namespace::db            /*{ns}!db{db_name} -> DatabaseDefinition
//! crate::key::namespace::di            /+{ns}!di
//! crate::key::namespace::lg            /*{ns}!lg{lg}
//! crate::key::namespace::us            /*{ns}!us{us}
//!
//! crate::key::namespace::access::all   /*{ns}&{ac}
//! crate::key::namespace::access::gr    /*{ns}&{ac}!gr{gr}
//!
//! crate::key::database::all            /*{ns}*{db}
//! crate::key::database::ac             /*{ns}*{db}!ac{ac_name}
//! crate::key::database::az             /*{ns}*{db}!az{az_name}
//! crate::key::database::bu             /*{ns}*{db}!bu{bu_name}
//! crate::key::database::fc             /*{ns}*{db}!fn{fc_name}
//! crate::key::database::md             /*{ns}*{db}!md{md_name} -> ModuleDefinition
//! crate::key::database::ml             /*{ns}*{db}!ml{ml_name}{vn}
//! crate::key::database::pa             /*{ns}*{db}!pa{pa_name}
//! crate::key::database::sq             /*{ns}*{db}!sq{sq_name}
//! crate::key::database::tb             /*{ns}*{db}!tb{tb_name} -> TableDefinition
//! crate::key::database::ti             /+{ns}*{db}!ti
//! crate::key::database::ts             /*{ns}*{db}!ts{ts}
//! crate::key::database::us             /*{ns}*{db}!us{us_name}
//! crate::key::database::vs             /*{ns}*{db}!vs
//! crate::key::database::cg             /*{ns}*{db}!cg{ty}
//!
//! crate::key::database::access::all    /*{ns}*{db}&{ac}
//! crate::key::database::access::gr     /*{ns}*{db}&{ac}!gr{gr}
//!
//! crate::key::table::all               /*{ns}*{db}*{tb_name}
//! crate::key::table::ev                /*{ns}*{db}*{tb_name}!ev{ev}
//! crate::key::table::fd                /*{ns}*{db}*{tb_name}!fd{fd}
//! crate::key::table::ft                /*{ns}*{db}*{tb_name}!ft{ft}
//! crate::key::table::ix                /*{ns}*{db}*{tb_name}!il{ix} -> ix_name
//! crate::key::table::ix                /*{ns}*{db}*{tb_name}!ix{ix_name} -> IndexDefinition
//! crate::key::table::lq                /*{ns}*{db}*{tb_name}!lq{lq}
//!
//! crate::key::index::all               /*{ns}*{db}*{tb_name}+{ix}
//! crate::key::index::bc                /*{ns}*{db}*{tb_name}+{ix}!bc{id}
//! crate::key::index::bd                /*{ns}*{db}*{tb_name}+{ix}!bd{id}
//! crate::key::index::bf                /*{ns}*{db}*{tb_name}+{ix}!bf{id}
//! crate::key::index::bi                /*{ns}*{db}*{tb_name}+{ix}!bi{id}
//! crate::key::index::bk                /*{ns}*{db}*{tb_name}+{ix}!bk{id}
//! crate::key::index::bl                /*{ns}*{db}*{tb_name}+{ix}!bl{id}
//! crate::key::index::bo                /*{ns}*{db}*{tb_name}+{ix}!bo{id}
//! crate::key::index::bp                /*{ns}*{db}*{tb_name}+{ix}!bp{id}
//! crate::key::index::bs                /*{ns}*{db}*{tb_name}+{ix}!bs
//! crate::key::index::bt                /*{ns}*{db}*{tb_name}+{ix}!bt{id}
//! crate::key::index::bu                /*{ns}*{db}*{tb_name}+{ix}!bu{id}
//! crate::key::index::dl                /*{ns}*{db}*{tb_name}+{ix}!dl{id}
//! crate::key::index::tf                /*{ns}*{db}*{tb_name}+{ix}!tf{term}{id}
//! crate::key::index                    /*{ns}*{db}*{tb_name}+{ix}*{fd}{id}
//!
//! crate::key::change::vs_key_prefix    /*{ns}*{db}#
//! crate::key::change::vs_key_suffix                *{tb_name}\00
//! crate::key::change::prefix           /*{ns}*{db}#
//! crate::key::change::prefix_ts        /*{ns}*{db}#{ts}
//! crate::key::change::suffix           /*{ns}*{db}#\ff
//! crate::key::change::cf               /*{ns}*{db}#{ts}*{tb_name}
//! crate::key::change::vs               /*{ns}*{db}#{ts}/*{ns}/*/{db}!vs*{tb_name}\0
//! crate::key::change::suffix_vs        /*{ns}*{db}#{ts}/*{ns}/*/{db}!vs
//!
//! crate::key::lqe                      /*{ns}*{db}%{ts}*{tb_name}
//!
//! crate::key::record                   /*{ns}*{db}*{tb_name}*{id}
//!
//! crate::key::graph                    /*{ns}*{db}*{tb_name}~{id}{eg}{ft}{fk}
//! crate::key::ref                      /*{ns}*{db}*{tb_name}&{id}{ft}{ff}{fk}
//!
//! crate::key::sequence::st             /*{ns}*{db}*{tb_name}*{sq}!st{id}
//! crate::key::sequence::ba             /*{ns}*{db}*{tb_name}*{sq}!ba{start}

use std::fmt::Debug;

use anyhow::{Context, Result};
use roaring::{RoaringBitmap, RoaringTreemap};

pub(crate) mod category;
pub(crate) mod change;
pub(crate) mod database;
pub(crate) mod graph;
pub(crate) mod index;
pub(crate) mod lqe;
pub(crate) mod mac;
pub(crate) mod namespace;
pub(crate) mod node;
pub(crate) mod record;
pub(crate) mod r#ref;
pub(crate) mod root;
pub(crate) mod sequence;
pub(crate) mod table;
pub(crate) mod version;

pub(crate) use mac::{impl_kv_key_storekey, impl_kv_range_storekey, impl_kv_value_revisioned, key};
// Needs to be public for the enterprise crate.
pub use surrealdb_kvs::{Key, KeyRange};

/// KVKey is a trait that defines a key for the key-value store.
pub(crate) trait KVKey: Debug + Sized {
	/// The associated value type for this key.
	type Value: KVValue;

	/// Encodes the key into a byte vector.
	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()>;

	/// Encodes the key into a byte vector.
	fn encode_key(&self) -> Result<Key<'static>> {
		let mut res = Vec::new();
		self.encode_buffer(&mut res)?;
		Ok(Key::from(res))
	}

	/// Returns the context the value decoder needs to reconstruct fields
	/// derived from the key. For most key types this is `()`; for
	/// `RecordKey` it is the `RecordId` used to inject the canonical `id`
	/// during record decode. Encode never needs the key (the value is
	/// self-describing on encode), only decode does.
	fn value_context(&self) -> <Self::Value as KVValue>::KeyContext;
}

pub(crate) trait KVKeyDecode<'a>: Sized {
	fn decode_key(bytes: &'a [u8]) -> Result<Self>;
}

pub(crate) trait KVRange {
	/// Encodes the key into a byte vector.
	fn encode_bound(&self) -> Result<Key<'static>>;

	/// Encodes the key into a byte vector.
	fn encode_range(&self) -> Result<KeyRange<'static>> {
		Ok(self.encode_bound()?.prefix_expect())
	}
}

/// KVValue is a trait that defines a value for the key-value store.
///
/// `KeyContext` is the data the value decoder needs from the storage key
/// to reconstruct fields that aren't stored in the value bytes. For most
/// types this is `()`; for `Record` it is `RecordId`, used to splice the
/// canonical `id` back into the decoded object (`Record::kv_encode_value`
/// strips it).
pub(crate) trait KVValue {
	type KeyContext;

	/// Encodes the value into a byte vector.
	fn kv_encode_value(&self) -> Result<Vec<u8>>;

	/// Decodes the value from a byte slice, consuming `ctx` to recover
	/// any fields derived from the storage key (see [`KeyContext`]).
	fn kv_decode_value(bytes: &[u8], ctx: Self::KeyContext) -> Result<Self>
	where
		Self: Sized;
}

impl KVValue for Vec<u8> {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.clone())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(bytes.to_vec())
	}
}

impl KVValue for String {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.as_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		std::str::from_utf8(bytes).context("String bytes must be valid utf8").map(str::to_owned)
	}
}

impl KVValue for u64 {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(self.to_be_bytes().to_vec())
	}

	#[inline]
	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let arr: [u8; 8] =
			bytes.try_into().map_err(|_| anyhow::anyhow!("u64 bytes must be 8 bytes"))?;
		Ok(u64::from_be_bytes(arr))
	}
}

impl KVValue for () {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(Vec::new())
	}

	fn kv_decode_value(_bytes: &[u8], _: ()) -> Result<Self> {
		Ok(())
	}
}

impl KVValue for RoaringBitmap {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(Self::deserialize_from(bytes)?)
	}
}

impl KVValue for RoaringTreemap {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut bytes = Vec::new();
		self.serialize_into(&mut bytes)?;
		Ok(bytes)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		Ok(Self::deserialize_from(bytes)?)
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::u64(123_u64, vec![0, 0, 0, 0, 0, 0, 0, 123])]
	#[case::unit((), Vec::new())]
	#[case::vec(vec![1, 2, 3], vec![1, 2, 3])]
	#[case::string(String::from("test"), b"test".to_vec())]
	#[case::roaring_bitmap(RoaringBitmap::new(), vec![58, 48, 0, 0, 0, 0, 0, 0])]
	#[case::roaring_treemap(RoaringTreemap::new(), vec![0, 0, 0, 0, 0, 0, 0, 0])]
	fn test_kv_value_primitives(#[case] value: impl KVValue, #[case] expected: Vec<u8>) {
		let encoded = value.kv_encode_value().unwrap();
		assert_eq!(encoded, expected);
	}
}
