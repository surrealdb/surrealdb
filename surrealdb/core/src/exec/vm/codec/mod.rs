//! The codec the VM reads and writes stored values through.
//!
//! The VM reads and writes stored values itself, so a program never needs the
//! Rust type to exist. A decoded value is a [`Value`]: a struct is an object
//! keyed by field name, an enum an object with one key naming its variant, and
//! each scalar the value of its kind. An option is `NONE` or its value — or,
//! for an option of a type that can itself be `NONE`, a one-element array
//! holding it.
//!
//! A catalog type is coded by its derive. Its [`TypeDesc`] names the type's
//! `CatalogVTable` (a row of the [`Codec`]'s catalog table), whose functions read an
//! entry in either grammar and write one at the feature level of the store
//! being written ([`encode_id_at`]), so the bytes the VM writes are the bytes
//! the Rust type writes, and depend on that level. A catalog value held inside
//! a `revision` container, such as the definition a table change carries, is
//! read and written in the form the container holds it in.
//!
//! Every other revisioned type is described by hand: a descriptor transcribes
//! the type's `#[revisioned]` history, the framing each revision was written
//! in, and each field or variant with the revisions it is alive in. Decoding
//! an older revision reads what that revision wrote and fills what it lacks —
//! a field's default, or a [`Convert`] that rebuilds it from a field the
//! revision had instead. Encoding always writes the type's current revision.
//!
//! The descriptors are checked against the Rust codec on every frozen compat
//! fixture (`kvs/compat`): the VM decodes the fixture and re-encodes it, and
//! the bytes must be the ones the Rust codec writes for the same fixture.
//!
//! A record is the one type stored in two encodings. It is written in the v1
//! record envelope (`catalog::StoredRecord`) when its metadata is none or an
//! edge on `V1_EDGE_VARIANT`, and in `revision` otherwise; a record an earlier
//! release wrote is in `revision`. The codec reads a `Record` in either
//! encoding, into the same value, and writes the `revision` encoding, which
//! every release reads: the codec has no transaction, so it knows neither the
//! store's feature level nor a table's current generation. It holds no table
//! layouts, so it reads a v1 record of generation 0 only: any other
//! generation is refused with `catalog::RecordLayoutError::NoLayouts`.

mod desc;
pub(crate) mod key;
mod types;
mod value;
mod wire;

use anyhow::{Result, bail};
use desc::{Body, TypeDesc};
use surrealdb_kvs::{EncodeContext, Encoded};
pub(crate) use types::{Codec, EDGE_CODEC, FULL_CODEC, LEAF_CODEC, TypeId};

use crate::catalog::{Metadata, Record, RecordFormat, RecordLayoutError, StoredRecord};
use crate::val::{Object, Value};

/// The type of a value stored as its UTF-8 bytes, unframed and unrevisioned:
/// how a `String` is stored as a key-value value.
pub(crate) const UTF8: &str = "Utf8";

/// The type stored in the v1 record envelope, or in `revision` when its
/// metadata needs it.
const RECORD: &str = "Record";

/// Decode `bytes`, a whole encoded value of type `name`, through `codec`.
pub(crate) fn decode(codec: &'static Codec, name: &str, bytes: &[u8]) -> Result<Value> {
	if name == UTF8 {
		return Ok(Value::String(std::str::from_utf8(bytes)?.into()));
	}
	if name == RECORD
		&& let Some(stored) = StoredRecord::parse(bytes)?
	{
		return envelope_value(codec, &stored);
	}
	decode_desc(codec, codec.lookup(name)?, bytes)
}

/// [`decode`] for the type `ty` names.
pub(crate) fn decode_id(codec: &'static Codec, ty: TypeId, bytes: &[u8]) -> Result<Value> {
	let Some(desc) = ty.desc(codec)? else {
		return Ok(Value::String(std::str::from_utf8(bytes)?.into()));
	};
	if desc.name == RECORD
		&& let Some(stored) = StoredRecord::parse(bytes)?
	{
		return envelope_value(codec, &stored);
	}
	decode_desc(codec, desc, bytes)
}

/// Decode `bytes`, a whole encoded value of the type `desc` describes. A
/// catalog entry is read in either grammar, as the value its derive converts
/// it to; any other value must be consumed whole.
fn decode_desc(codec: &'static Codec, desc: &TypeDesc, bytes: &[u8]) -> Result<Value> {
	if let Body::Catalog(vtable) = &desc.body {
		return (vtable.decode)(bytes);
	}
	let mut r = wire::Reader::in_codec(bytes, codec);
	let value = desc::decode_named(desc, &mut r)?;
	if !r.is_empty() {
		bail!("{} trailing bytes after a {}", r.remaining(), desc.name);
	}
	Ok(value)
}

/// Whether `ty` names a catalog type, whose bytes depend on the feature level
/// they are written at: a program writes one through
/// `Transaction::put_levelled_raw` with [`encode_id_at`] as its encoder.
pub(crate) fn is_catalog(codec: &Codec, ty: TypeId) -> bool {
	matches!(
		ty.desc(codec),
		Ok(Some(TypeDesc {
			body: Body::Catalog(_),
			..
		}))
	)
}

/// [`encode`] for the type `ty` names, at the level and over the prior entry
/// `ctx` gives. Only a catalog type's bytes depend on the context; the floor
/// is the release its bytes need to be read, which is `None` for a type with
/// no release floor.
pub(crate) fn encode_id_at(
	codec: &'static Codec,
	ty: TypeId,
	value: &Value,
	ctx: &EncodeContext<'_>,
) -> Result<Encoded> {
	let Some(desc) = ty.desc(codec)? else {
		let Value::String(s) = value else {
			bail!("a {UTF8} value that is not a string: {value:?}");
		};
		return Ok(Encoded {
			bytes: s.as_bytes().to_vec(),
			floor: None,
		});
	};
	encode_desc(codec, desc, value, ctx)
}

/// [`encode_id_at`] for a store at the legacy feature level: the bytes of a
/// type whose encoding does not depend on the level.
pub(crate) fn encode_id(codec: &'static Codec, ty: TypeId, value: &Value) -> Result<Vec<u8>> {
	encode_id_at(codec, ty, value, &EncodeContext::LEGACY).map(|encoded| encoded.bytes)
}

/// Encode `value` as type `name`, at its current revision. A catalog type is
/// written for a store at the legacy feature level, in `revision`.
pub(crate) fn encode(codec: &'static Codec, name: &str, value: &Value) -> Result<Vec<u8>> {
	if name == UTF8 {
		let Value::String(s) = value else {
			bail!("a {UTF8} value that is not a string: {value:?}");
		};
		return Ok(s.as_bytes().to_vec());
	}
	encode_desc(codec, codec.lookup(name)?, value, &EncodeContext::LEGACY)
		.map(|encoded| encoded.bytes)
}

/// Encode `value` as the type `desc` describes.
fn encode_desc(
	codec: &'static Codec,
	desc: &TypeDesc,
	value: &Value,
	ctx: &EncodeContext<'_>,
) -> Result<Encoded> {
	if desc.name == RECORD {
		return Ok(Encoded {
			bytes: encode_record_value(codec, value)?,
			floor: None,
		});
	}
	if let Body::Catalog(vtable) = &desc.body {
		return (vtable.encode_at)(value, ctx);
	}
	let mut w = wire::Writer::scratch().in_codec(codec);
	desc::encode_named(desc, value, &mut w)?;
	Ok(Encoded {
		bytes: w.finish(),
		floor: None,
	})
}

/// A record read from the v1 envelope, as [`record_value`] holds it. The codec holds no table
/// layouts, so a record of a generation other than
/// [`StoredRecord::NO_GENERATION`] is refused: it is read only against its table's layouts. A
/// packed row always names a nonzero generation, so it is refused here before its body is read.
fn envelope_value(codec: &'static Codec, stored: &StoredRecord<'_>) -> Result<Value> {
	let generation = stored.stamp();
	if generation != StoredRecord::NO_GENERATION {
		return Err(anyhow::Error::new(crate::catalog::Error::Layout(
			RecordLayoutError::NoLayouts {
				generation,
			},
		)));
	}
	record_value(codec, stored.decode_document()?)
}

/// A record as the codec holds a `Record` it decodes from `revision`: an
/// object of its metadata, as the descriptors hold an `Option<Metadata>`, and
/// its data.
fn record_value(codec: &'static Codec, record: Record) -> Result<Value> {
	let metadata = match &record.metadata {
		Some(metadata) => decode(codec, "Metadata", &revision::to_vec(metadata)?)?,
		None => Value::None,
	};
	Ok(Value::Object(Object::from_iter([
		("metadata".to_owned(), metadata),
		("data".to_owned(), record.data),
	])))
}

/// The `revision` encoding `catalog::encode_record` gives the record `value`
/// holds as [`record_value`] gives it, its `"id"` kept: the value names no key
/// to leave it out for.
fn encode_record_value(codec: &'static Codec, value: &Value) -> Result<Vec<u8>> {
	let Value::Object(fields) = value else {
		bail!("a {RECORD} value that is not an object: {value:?}");
	};
	let metadata = match fields.get("metadata") {
		None | Some(Value::None) => None,
		Some(metadata) => {
			Some(revision::from_slice::<Metadata>(&encode(codec, "Metadata", metadata)?)?)
		}
	};
	let data = fields.get("data").unwrap_or(&Value::None);
	crate::catalog::encode_record(RecordFormat::Revision, metadata.as_ref(), data, None)
}

/// A decoded `Record` as a read from its key sees it: the key is the source
/// of truth for the record's identity, so a record whose data is an object
/// has its `id` set to the key's, whatever the stored bytes held.
#[cfg(test)]
pub(crate) fn record_read_at(mut record: Value, rid: &crate::val::RecordId) -> Value {
	if let Value::Object(fields) = &mut record
		&& let Some(Value::Object(data)) = fields.get_mut("data")
	{
		data.insert("id".to_owned(), Value::RecordId(rid.clone()));
	}
	record
}

/// What a value decoded from under a key reads as, given the key's context.
#[cfg(test)]
pub(crate) trait KeyContext {
	fn read(&self, value: Value) -> Value;
}

#[cfg(test)]
impl KeyContext for () {
	fn read(&self, value: Value) -> Value {
		value
	}
}

#[cfg(test)]
impl KeyContext for crate::catalog::RecordContext {
	fn read(&self, value: Value) -> Value {
		record_read_at(value, &self.id)
	}
}

/// Whether the VM holds a descriptor for type `name`.
#[cfg(test)]
pub(crate) fn describes(codec: &Codec, name: &str) -> bool {
	codec.lookup(name).is_ok()
}

#[cfg(test)]
mod tests;
