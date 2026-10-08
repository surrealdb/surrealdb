//! The `revision` codec, as data the VM interprets.
//!
//! Stored values are `revision` bytes. The VM reads and writes them itself,
//! from a [`TypeDesc`] per revisioned type rather than through the Rust
//! type's generated impl, so a program never needs the Rust type to exist.
//! A decoded value is a [`Value`]: a struct is an object keyed by field name,
//! an enum an object with one key naming its variant, and each scalar the
//! value of its kind. An option is `NONE` or its value — or, for an option of
//! a type that can itself be `NONE`, a one-element array holding it.
//!
//! A descriptor transcribes the type's `#[revisioned]` history: the framing
//! each revision was written in, and each field or variant with the
//! revisions it is alive in. Decoding an older revision reads what that
//! revision wrote and fills what it lacks — a field's default, or a
//! [`Convert`] that rebuilds it from a field the revision had instead.
//! Encoding always writes the type's current revision.
//!
//! The descriptors are checked against the Rust codec on every frozen
//! compat fixture (`kvs/compat`): the VM decodes the fixture and re-encodes
//! it, and the bytes must be the ones the Rust codec writes for the same
//! fixture.

mod desc;
pub(crate) mod key;
mod types;
mod value;
mod wire;

use anyhow::{Result, bail};
pub(crate) use types::TypeId;

use crate::val::Value;

/// The type of a value stored as its UTF-8 bytes, unframed and unrevisioned:
/// how a `String` is stored as a key-value value.
pub(crate) const UTF8: &str = "Utf8";

/// Decode `bytes`, a whole encoded value of type `name`.
pub(crate) fn decode(name: &str, bytes: &[u8]) -> Result<Value> {
	if name == UTF8 {
		return Ok(Value::String(std::str::from_utf8(bytes)?.into()));
	}
	let desc = types::lookup(name)?;
	let mut r = wire::Reader::new(bytes);
	let value = desc::decode_named(desc, &mut r)?;
	if !r.is_empty() {
		bail!("{} trailing bytes after a {name}", r.remaining());
	}
	Ok(value)
}

/// [`decode`] for the type `ty` names.
pub(crate) fn decode_id(ty: TypeId, bytes: &[u8]) -> Result<Value> {
	let Some(desc) = ty.desc()? else {
		return Ok(Value::String(std::str::from_utf8(bytes)?.into()));
	};
	let mut r = wire::Reader::new(bytes);
	let value = desc::decode_named(desc, &mut r)?;
	if !r.is_empty() {
		bail!("{} trailing bytes after a {}", r.remaining(), desc.name);
	}
	Ok(value)
}

/// [`encode`] for the type `ty` names.
pub(crate) fn encode_id(ty: TypeId, value: &Value) -> Result<Vec<u8>> {
	let Some(desc) = ty.desc()? else {
		let Value::String(s) = value else {
			bail!("a {UTF8} value that is not a string: {value:?}");
		};
		return Ok(s.as_bytes().to_vec());
	};
	let mut w = wire::Writer::scratch();
	desc::encode_named(desc, value, &mut w)?;
	Ok(w.finish())
}

/// Encode `value` as type `name`, at its current revision.
pub(crate) fn encode(name: &str, value: &Value) -> Result<Vec<u8>> {
	if name == UTF8 {
		let Value::String(s) = value else {
			bail!("a {UTF8} value that is not a string: {value:?}");
		};
		return Ok(s.as_bytes().to_vec());
	}
	let desc = types::lookup(name)?;
	let mut w = wire::Writer::scratch();
	desc::encode_named(desc, value, &mut w)?;
	Ok(w.finish())
}

/// The bytes [`encode`] writes for a `Record` whose `metadata` and `data`
/// fields hold these two values, encoded from them where they are rather than
/// from an object gathering them.
pub(crate) fn encode_record(metadata: &Value, data: &Value) -> Result<Vec<u8>> {
	let desc = types::lookup("Record")?;
	let mut w = wire::Writer::scratch();
	desc::encode_struct(
		desc,
		|field| match field.name {
			"metadata" => metadata,
			"data" => data,
			_ => &Value::None,
		},
		&mut w,
	)?;
	Ok(w.finish())
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
impl KeyContext for crate::val::RecordId {
	fn read(&self, value: Value) -> Value {
		record_read_at(value, self)
	}
}

/// Whether the VM holds a descriptor for type `name`.
#[cfg(test)]
pub(crate) fn describes(name: &str) -> bool {
	types::lookup(name).is_ok()
}

#[cfg(test)]
mod tests;
