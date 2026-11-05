//! Keys, encoded and decoded from the `keyspace!` schema's routes.
//!
//! The schema emits every key type's layout as a [`KeyRoute`]: literal bytes
//! and typed fields in encoder order. A key is encoded from its route and an
//! object holding its fields, and decoded back into one, with the `storekey`
//! encoding of each field type:
//!
//! | schema type | encoding |
//! |---|---|
//! | `ReclaimKind`, `Expunge` | one byte, the discriminant their codecs write |
//! | `u16`, `u32`, `u64`, and the id and counter aliases over them | big-endian |
//! | `i64` | big-endian with the sign bit flipped, so bytes sort as numbers |
//! | `bool` | `3` for true, `2` for false |
//! | `Uuid` | its 16 bytes |
//! | `Str`, `Table` | UTF-8 with `0x00` and `0x01` escaped behind `0x01`, then `0x00` |
//!
//! A literal byte is never `0x00` or `0x01` (the schema check forbids it), so
//! literals are written as they are.

use anyhow::{Context, Result, bail};
use surrealdb_kvs::key::{KeyRoute, KeyStep};

use crate::val::{Number, Value};

/// A key type, as its index into the schema's route table
/// (`KEY_ROUTES`): plain data a program stores in place of the route's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RouteId(pub(crate) u16);

impl RouteId {
	/// The id of key type `name`. The name is a literal the lowering writes;
	/// one the schema does not hold is a bug in the lowering, not a runtime
	/// condition.
	pub(crate) fn named(name: &str) -> Self {
		match by_name().get(name) {
			Some(id) => Self(*id),
			None => panic!("no key route named `{name}`"),
		}
	}

	/// The route this id names.
	pub(crate) fn route(self) -> Result<&'static KeyRoute> {
		surrealdb_datastore::key::schema::KEY_ROUTES
			.get(self.0 as usize)
			.with_context(|| format!("no key route at index {}", self.0))
	}

	/// The name of the key type, for diagnostics.
	pub(crate) fn name(self) -> &'static str {
		self.route().map_or("<unknown route>", |route| route.name)
	}
}

fn by_name() -> &'static std::collections::HashMap<&'static str, u16> {
	static BY_NAME: std::sync::LazyLock<std::collections::HashMap<&'static str, u16>> =
		std::sync::LazyLock::new(|| {
			surrealdb_datastore::key::schema::KEY_ROUTES
				.iter()
				.enumerate()
				.map(|(index, route)| {
					let id = u16::try_from(index).expect("fewer than 65536 key routes");
					(route.name, id)
				})
				.collect()
		});
	&BY_NAME
}

/// The route of key type `name`.
pub(crate) fn route(name: &str) -> Result<&'static KeyRoute> {
	let id = by_name().get(name).with_context(|| format!("no key route named `{name}`"))?;
	RouteId(*id).route()
}

/// The width of a fixed-width integer field of schema type `ty`, and whether
/// it is signed.
fn integer(ty: &str) -> Option<(usize, bool)> {
	Some(match ty {
		"ReclaimKind" | "Expunge" => (1, false),
		"u16" | "TaskLeaseType" => (2, false),
		"u32"
		| "NamespaceId"
		| "DatabaseId"
		| "IndexId"
		| "BuildTicketMutationSeq"
		| "AppendingId"
		| "BatchId" => (4, false),
		"u64" | "DocId" | "BuildGeneration" | "BuildTicket" | "ElementId" => (8, false),
		"i64" => (8, true),
		_ => return None,
	})
}

/// Encode a key of type `name` from `fields`, an object holding each field by
/// its schema name.
#[cfg(test)]
pub(crate) fn encode(name: &str, fields: &Value) -> Result<Vec<u8>> {
	encode_route(route(name)?, fields)
}

/// [`encode`] for the key type `id` names.
pub(crate) fn encode_id(id: RouteId, fields: &Value) -> Result<Vec<u8>> {
	encode_route(id.route()?, fields)
}

fn encode_route(route: &'static KeyRoute, fields: &Value) -> Result<Vec<u8>> {
	let Value::Object(fields) = fields else {
		bail!("a key's fields are an object");
	};
	let mut out = Vec::new();
	for step in route.steps {
		match step {
			KeyStep::Lit(bytes) => out.extend_from_slice(bytes),
			KeyStep::Field {
				name,
				ty,
			} => {
				let value = fields
					.get(name)
					.with_context(|| format!("key `{}` has no field `{name}`", route.name))?;
				encode_field(ty, value, &mut out).map_err(|e| in_field(e, name, route))?;
			}
			KeyStep::List {
				..
			} => bail!("key `{}` holds a list field the VM does not encode yet", route.name),
		}
	}
	Ok(out)
}

/// A field of a key a write encodes on every record, borrowed from what the
/// record already holds rather than gathered into an object.
#[derive(Debug, Clone, Copy)]
pub(crate) enum KeyField<'a> {
	/// A fixed-width integer field.
	Int(i64),
	/// A table name, or any other string field.
	Str(&'a str),
	/// A record id's key.
	Id(&'a crate::val::RecordIdKey),
	/// An edge direction.
	Dir(crate::expr::Dir),
}

/// Encode a key of type `name` from `fields`, each field by its schema name,
/// as [`encode`] encodes one from an object holding the same fields.
pub(crate) fn encode_fields(name: &str, fields: &[(&str, KeyField<'_>)]) -> Result<Vec<u8>> {
	let route = route(name)?;
	let indexed = indexed_format(route)?;
	let mut out = Vec::with_capacity(64);
	let find = |name: &str| {
		fields
			.iter()
			.find(|(field, _)| *field == name)
			.map(|(_, value)| *value)
			.with_context(|| format!("key `{}` has no field `{name}`", route.name))
	};
	for step in route.steps {
		match step {
			KeyStep::Lit(bytes) => out.extend_from_slice(bytes),
			KeyStep::Field {
				name,
				ty,
			} => {
				encode_borrowed(ty, find(name)?, indexed, &mut out)
					.map_err(|e| in_field(e, name, route))?;
			}
			// An index entry's indexed values are encoded by the B-tree key
			// kernels (`idx::kernels::btree`), which both write paths and the
			// read path share.
			KeyStep::List {
				..
			} => bail!("key `{}` holds a list field the VM does not encode", route.name),
		}
	}
	Ok(out)
}

/// Whether a route's fields are encoded under the index format, which orders
/// the numbers of every kind as one, orders a datetime by time, and gives each
/// value the byte form an index compares, rather than under the default format.
///
/// The schema names the format exactly as the route declares it. A route
/// pinned to any other format is refused: encoding its fields under one of
/// these two would write bytes that are not that route's key.
fn indexed_format(route: &KeyRoute) -> Result<bool> {
	match route.format {
		// A format-generic route encodes the same under every format.
		"()" | "<generic>" => Ok(false),
		"IndexFormat" => Ok(true),
		other => {
			bail!(
				"key `{}` is encoded under `{other}`, a format the VM does not encode",
				route.name
			)
		}
	}
}

fn encode_borrowed(ty: &str, value: KeyField<'_>, indexed: bool, out: &mut Vec<u8>) -> Result<()> {
	match value {
		KeyField::Int(v) => encode_field(ty, &Value::from(v), out),
		KeyField::Str(s) if matches!(ty, "Str" | "Table") => {
			escaped(s.as_bytes(), out);
			Ok(())
		}
		// A record id's key under the route's format: an index entry's id is
		// encoded as its indexed values are.
		KeyField::Id(key) if ty == "Id" && indexed => {
			storekey::encode_format::<crate::val::IndexFormat, _, _>(&mut *out, key)
				.map_err(encode_error)
		}
		KeyField::Id(key) if ty == "Id" => storekey::encode(&mut *out, key).map_err(encode_error),
		// A record id's key as an identity, which spells the key losslessly
		// whatever the route's format is.
		KeyField::Id(key) if ty == "Identity" && indexed => {
			let identity = crate::val::RecordIdentity(key.clone());
			storekey::encode_format::<crate::val::IndexFormat, _, _>(&mut *out, &identity)
				.map_err(encode_error)
		}
		KeyField::Id(key) if ty == "Identity" => {
			let identity = crate::val::RecordIdentity(key.clone());
			storekey::encode(&mut *out, &identity).map_err(encode_error)
		}
		KeyField::Dir(dir) if ty == "Dir" => {
			storekey::encode(&mut *out, &dir).map_err(encode_error)
		}
		other @ (KeyField::Str(_) | KeyField::Id(_) | KeyField::Dir(_)) => {
			bail!("the VM does not encode a `{ty}` key field from {other:?}")
		}
	}
}

fn encode_field(ty: &str, value: &Value, out: &mut Vec<u8>) -> Result<()> {
	if let Some((width, signed)) = integer(ty) {
		let Value::Number(Number::Int(v)) = value else {
			bail!("expected an integer, found {value:?}");
		};
		let v = *v;
		match (width, signed) {
			(1, false) => out.push(u8::try_from(v)?),
			(2, false) => out.extend_from_slice(&u16::try_from(v)?.to_be_bytes()),
			(4, false) => out.extend_from_slice(&u32::try_from(v)?.to_be_bytes()),
			(8, false) => out.extend_from_slice(&u64::try_from(v)?.to_be_bytes()),
			(8, true) => out.extend_from_slice(&((v ^ i64::MIN) as u64).to_be_bytes()),
			_ => unreachable!("the integer widths the schema declares"),
		}
		return Ok(());
	}
	match (ty, value) {
		("bool", Value::Bool(b)) => out.push(if *b {
			3
		} else {
			2
		}),
		("Uuid", Value::Uuid(u)) => out.extend_from_slice(u.0.as_bytes()),
		("Str" | "Table", Value::String(s)) => escaped(s.as_bytes(), out),
		("Str" | "Table", Value::Table(t)) => escaped(t.as_str().as_bytes(), out),
		// A record id's key, in the `storekey` encoding every key type gives
		// its `Id` fields; the record id's table is not part of it.
		// An edge direction, in the `storekey` encoding of `Dir`.
		("Dir", Value::String(dir)) => {
			let dir = match dir.as_str() {
				"in" => crate::expr::Dir::In,
				"out" => crate::expr::Dir::Out,
				"both" => crate::expr::Dir::Both,
				other => bail!("`{other}` is not a direction"),
			};
			storekey::encode(&mut *out, &dir).map_err(encode_error)?
		}
		("Id", Value::RecordId(rid)) => {
			storekey::encode(&mut *out, &rid.key).map_err(encode_error)?
		}
		(ty, value) => bail!("the VM does not encode a `{ty}` key field from {value:?}"),
	}
	Ok(())
}

/// A key field's encoding failure. A value's own refusal of its key encoding
/// ([`surrealdb_kvs::key::Error`], such as an object with an empty field name)
/// stays that error, so the caller is told what was wrong with the value;
/// any other failure is its message.
fn encode_error(error: storekey::EncodeError) -> anyhow::Error {
	match error {
		storekey::EncodeError::Custom(reason) => {
			match reason.downcast::<surrealdb_kvs::key::Error>() {
				Ok(refusal) => anyhow::Error::new(*refusal),
				Err(reason) => anyhow::anyhow!("{reason}"),
			}
		}
		error @ storekey::EncodeError::Io(_) => anyhow::anyhow!("{error}"),
	}
}

/// `error`, raised encoding field `name` of `route`, located at that field. A
/// value's own refusal of its key encoding ([`encode_error`]) is reported as
/// it is: it describes the caller's value, not the key layout.
fn in_field(error: anyhow::Error, name: &str, route: &KeyRoute) -> anyhow::Error {
	if error.is::<surrealdb_kvs::key::Error>() {
		error
	} else {
		error.context(format!("field `{name}` of key `{}`", route.name))
	}
}

fn escaped(bytes: &[u8], out: &mut Vec<u8>) {
	for b in bytes {
		if *b <= 1 {
			out.push(1);
		}
		out.push(*b);
	}
	out.push(0);
}

/// Decode a key of type `name` into an object of its fields.
#[cfg(test)]
pub(crate) fn decode(name: &str, bytes: &[u8]) -> Result<Value> {
	let route = route(name)?;
	let mut rest = bytes;
	let mut fields = crate::val::Object::default();
	for step in route.steps {
		match step {
			KeyStep::Lit(lit) => {
				rest = rest
					.strip_prefix(*lit)
					.with_context(|| format!("bytes that are not a `{}` key", route.name))?;
			}
			KeyStep::Field {
				name,
				ty,
			} => {
				let (value, after) = decode_field(ty, rest)
					.with_context(|| format!("field `{name}` of key `{}`", route.name))?;
				fields.insert((*name).to_owned(), value);
				rest = after;
			}
			KeyStep::List {
				..
			} => bail!("key `{}` holds a list field the VM does not decode yet", route.name),
		}
	}
	if !rest.is_empty() {
		bail!("{} trailing bytes after a `{}` key", rest.len(), route.name);
	}
	Ok(Value::Object(fields))
}

#[cfg(test)]
fn decode_field<'b>(ty: &str, bytes: &'b [u8]) -> Result<(Value, &'b [u8])> {
	let take = |n: usize| -> Result<(&'b [u8], &'b [u8])> {
		if bytes.len() < n {
			bail!("a key field ended early");
		}
		Ok(bytes.split_at(n))
	};
	if let Some((width, signed)) = integer(ty) {
		let (head, rest) = take(width)?;
		let v = match (width, signed) {
			(1, false) => head[0] as i64,
			(2, false) => u16::from_be_bytes([head[0], head[1]]) as i64,
			(4, false) => u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as i64,
			(8, false) => i64::try_from(u64::from_be_bytes(head.try_into()?))?,
			(8, true) => (u64::from_be_bytes(head.try_into()?) as i64) ^ i64::MIN,
			_ => unreachable!("the integer widths the schema declares"),
		};
		return Ok((Value::Number(Number::Int(v)), rest));
	}
	match ty {
		"bool" => {
			let (head, rest) = take(1)?;
			let b = match head[0] {
				3 => true,
				2 => false,
				b => bail!("an invalid key bool {b}"),
			};
			Ok((Value::Bool(b), rest))
		}
		"Uuid" => {
			let (head, rest) = take(16)?;
			Ok((Value::Uuid(crate::val::Uuid(uuid::Uuid::from_slice(head)?)), rest))
		}
		"Str" | "Table" => {
			let mut out = Vec::new();
			let mut i = 0;
			loop {
				match bytes.get(i) {
					None => bail!("an unterminated key string"),
					Some(0) => break,
					Some(1) => {
						out.push(*bytes.get(i + 1).context("a dangling key escape")?);
						i += 2;
					}
					Some(b) => {
						out.push(*b);
						i += 1;
					}
				}
			}
			let s = String::from_utf8(out).context("a key string that is not UTF-8")?;
			Ok((Value::String(s.into()), &bytes[i + 1..]))
		}
		ty => bail!("the VM does not decode a `{ty}` key field"),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::key::{KeyRole, KeyRoute};

	use super::indexed_format;

	fn pinned(format: &'static str) -> KeyRoute {
		KeyRoute {
			name: "PinnedKey",
			route: "/pinned",
			role: KeyRole::Key,
			value: None,
			format,
			steps: &[],
		}
	}

	/// Only the index format itself selects it; a format whose name merely
	/// ends the same way is refused rather than encoded as another one.
	#[test]
	fn a_route_format_is_matched_exactly() {
		assert!(!indexed_format(&pinned("()")).unwrap());
		assert!(!indexed_format(&pinned("<generic>")).unwrap());
		assert!(indexed_format(&pinned("IndexFormat")).unwrap());
		for other in ["IndexIdFormat", "crate::val::IndexFormat", "MyIndexFormat"] {
			assert!(indexed_format(&pinned(other)).is_err(), "{other}");
		}
	}

	/// Every route the schema declares names a format the VM encodes.
	#[test]
	fn every_schema_route_names_a_known_format() {
		for route in surrealdb_datastore::key::schema::KEY_ROUTES {
			indexed_format(route).unwrap();
		}
	}
}
