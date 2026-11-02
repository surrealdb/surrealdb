//! Descriptors, and the decoder and encoder that interpret them.

use anyhow::{Context, Result, bail};
use rust_decimal::Decimal;

use super::value;
use super::wire::{Reader, Writer};
use crate::catalog::stored::CatalogVTable;
use crate::val::{Number, Object, Value};

/// The shape of an encoded value.
#[derive(Debug, Clone)]
pub(super) enum Ty {
	Bool,
	/// Unsigned integers are varints.
	Uint,
	/// Signed integers are zigzag varints.
	Int,
	/// A `String`, `Strand`, `TableName` or `str`: length and UTF-8.
	String,
	/// A `Vec<u8>`: length and bytes.
	Bytes,
	Uuid,
	Option(Box<Ty>),
	/// Length, then each element.
	Vec(Box<Ty>),
	Tuple(Vec<Ty>),
	/// A type with a descriptor of its own.
	Named(&'static str),
	/// A type of the `Value` family, read and written natively
	/// ([`super::value`]).
	Native(Native),
}

/// The `Value` family's types, each of which decodes to the value it is.
#[derive(Debug, Clone, Copy)]
pub(super) enum Native {
	Value,
	Number,
	/// `val::Datetime`: a revisioned newtype over a `chrono` datetime.
	Datetime,
	RecordId,
	/// A `RecordIdKey` on its own, read as a record id of the empty table.
	RecordIdKey,
}

/// How one revision of a type is framed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Framing {
	/// Fields in order; an enum's variant as a discriminant varint first.
	Plain,
	/// `optimised, indexed_struct`: a struct's fields behind a `u32`
	/// little-endian length and a prologue of each field's `u32` offset into
	/// the payload.
	Indexed,
}

/// A field that a revision may lack, and what it reads as then.
#[derive(Debug, Clone)]
pub(super) enum Missing {
	/// The type's default: `NONE`, zero, empty, `false`.
	Default,
	/// A fixed value.
	Value(fn() -> Value),
}

/// One field of a struct, or of a struct-like variant.
#[derive(Debug, Clone)]
pub(super) struct Field {
	pub(super) name: &'static str,
	pub(super) ty: Ty,
	/// The first revision that writes it.
	pub(super) start: u16,
	/// What it reads as in a revision that does not write it.
	pub(super) missing: Missing,
}

impl Field {
	pub(super) fn new(name: &'static str, ty: Ty) -> Self {
		Self {
			name,
			ty,
			start: 1,
			missing: Missing::Default,
		}
	}

	pub(super) fn start(mut self, start: u16) -> Self {
		self.start = start;
		self
	}

	pub(super) fn missing(mut self, missing: Missing) -> Self {
		self.missing = missing;
		self
	}

	fn alive(&self, revision: u16) -> bool {
		self.start <= revision
	}
}

/// One variant of an enum.
#[derive(Debug, Clone)]
pub(super) struct Variant {
	pub(super) name: &'static str,
	pub(super) shape: Shape,
	pub(super) start: u16,
	pub(super) end: Option<u16>,
	/// For a variant no current revision writes, the current value it
	/// reads as.
	pub(super) convert: Option<fn(Value) -> Result<Value>>,
}

/// What a variant carries.
#[derive(Debug, Clone)]
pub(super) enum Shape {
	Unit,
	/// Positional fields; one field reads as its value, several as an array.
	Tuple(Vec<Ty>),
	/// Named fields, read as an object.
	Struct(Vec<Field>),
}

impl Variant {
	pub(super) fn new(name: &'static str, shape: Shape) -> Self {
		Self {
			name,
			shape,
			start: 1,
			end: None,
			convert: None,
		}
	}

	pub(super) fn start(mut self, start: u16) -> Self {
		self.start = start;
		self
	}

	/// Retire the variant at revision `end`: what an older revision wrote
	/// as it reads as `convert` of its body.
	pub(super) fn end(mut self, end: u16, convert: fn(Value) -> Result<Value>) -> Self {
		self.end = Some(end);
		self.convert = Some(convert);
		self
	}

	fn alive(&self, revision: u16) -> bool {
		self.start <= revision && self.end.is_none_or(|end| revision < end)
	}
}

/// What a type is.
#[derive(Debug, Clone)]
pub(super) enum Body {
	Struct(Vec<Field>),
	Enum(Vec<Variant>),
	/// Written as another shape, with no revision header of its own: an id
	/// newtype over a `u32`, a string wrapper.
	Transparent(Ty),
	/// A catalog type, read and written by the codec its derive generates.
	/// The type's own entry is read and written by [`CatalogVTable::decode`]
	/// and [`CatalogVTable::encode_at`]; a copy held inside a `revision`
	/// container, by [`CatalogVTable::decode_embedded`] and
	/// [`CatalogVTable::encode_embedded`].
	Catalog(&'static CatalogVTable),
}

/// A revisioned type.
#[derive(Debug, Clone)]
pub(crate) struct TypeDesc {
	pub(super) name: &'static str,
	/// The revision encoding writes.
	pub(super) current: u16,
	/// The first revision framed `optimised, indexed_struct`, if any.
	pub(super) indexed_from: Option<u16>,
	pub(super) body: Body,
}

impl TypeDesc {
	pub(super) fn structure(name: &'static str, current: u16, fields: Vec<Field>) -> Self {
		Self {
			name,
			current,
			indexed_from: None,
			body: Body::Struct(fields),
		}
	}

	pub(super) fn enumeration(name: &'static str, current: u16, variants: Vec<Variant>) -> Self {
		Self {
			name,
			current,
			indexed_from: None,
			body: Body::Enum(variants),
		}
	}

	pub(super) fn transparent(name: &'static str, ty: Ty) -> Self {
		Self {
			name,
			current: 1,
			indexed_from: None,
			body: Body::Transparent(ty),
		}
	}

	/// The descriptor of a catalog type, which names its generated codec.
	pub(super) fn catalog(vtable: &'static CatalogVTable) -> Self {
		Self {
			name: vtable.name,
			current: 0,
			indexed_from: None,
			body: Body::Catalog(vtable),
		}
	}

	pub(super) fn indexed_from(mut self, revision: u16) -> Self {
		self.indexed_from = Some(revision);
		self
	}

	fn framing(&self, revision: u16) -> Framing {
		match self.indexed_from {
			Some(from) if revision >= from => Framing::Indexed,
			_ => Framing::Plain,
		}
	}
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode a value of `desc` held inside a `revision` container, from the
/// front of `r`.
pub(super) fn decode_named(desc: &TypeDesc, r: &mut Reader<'_>) -> Result<Value> {
	match &desc.body {
		Body::Transparent(ty) => return decode_ty(ty, r),
		Body::Catalog(vtable) => return decode_embedded(vtable, r),
		Body::Struct(_) | Body::Enum(_) => {}
	}
	let revision = u16::try_from(r.varint()?)
		.map_err(|_| anyhow::anyhow!("a revision past u16 for {}", desc.name))?;
	if revision == 0 || revision > desc.current {
		bail!("{} has no revision {revision}", desc.name);
	}
	let framing = desc.framing(revision);
	match &desc.body {
		Body::Struct(fields) => match framing {
			Framing::Plain => decode_fields(fields, revision, r),
			Framing::Indexed => {
				let mut payload = r.varlen()?;
				let alive = fields.iter().filter(|f| f.alive(revision)).count();
				payload.take(alive * 4)?;
				decode_fields(fields, revision, &mut payload)
			}
		},
		Body::Enum(variants) => decode_variant(desc, variants, revision, r),
		Body::Transparent(_) | Body::Catalog(_) => unreachable!("handled above"),
	}
	.with_context(|| format!("decoding a {} at revision {revision}", desc.name))
}

/// Decode the catalog type `vtable` describes from the front of `r`, where it
/// is held inside a `revision` container, and advance `r` past exactly what
/// it read.
fn decode_embedded(vtable: &CatalogVTable, r: &mut Reader<'_>) -> Result<Value> {
	let mut cursor = r.rest();
	let value = (vtable.decode_embedded)(&mut cursor)?;
	let used = r
		.remaining()
		.checked_sub(cursor.len())
		.with_context(|| format!("a {} read past what it was given", vtable.name))?;
	r.advance(used)?;
	Ok(value)
}

fn decode_fields(fields: &[Field], revision: u16, r: &mut Reader<'_>) -> Result<Value> {
	let mut read = Vec::with_capacity(fields.len());
	for field in fields.iter().filter(|f| f.alive(revision)) {
		read.push((field.name, decode_ty(&field.ty, r)?));
	}
	let mut object = Object::default();
	for field in fields {
		let value = match read.iter().position(|(name, _)| *name == field.name) {
			Some(at) => read.swap_remove(at).1,
			None => missing(field)?,
		};
		object.insert(field.name.to_owned(), value);
	}
	Ok(Value::Object(object))
}

fn missing(field: &Field) -> Result<Value> {
	Ok(match &field.missing {
		Missing::Default => default_of(&field.ty)?,
		Missing::Value(value) => value(),
	})
}

fn default_of(ty: &Ty) -> Result<Value> {
	Ok(match ty {
		Ty::Bool => Value::Bool(false),
		Ty::Uint | Ty::Int => Value::Number(Number::Int(0)),
		Ty::String => Value::String(Default::default()),
		Ty::Bytes => Value::Bytes(Default::default()),
		Ty::Option(_) => Value::None,
		Ty::Vec(_) => Value::Array(Default::default()),
		Ty::Uuid => Value::Uuid(Default::default()),
		other @ (Ty::Tuple(_) | Ty::Named(_) | Ty::Native(_)) => {
			bail!("no default for a missing {other:?}")
		}
	})
}

fn decode_variant(
	desc: &TypeDesc,
	variants: &[Variant],
	revision: u16,
	r: &mut Reader<'_>,
) -> Result<Value> {
	let alive: Vec<&Variant> = variants.iter().filter(|v| v.alive(revision)).collect();
	let index = usize::try_from(r.varint()?).context("a variant index past usize")?;
	let variant =
		alive.get(index).with_context(|| format!("no variant {index} of {}", desc.name))?;
	let body = decode_shape(&variant.shape, revision, r)?;
	if let Some(convert) = variant.convert {
		return convert(body);
	}
	let mut object = Object::default();
	object.insert(variant.name.to_owned(), body);
	Ok(Value::Object(object))
}

fn decode_shape(shape: &Shape, revision: u16, r: &mut Reader<'_>) -> Result<Value> {
	Ok(match shape {
		Shape::Unit => Value::None,
		Shape::Tuple(tys) if tys.len() == 1 => decode_ty(&tys[0], r)?,
		Shape::Tuple(tys) => {
			let mut out = Vec::with_capacity(tys.len());
			for ty in tys {
				out.push(decode_ty(ty, r)?);
			}
			Value::Array(out.into())
		}
		Shape::Struct(fields) => decode_fields(fields, revision, r)?,
	})
}

fn int(v: i128) -> Result<Value> {
	Ok(Value::Number(match i64::try_from(v) {
		Ok(v) => Number::Int(v),
		Err(_) => Number::Decimal(
			Decimal::try_from_i128_with_scale(v, 0)
				.map_err(|_| anyhow::anyhow!("an integer past a decimal"))?,
		),
	}))
}

fn decode_ty(ty: &Ty, r: &mut Reader<'_>) -> Result<Value> {
	Ok(match ty {
		Ty::Bool => match r.u8()? {
			0 => Value::Bool(false),
			1 => Value::Bool(true),
			b => bail!("an invalid bool {b}"),
		},
		Ty::Uint => int(i128::try_from(r.varint()?)?)?,
		Ty::Int => int(r.zigzag()?)?,
		Ty::String => Value::String(
			std::str::from_utf8(r.bytes()?).context("a string that is not UTF-8")?.into(),
		),
		Ty::Bytes => Value::Bytes(r.bytes()?.to_vec().into()),
		Ty::Uuid => Value::Uuid(crate::val::Uuid(uuid::Uuid::from_bytes(r.array()?))),
		Ty::Option(inner) => match r.u8()? {
			0 => Value::None,
			1 if holds_none(inner) => Value::Array(vec![decode_ty(inner, r)?].into()),
			1 => decode_ty(inner, r)?,
			b => bail!("an invalid option tag {b}"),
		},
		Ty::Vec(inner) => {
			let len = r.len()?;
			let mut out = Vec::with_capacity(len.min(4096));
			for _ in 0..len {
				out.push(decode_ty(inner, r)?);
			}
			Value::Array(out.into())
		}
		Ty::Tuple(tys) => {
			let mut out = Vec::with_capacity(tys.len());
			for ty in tys {
				out.push(decode_ty(ty, r)?);
			}
			Value::Array(out.into())
		}
		Ty::Named(name) => decode_named(r.codec()?.lookup(name)?, r)?,
		Ty::Native(native) => match native {
			Native::Value => value::decode(r)?,
			Native::Number => Value::Number(value::decode_number(r)?),
			Native::Datetime => Value::Datetime(value::decode_val_datetime(r)?),
			Native::RecordId => Value::RecordId(value::decode_record_id(r)?),
			Native::RecordIdKey => Value::RecordId(crate::val::RecordId {
				table: crate::val::TableName::new(""),
				key: value::decode_record_id_key(r)?,
			}),
		},
	})
}

/// Whether a value of `ty` can itself be `NONE`, so an option of it reads
/// `Some(x)` as the one-element array `[x]` and only `None` as `NONE`.
fn holds_none(ty: &Ty) -> bool {
	matches!(ty, Ty::Native(Native::Value) | Ty::Option(_))
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode `value` as `desc` held inside a `revision` container.
pub(super) fn encode_named(desc: &TypeDesc, value: &Value, w: &mut Writer) -> Result<()> {
	match &desc.body {
		Body::Transparent(ty) => return encode_ty(ty, value, w),
		Body::Catalog(vtable) => return w.embedded(|out| (vtable.encode_embedded)(value, out)),
		Body::Struct(_) | Body::Enum(_) => {}
	}
	match &desc.body {
		Body::Struct(_) => {
			let object = object(value)?;
			encode_struct(desc, |field| object.get(field.name).unwrap_or(&Value::None), w)
		}
		Body::Enum(variants) => {
			w.varint(desc.current as u128);
			encode_variant(desc, variants, desc.current, value, w)
				.map_err(|e| located(e, || format!("encoding a {}", desc.name)))
		}
		Body::Transparent(_) | Body::Catalog(_) => unreachable!("handled above"),
	}
}

/// Encode struct `desc` at its current revision, each field alive in it the
/// value `field` answers for it, in the framing the revision was written in.
pub(super) fn encode_struct<'v>(
	desc: &TypeDesc,
	mut field: impl FnMut(&Field) -> &'v Value,
	w: &mut Writer,
) -> Result<()> {
	let Body::Struct(fields) = &desc.body else {
		bail!("{} is not a struct", desc.name);
	};
	let revision = desc.current;
	w.varint(revision as u128);
	let alive = || fields.iter().filter(|f| f.alive(revision));
	let encode = |f: &Field, value: &Value, w: &mut Writer| {
		encode_ty(&f.ty, value, w).map_err(|e| located(e, || format!("field `{}`", f.name)))
	};
	match desc.framing(revision) {
		Framing::Plain => {
			for f in alive() {
				encode(f, field(f), w)?;
			}
			Ok(())
		}
		// A table of where each field's body starts, counted from the start of
		// the table, then the bodies.
		Framing::Indexed => w.varlen(|w| {
			let count = alive().count();
			let table = w.reserve(count * 4);
			for (at, f) in alive().enumerate() {
				let off = w.since(table)?;
				w.patch_u32(table + at * 4, off);
				encode(f, field(f), w)?;
			}
			Ok(())
		}),
	}
	.map_err(|e| located(e, || format!("encoding a {}", desc.name)))
}

/// `e`, an error encoding a value, located by `at`; the error a closure's
/// encoding raises is the user's own, and is left as it is.
fn located(e: anyhow::Error, at: impl FnOnce() -> String) -> anyhow::Error {
	if e.downcast_ref::<revision::Error>().is_some() {
		e
	} else {
		e.context(at())
	}
}

fn object(value: &Value) -> Result<&Object> {
	if let Value::Object(object) = value {
		Ok(object)
	} else {
		bail!("expected an object, found {value:?}")
	}
}

fn encode_fields(fields: &[Field], revision: u16, value: &Value, w: &mut Writer) -> Result<()> {
	let object = object(value)?;
	for field in fields.iter().filter(|f| f.alive(revision)) {
		let value = object.get(field.name).unwrap_or(&Value::None);
		encode_ty(&field.ty, value, w)
			.map_err(|e| located(e, || format!("field `{}`", field.name)))?;
	}
	Ok(())
}

fn encode_variant(
	desc: &TypeDesc,
	variants: &[Variant],
	revision: u16,
	value: &Value,
	w: &mut Writer,
) -> Result<()> {
	let object = object(value)?;
	let (name, body) = object
		.iter()
		.next()
		.filter(|_| object.len() == 1)
		.context("a variant is an object with one key")?;
	let alive: Vec<&Variant> = variants.iter().filter(|v| v.alive(revision)).collect();
	let index = alive
		.iter()
		.position(|v| v.name == name.as_str())
		.with_context(|| format!("{} has no variant `{name}`", desc.name))?;
	w.varint(index as u128);
	encode_shape(&alive[index].shape, revision, body, w)
}

fn encode_shape(shape: &Shape, revision: u16, value: &Value, w: &mut Writer) -> Result<()> {
	match shape {
		Shape::Unit => Ok(()),
		Shape::Tuple(tys) if tys.len() == 1 => encode_ty(&tys[0], value, w),
		Shape::Tuple(tys) => {
			let items = array(value)?;
			if items.len() != tys.len() {
				bail!("a tuple variant of {} fields holding {}", tys.len(), items.len());
			}
			for (ty, item) in tys.iter().zip(items) {
				encode_ty(ty, item, w)?;
			}
			Ok(())
		}
		Shape::Struct(fields) => encode_fields(fields, revision, value, w),
	}
}

fn array(value: &Value) -> Result<&[Value]> {
	if let Value::Array(array) = value {
		Ok(array.0.as_slice())
	} else {
		bail!("expected an array, found {value:?}")
	}
}

fn integer(value: &Value) -> Result<i128> {
	if let Value::Number(Number::Int(v)) = value {
		Ok(*v as i128)
	} else if let Value::Number(Number::Decimal(d)) = value
		&& d.fract().is_zero()
	{
		d.mantissa().checked_div(10i128.pow(d.scale())).context("an integer decimal")
	} else {
		bail!("expected an integer, found {value:?}")
	}
}

fn encode_ty(ty: &Ty, value: &Value, w: &mut Writer) -> Result<()> {
	match ty {
		Ty::Bool => {
			if let Value::Bool(b) = value {
				w.u8(*b as u8);
			} else {
				bail!("expected a bool, found {value:?}");
			}
		}
		Ty::Uint => w.varint(u128::try_from(integer(value)?)?),
		Ty::Int => w.zigzag(integer(value)?),
		Ty::String => match value {
			Value::String(s) => w.bytes(s.as_bytes()),
			Value::Table(t) => w.bytes(t.as_str().as_bytes()),
			other @ (Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => bail!("expected a string, found {other:?}"),
		},
		Ty::Bytes => {
			if let Value::Bytes(b) = value {
				w.bytes(&b.0);
			} else {
				bail!("expected bytes, found {value:?}");
			}
		}
		Ty::Uuid => {
			if let Value::Uuid(u) = value {
				w.raw(u.0.as_bytes());
			} else {
				bail!("expected a uuid, found {value:?}");
			}
		}
		Ty::Option(inner) => match value {
			Value::None => w.u8(0),
			some @ (Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_))
				if holds_none(inner) =>
			{
				let [some] = array(some)? else {
					bail!("an option of a value is NONE or a one-element array");
				};
				w.u8(1);
				encode_ty(inner, some, w)?;
			}
			some @ (Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => {
				w.u8(1);
				encode_ty(inner, some, w)?;
			}
		},
		Ty::Vec(inner) => {
			let items = array(value)?;
			w.varint(items.len() as u128);
			for item in items {
				encode_ty(inner, item, w)?;
			}
		}
		Ty::Tuple(tys) => {
			let items = array(value)?;
			if items.len() != tys.len() {
				bail!("a tuple of {} holding {}", tys.len(), items.len());
			}
			for (ty, item) in tys.iter().zip(items) {
				encode_ty(ty, item, w)?;
			}
		}
		Ty::Named(name) => encode_named(w.codec()?.lookup(name)?, value, w)?,
		Ty::Native(native) => match (native, value) {
			(Native::Value, value) => value::encode(value, w)?,
			(Native::Number, Value::Number(n)) => value::encode_number(n, w)?,
			(Native::Datetime, Value::Datetime(d)) => value::encode_val_datetime(d, w),
			(Native::RecordId, Value::RecordId(rid)) => value::encode_record_id(rid, w)?,
			(Native::RecordIdKey, Value::RecordId(rid)) => {
				value::encode_record_id_key(&rid.key, w)?
			}
			(native, other) => bail!("expected a {native:?}, found {other:?}"),
		},
	}
	Ok(())
}
