//! The static description of a derived type: what the schema lock renders and the lock checks
//! compare.

use surrealdb_kvs::release::Release;

/// A derived type's schema, declared once by its attribute table.
///
/// The names of types are the Rust identifiers; the lock keys on them.
#[derive(Clone, Copy, Debug)]
pub struct TypeSchema {
	pub name: &'static str,
	/// The release the type first shipped in.
	pub since: Release,
	/// Retired numbers: field numbers of a struct, variant numbers of an enum.
	pub reserved: &'static [u32],
	/// The type is also embedded in a `revision` container, through `EmbeddedEntry`.
	pub embedded_in_revision: bool,
	/// Every entry of the type has the first release as its floor, forever.
	pub floor_fixed: bool,
	pub shape: Shape,
}

#[derive(Clone, Copy, Debug)]
pub enum Shape {
	Struct(&'static [FieldSchema]),
	Enum(&'static [VariantSchema]),
	/// A single-field newtype, encoded as its field. Holds the field's type name.
	Transparent(fn(&mut String)),
}

/// One field number of a struct or struct variant.
///
/// A field that supersedes an older number has two entries: its own, whose `supersedes` names the
/// older number, and one for the older number holding the narrower type.
#[derive(Clone, Copy, Debug)]
pub struct FieldSchema {
	pub n: u32,
	pub name: &'static str,
	/// Writes the field's wire type name (see [`StoredField::write_type_name`]).
	///
	/// [`StoredField::write_type_name`]: super::StoredField::write_type_name
	pub ty: fn(&mut String),
	pub class: Class,
	pub since: Release,
	pub absent: Absent,
	pub supersedes: Option<u32>,
	/// For an ignorable field, why an earlier release loses nothing by skipping it.
	pub independent: Option<&'static str>,
	/// The field is an `Unrevisioned<_>`: the `revision` encoding leaves it out.
	pub unrevisioned: bool,
}

impl FieldSchema {
	/// The wire type name of the field.
	pub fn type_name(&self) -> String {
		let mut out = String::new();
		(self.ty)(&mut out);
		out
	}
}

/// A field's class, which is the low bit of its number: even is critical, odd is ignorable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
	Critical,
	Ignorable,
}

/// What a field's absence means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Absent {
	/// Absence is refused.
	Required,
	/// Absence is `None`.
	None,
	/// Absence is what the named function returns.
	Default(&'static str),
}

/// One variant of an enum. A legacy variant, which is read from `revision` bytes only and never
/// written, has no number and a unit payload.
#[derive(Clone, Copy, Debug)]
pub struct VariantSchema {
	pub n: Option<u32>,
	pub name: &'static str,
	pub since: Release,
	pub payload: Payload,
}

#[derive(Clone, Copy, Debug)]
pub enum Payload {
	Unit,
	/// One element, encoded as its item. Holds its wire type name.
	Item(fn(&mut String)),
	/// Several elements in a positional map, numbered 2, 4, 6, … in order.
	Positional(&'static [fn(&mut String)]),
	Fields(&'static [FieldSchema]),
}
