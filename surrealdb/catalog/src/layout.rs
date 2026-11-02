//! Row layouts of table generations: the immutable descriptor each generation's rows are read
//! against, and the set of them one table holds.
//!
//! A table's rows are stamped with a generation. Generation 0 needs no descriptor and holds
//! document rows. Every other generation has one [`LayoutDescriptor`], stored under its own key,
//! written only by the transaction that mints the generation and never rewritten. A descriptor
//! names its table and says how the rows of its generation are laid out: as documents, or as packed
//! rows whose columns, bit counts and slot offsets the descriptor lists.
//!
//! A descriptor is read once and trusted afterwards, so [`LayoutDescriptor::validate`] proves, on
//! every read and every write, that a packed descriptor has exactly one canonical form: every
//! presence bit, bool bit, fixed slot and var index it names lies inside the sizes it declares.
//! A reader that follows only the offsets of a validated descriptor never reads a wrong offset.
//!
//! [`TableLayouts`] is the table's generation ring and descriptors as one snapshot of its keys
//! lists them. A descriptor this build cannot use is kept as a refusal for its generation alone, so
//! a newer release's descriptor costs an earlier release only the rows of that generation. A
//! generation is a position in the table's [`GenerationRing`]: a stamp outside the ring is refused
//! without a lookup.
//!
//! # Physical kinds
//!
//! [`PhysicalKind`] is a stored enum with permanent numeric codes. A code that is not a variant is
//! refused as an unknown variant. [`PLANNED_PHYSICAL_KINDS`] lists the codes held for kinds that
//! are not defined yet; each is given only to the kind it names.

use std::sync::{Arc, OnceLock};

use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, Encoded};

use crate::generation::{Generation, GenerationRing};
use crate::migration::Migration;
use crate::record::RecordFormat;
use crate::stored::{
	EntryError, SchemaError, Stored, StoredType, canonical, decode_entry, encode_stored,
};
use crate::val::{TableName, Value};
use crate::{DatabaseId, Error, NamespaceId};

/// The most columns a packed descriptor holds.
pub const MAX_COLUMNS: usize = 65_535;

/// The largest fixed region, in bytes, a packed descriptor declares.
pub const MAX_FIXED_SIZE: u32 = 1 << 24;

/// The widest `Fixed(n)` slot, in bytes.
const MAX_FIXED_SLOT: u32 = 4096;

/// The most tables one `RecordKey` column names when it names more than one.
pub(crate) const MAX_KEY_TABLES: usize = 256;

/// The deepest column path a packed descriptor holds: the deepest a stored value nests.
///
/// It bounds the recursion of the encoder's walk and of the full decoder, which recurse once per
/// object node, and it refuses no descriptor a record could fill.
pub const MAX_PATH_DEPTH: usize = crate::val::MAX_VALUE_DEPTH;

/// Codes held for physical kinds that are not defined yet, with the kind each is held for.
///
/// A planned code is a gap in [`PhysicalKind`]: it decodes as an unknown variant, which is a
/// refusal, and it is given only to the kind it names.
pub const PLANNED_PHYSICAL_KINDS: &[(u32, &str)] = &[
	(11, "BF16"),
	(20, "DurationWide"),
	(28, "DictionaryString"),
	(29, "BigInt"),
	(31, "I128"),
	(32, "U128"),
	(33, "List"),
];

/// How the rows of one table generation are laid out, and the table they belong to.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0", validate)]
pub struct LayoutDescriptor {
	/// The table the descriptor belongs to. A reader refuses a generation whose descriptor names
	/// another table.
	#[stored(n = 2)]
	pub table: TableName,
	/// The layout of the generation's rows.
	#[stored(n = 4)]
	pub layout: RowLayout,
	/// The migration the generation was minted for. A row stamped before the generation passes
	/// through the step when it is read, so the step is as immutable as the descriptor and is
	/// retired with it.
	#[stored(n = 6)]
	pub migration: Option<Migration>,
	/// The newest generation, up to and including this one, whose descriptor carries a migration:
	/// this generation itself when it carries one, and the one before it that does otherwise.
	/// `None` when no generation of the ring up to this one does. The descriptor of the head
	/// therefore says which rows may need a migration step without any other descriptor being
	/// read.
	#[stored(n = 8)]
	pub migrated: Option<Generation>,
	/// The verification epoch that vouches for every row stamped with this generation: each such
	/// row is one the whole decode accepts. `None` when nothing vouches for them, and every
	/// in-place read of a row of the generation walks the row first (ADR 0010).
	///
	/// A build sets it on the descriptor of a generation it mints, which no row can carry before
	/// the mint: every row of the generation is then written by a record encoder, which writes
	/// only rows the whole decode accepts. A reader trusts an epoch of [`VERIFIED_EPOCH`] or later
	/// ([`Self::is_verified`]), so a release that finds an encoder that broke that promise
	/// withdraws the trust in every generation it minted by raising the epoch.
	#[stored(
		n = 9,
		ignorable,
		independent = "a build that skips it walks every row of the generation before an in-place read, which is what a generation with no verification gets"
	)]
	pub verified: Option<u32>,
}

/// The verification epoch a build stamps on the descriptor of a generation it mints, and the
/// oldest epoch a reader trusts ([`LayoutDescriptor::is_verified`]).
pub const VERIFIED_EPOCH: u32 = 1;

impl LayoutDescriptor {
	/// The descriptor of a generation whose rows are all in the document layout.
	pub fn document(table: TableName) -> Self {
		Self {
			table,
			layout: RowLayout::Document,
			migration: None,
			migrated: None,
			verified: None,
		}
	}

	/// Whether every row stamped with this generation is vouched for as one the whole decode
	/// accepts: the descriptor carries a verification epoch this build trusts.
	#[inline]
	pub fn is_verified(&self) -> bool {
		self.verified.is_some_and(|epoch| epoch >= VERIFIED_EPOCH)
	}

	/// This descriptor for a generation minted by `migration`.
	pub fn with_migration(mut self, migration: Migration) -> Self {
		self.migration = Some(migration);
		self
	}

	/// Whether `other` lays rows out as this descriptor does: the same table and the same layout,
	/// whatever migration or verification either carries. Compared by canonical bytes, so two
	/// layouts of the same meaning are the same layout.
	pub fn has_layout_of(&self, other: &Self) -> anyhow::Result<bool> {
		let bare = |descriptor: &Self| -> anyhow::Result<Vec<u8>> {
			if descriptor.migration.is_none()
				&& descriptor.migrated.is_none()
				&& descriptor.verified.is_none()
			{
				return descriptor.kv_canonical();
			}
			Self {
				table: descriptor.table.clone(),
				layout: descriptor.layout.clone(),
				migration: None,
				migrated: None,
				verified: None,
			}
			.kv_canonical()
		};
		Ok(bare(self)? == bare(other)?)
	}

	/// Checks the structural rules and the canonical form of the layout.
	///
	/// A packed descriptor that passes proves every presence bit, bool bit, fixed slot and var
	/// index it lists in bounds of its own sizes. Every refusal names the type and field number
	/// at fault. Decoding a descriptor runs this check, so a decoded descriptor always passes it.
	pub fn validate(&self) -> Result<(), SchemaError> {
		match &self.layout {
			RowLayout::Document => Ok(()),
			RowLayout::Packed(packed) => packed.validate(),
		}
	}
}

/// The refusal of field `n` of the stored type `T`.
pub(crate) fn out_of_range<T: StoredType>(n: u32) -> SchemaError {
	let ty = T::SCHEMA.name;
	SchemaError::OutOfRange {
		ty,
		n,
	}
}

/// A descriptor is written and read at the store's feature level, and refused when it does not
/// pass [`LayoutDescriptor::validate`], in either direction.
impl KVValue for LayoutDescriptor {
	type KeyContext = ();

	const LEVELLED: bool = true;

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		Err(Error::Entry(EntryError::Unlevelled {
			ty: <Self as StoredType>::SCHEMA.name,
		})
		.into())
	}

	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(encode_stored(self, ctx).map_err(Error::Entry)?)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}

/// The layout of the rows of a generation.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub enum RowLayout {
	/// Every row of the generation is in the document layout.
	#[stored(n = 1)]
	Document,
	/// Every row of the generation is packed as the descriptor says.
	#[stored(n = 2)]
	Packed(PackedDescriptor),
}

/// The columns of a packed layout, and the sizes of the regions of a row.
///
/// A packed row's body starts with `ceil(presence_bits / 8)` presence bytes, then
/// `ceil(bool_bits / 8)` bool bytes. Bit `i` is bit `i % 8` of byte `i / 8`, least significant
/// first, and the bits past the count in the last byte are zero. The fixed region follows from the
/// start of the body: `fixed_size` bytes in all, presence and bool bytes included. After it comes
/// the end-offset table of the `var_count` variable fields, whose entry width the row's header
/// gives. A row is at least `fixed_size + var_count * width` bytes.
///
/// The form is canonical: presence bits, bool bits, fixed slots and var indexes are assigned
/// densely in column order, with no padding between fixed slots.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct PackedDescriptor {
	/// The columns, strictly ascending by `(path, role)`. An object node sorts before every
	/// column under it.
	#[stored(n = 2)]
	pub columns: Vec<Column>,
	/// The number of presence bits.
	#[stored(n = 4)]
	pub presence_bits: u32,
	/// The number of bool bits.
	#[stored(n = 6)]
	pub bool_bits: u32,
	/// The size of the fixed region in bytes, presence and bool bytes included.
	#[stored(n = 8)]
	pub fixed_size: u32,
	/// The number of variable fields: the number of `Var` columns.
	#[stored(n = 10)]
	pub var_count: u32,
}

impl PackedDescriptor {
	/// Checks the structural rules and the canonical form of the descriptor.
	///
	/// 1. At most [`MAX_COLUMNS`] columns, strictly ascending by `(path, role)`.
	/// 2. Path segments are non-empty and a `Field` path is non-empty. A `Remainder` is a `Value`
	///    in a `Var` slot. An `Object` has no slot and the role `Field`.
	/// 3. Every proper prefix of a `Field` path, and the whole path of a non-root `Remainder`, is
	///    the path of an `Object` column.
	/// 4. A column's location agrees with its kind, `Fixed(n)` has `1 <= n <= 4096` and
	///    `LiteralCode(w)` has `w` of 1 or 2.
	/// 5. Presence bits are dense in column order, and `presence_bits` counts them.
	/// 6. Bool bits are dense in column order, and `bool_bits` counts them.
	/// 7. Fixed slots are dense from the end of the bool bytes, and `fixed_size` is the end of the
	///    last, at most [`MAX_FIXED_SIZE`].
	/// 8. Var indexes are dense in column order, and `var_count` counts them.
	/// 9. `literals` is on `LiteralCode` only, non-empty and within the code width. `tables` is on
	///    `RecordKey` only, non-empty, and at most 256 when more than one.
	/// 10. A path is at most `MAX_VALUE_DEPTH` segments long, no `Field` column is the root `id`, a
	///     `Remainder` has `Optional` presence, and the literals of a `LiteralCode` column are
	///     distinct scalars and the tables of a `RecordKey` column are distinct.
	///
	/// Rules 1 to 10 are permanent reader rules. A writer that relaxes one of them, or one of the
	/// row-level refusals of [`packed`](crate::packed), must add a descriptor field that raises the
	/// descriptor's read floor, so that a reader without the relaxation refuses the descriptor at
	/// admission instead of meeting it as a malformed one.
	///
	/// All arithmetic on sizes and offsets is checked `u64` arithmetic.
	pub fn validate(&self) -> Result<(), SchemaError> {
		if self.columns.len() > MAX_COLUMNS {
			return Err(out_of_range::<Self>(2));
		}
		let header = u64::from(self.presence_bits)
			.div_ceil(8)
			.checked_add(u64::from(self.bool_bits).div_ceil(8))
			.ok_or_else(|| out_of_range::<Self>(8))?;
		let max_fixed = u64::from(MAX_FIXED_SIZE);
		let mut presence_next = 0u64;
		let mut bool_next = 0u64;
		let mut fixed_end = header;
		let mut var_next = 0u64;
		// The object nodes that are prefixes of the current column's path, shortest first. The
		// stack is dense: the entry at index `i` is the node at depth `i + 1`.
		let mut nodes: Vec<&[String]> = Vec::new();
		let mut previous: Option<&Column> = None;
		for column in &self.columns {
			column.validate_shape()?;
			if let Some(previous) = previous
				&& (&previous.path, previous.role) >= (&column.path, column.role)
			{
				return Err(out_of_range::<Column>(2));
			}
			previous = Some(column);

			while let Some(&top) = nodes.last() {
				if column.path.starts_with(top) {
					break;
				}
				nodes.pop();
			}
			let needed = match column.role {
				ColumnRole::Field => column.path.len().saturating_sub(1),
				ColumnRole::Remainder => column.path.len(),
			};
			if nodes.len() != needed {
				return Err(out_of_range::<Column>(2));
			}
			if column.kind == PhysicalKind::Object {
				nodes.push(&column.path);
			}

			match column.presence {
				Presence::Required => {}
				Presence::Optional {
					bit,
				}
				| Presence::Nullable {
					bit,
				} => {
					if u64::from(bit) != presence_next {
						return Err(out_of_range::<Presence>(2));
					}
					presence_next =
						presence_next.checked_add(1).ok_or_else(|| out_of_range::<Presence>(2))?;
				}
				Presence::OptionalNullable {
					bit,
				} => {
					if u64::from(bit) != presence_next {
						return Err(out_of_range::<Presence>(2));
					}
					presence_next =
						presence_next.checked_add(2).ok_or_else(|| out_of_range::<Presence>(2))?;
				}
			}

			match column.location {
				Location::Bit(bit) => {
					if u64::from(bit) != bool_next {
						return Err(out_of_range::<Column>(8));
					}
					bool_next =
						bool_next.checked_add(1).ok_or_else(|| out_of_range::<Column>(8))?;
				}
				Location::Fixed(offset) => {
					let width =
						column.kind.fixed_width().ok_or_else(|| out_of_range::<Column>(8))?;
					if u64::from(offset) != fixed_end {
						return Err(out_of_range::<Column>(8));
					}
					fixed_end = fixed_end
						.checked_add(u64::from(width))
						.filter(|end| *end <= max_fixed)
						.ok_or_else(|| out_of_range::<Self>(8))?;
				}
				Location::Var(index) => {
					if u64::from(index) != var_next {
						return Err(out_of_range::<Column>(8));
					}
					var_next = var_next.checked_add(1).ok_or_else(|| out_of_range::<Column>(8))?;
				}
				Location::None => {}
			}
		}
		if presence_next != u64::from(self.presence_bits) {
			return Err(out_of_range::<Self>(4));
		}
		if bool_next != u64::from(self.bool_bits) {
			return Err(out_of_range::<Self>(6));
		}
		if fixed_end != u64::from(self.fixed_size) || fixed_end > max_fixed {
			return Err(out_of_range::<Self>(8));
		}
		if var_next != u64::from(self.var_count) {
			return Err(out_of_range::<Self>(10));
		}
		Ok(())
	}

	/// The index of the column at `path` with `role`, by binary search over the column order.
	///
	/// The columns of a validated descriptor are strictly ascending by `(path, role)`, so a path
	/// has at most one column per role. An unvalidated descriptor may answer `None` for a column it
	/// holds.
	pub fn column_at<S: AsRef<str>>(&self, path: &[S], role: ColumnRole) -> Option<usize> {
		self.columns
			.binary_search_by(|column| {
				column
					.path
					.iter()
					.map(String::as_str)
					.cmp(path.iter().map(AsRef::<str>::as_ref))
					.then_with(|| column.role.cmp(&role))
			})
			.ok()
	}
}

/// One column of a packed layout.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct Column {
	/// The path segments from the record root. A `Remainder` has the path of the open node it
	/// completes, and the record root's path is empty.
	#[stored(n = 2)]
	pub path: Vec<String>,
	/// The physical kind of the column's value.
	#[stored(n = 4)]
	pub kind: PhysicalKind,
	/// What a clear presence bit means, and which bits the column uses.
	#[stored(n = 6)]
	pub presence: Presence,
	/// Where the column's value is held.
	#[stored(n = 8)]
	pub location: Location,
	/// Whether the column holds the field at `path` or the remainder of an open object.
	#[stored(n = 10)]
	pub role: ColumnRole,
	/// For `LiteralCode` only: the literal union's values, in code order.
	#[stored(n = 12, default = "Vec::new")]
	pub literals: Vec<Value>,
	/// For `RecordKey` only: the tables a key may name. With more than one, the row stores a
	/// one-byte table code before the key.
	#[stored(n = 14, default = "Vec::new")]
	pub tables: Vec<TableName>,
}

impl Column {
	/// The rules that concern this column alone: segments, role, kind, the location kind agrees
	/// with, the literal and table lists, and the permanent reader rules on path depth, the root
	/// `id`, a remainder's presence, and distinct literals and tables. The rules that relate
	/// columns to each other are [`PackedDescriptor::validate`]'s. The permanent reader rules
	/// cannot be relaxed without raising the descriptor's read floor (see
	/// [`PackedDescriptor::validate`]).
	fn validate_shape(&self) -> Result<(), SchemaError> {
		if self.path.iter().any(String::is_empty) {
			return Err(out_of_range::<Self>(2));
		}
		if self.role == ColumnRole::Field && self.path.is_empty() {
			return Err(out_of_range::<Self>(2));
		}
		if let PhysicalKind::Fixed(width) = self.kind
			&& !(1..=MAX_FIXED_SLOT).contains(&width)
		{
			return Err(out_of_range::<Self>(4));
		} else if let PhysicalKind::LiteralCode(width) = self.kind
			&& !(1..=2).contains(&width)
		{
			return Err(out_of_range::<Self>(4));
		}
		if self.role == ColumnRole::Remainder && self.kind != PhysicalKind::Value {
			return Err(out_of_range::<Self>(4));
		}
		if self.kind == PhysicalKind::Object && self.role != ColumnRole::Field {
			return Err(out_of_range::<Self>(10));
		}
		let agrees = if self.kind == PhysicalKind::Bool {
			matches!(self.location, Location::Bit(_))
		} else if self.kind == PhysicalKind::Object {
			matches!(self.location, Location::None)
		} else if self.kind.is_variable() {
			matches!(self.location, Location::Var(_))
		} else {
			matches!(self.location, Location::Fixed(_))
		};
		if !agrees {
			return Err(out_of_range::<Self>(8));
		}
		if let PhysicalKind::LiteralCode(width) = self.kind {
			// `width` is 1 or 2, so the capacity is 256 or 65536.
			let capacity = 1usize << (8 * width);
			if self.literals.is_empty() || self.literals.len() > capacity {
				return Err(out_of_range::<Self>(12));
			}
		} else if !self.literals.is_empty() {
			return Err(out_of_range::<Self>(12));
		}
		if self.kind == PhysicalKind::RecordKey {
			if self.tables.is_empty() || self.tables.len() > MAX_KEY_TABLES {
				return Err(out_of_range::<Self>(14));
			}
		} else if !self.tables.is_empty() {
			return Err(out_of_range::<Self>(14));
		}
		if self.path.len() > MAX_PATH_DEPTH {
			return Err(out_of_range::<Self>(2));
		}
		if self.role == ColumnRole::Field && matches!(self.path.as_slice(), [only] if only == "id")
		{
			return Err(out_of_range::<Self>(2));
		}
		if self.role == ColumnRole::Remainder && !matches!(self.presence, Presence::Optional { .. })
		{
			return Err(out_of_range::<Self>(6));
		}
		if matches!(self.kind, PhysicalKind::LiteralCode(_))
			&& !self.literals_are_distinct_scalars()
		{
			return Err(out_of_range::<Self>(12));
		}
		if self.kind == PhysicalKind::RecordKey && !self.tables_are_distinct() {
			return Err(out_of_range::<Self>(14));
		}
		Ok(())
	}

	/// Whether no two of the column's tables are the same table. Allocates once per descriptor
	/// decode, never per row.
	fn tables_are_distinct(&self) -> bool {
		let mut names: Vec<&str> = self.tables.iter().map(TableName::as_str).collect();
		names.sort_unstable();
		names.windows(2).all(|pair| pair[0] != pair[1])
	}

	/// Whether every literal is a string, number, duration or bool, and no two have the same
	/// stored encoding.
	///
	/// The stored grammar gives equal scalars equal bytes and keeps float bits and decimal scale,
	/// so values with distinct encodings are distinct under `same_value`. Allocates once per
	/// descriptor decode, never per row.
	fn literals_are_distinct_scalars(&self) -> bool {
		let mut encodings: Vec<Vec<u8>> = Vec::with_capacity(self.literals.len());
		for literal in &self.literals {
			if !matches!(
				literal,
				Value::String(_) | Value::Number(_) | Value::Duration(_) | Value::Bool(_)
			) {
				return false;
			}
			match crate::val::stored::encode(literal) {
				Ok(bytes) => encodings.push(bytes),
				Err(_) => return false,
			}
		}
		encodings.sort_unstable();
		encodings.windows(2).all(|pair| pair[0] != pair[1])
	}
}

/// Whether a column is always present, and which bits say so when it is not.
///
/// A column's presence is meaningful only when every enclosing object node is present; when a node
/// is absent, every column under it has its presence bits clear.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub enum Presence {
	/// No presence bit: the value is present whenever the enclosing nodes are.
	#[stored(n = 1)]
	Required,
	/// A set bit means the value is in its location; a clear bit means NONE.
	#[stored(n = 2)]
	Optional {
		/// The index of the presence bit.
		#[stored(n = 2)]
		bit: u32,
	},
	/// A set bit means the value is present; a clear bit means NULL.
	#[stored(n = 3)]
	Nullable {
		/// The index of the presence bit.
		#[stored(n = 2)]
		bit: u32,
	},
	/// Two bits, `bit` and `bit + 1`. `bit` set means present. Both clear means NONE. Only
	/// `bit + 1` set means NULL. Both set is malformed.
	#[stored(n = 4)]
	OptionalNullable {
		/// The index of the first of the two presence bits.
		#[stored(n = 2)]
		bit: u32,
	},
}

/// Where a column's value is held in a packed row.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub enum Location {
	/// An index into the bool bits.
	#[stored(n = 1)]
	Bit(u32),
	/// A byte offset from the start of the body.
	#[stored(n = 2)]
	Fixed(u32),
	/// An index into the end-offset table of variable fields.
	#[stored(n = 3)]
	Var(u32),
	/// No slot: an object node.
	#[stored(n = 4)]
	None,
}

/// What a column holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Stored)]
#[stored(since = "4.0")]
pub enum ColumnRole {
	/// The field at the column's path; for an `Object` kind, the object node.
	#[stored(n = 1)]
	Field,
	/// The document of every key of the open node at the column's path that has no column of its
	/// own. An empty path is the record root.
	#[stored(n = 2)]
	Remainder,
}

/// The physical representation of a column's value. The variant number is its permanent code.
///
/// A kind added later carries its own `since`, so a descriptor that uses it needs the release that
/// defined it. [`PLANNED_PHYSICAL_KINDS`] lists the codes held for kinds not defined yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub enum PhysicalKind {
	/// One bit of the bool region.
	#[stored(n = 1)]
	Bool,
	/// A signed integer of 1 byte.
	#[stored(n = 2)]
	I8,
	/// A signed integer of 2 bytes.
	#[stored(n = 3)]
	I16,
	/// A signed integer of 4 bytes.
	#[stored(n = 4)]
	I32,
	/// A signed integer of 8 bytes.
	#[stored(n = 5)]
	I64,
	/// An unsigned integer of 1 byte.
	#[stored(n = 6)]
	U8,
	/// An unsigned integer of 2 bytes.
	#[stored(n = 7)]
	U16,
	/// An unsigned integer of 4 bytes.
	#[stored(n = 8)]
	U32,
	/// An unsigned integer of 8 bytes.
	#[stored(n = 9)]
	U64,
	/// A float of 2 bytes.
	#[stored(n = 10)]
	F16,
	/// A float of 4 bytes.
	#[stored(n = 12)]
	F32,
	/// A float of 8 bytes.
	#[stored(n = 13)]
	F64,
	/// A decimal of 16 bytes.
	#[stored(n = 14)]
	Decimal16,
	/// Bytes of exactly the given width, from 1 to 4096.
	#[stored(n = 15)]
	Fixed(u32),
	/// A uuid of 16 bytes.
	#[stored(n = 16)]
	Uuid,
	/// A datetime of 8 bytes: nanoseconds since the epoch, with an escape for values beyond them.
	#[stored(n = 17)]
	Datetime,
	/// A datetime of 12 bytes: whole seconds as an `i64`, then nanoseconds as a `u32`.
	#[stored(n = 18)]
	DatetimeWide,
	/// A duration of 8 bytes: nanoseconds, with an escape for values beyond them.
	#[stored(n = 19)]
	Duration,
	/// A geometry point of 16 bytes: two `f64`.
	#[stored(n = 21)]
	Point,
	/// An index of the given width, 1 or 2 bytes, into the column's `literals`.
	#[stored(n = 22)]
	LiteralCode(u32),
	/// A variable-width string.
	#[stored(n = 23)]
	String,
	/// Variable-width bytes.
	#[stored(n = 24)]
	Bytes,
	/// A variable-width record key of one of the column's `tables`.
	#[stored(n = 25)]
	RecordKey,
	/// A variable-width decimal.
	#[stored(n = 26)]
	DecimalVar,
	/// A variable-width value in the stored-value grammar.
	#[stored(n = 27)]
	Value,
	/// A variable-width `bigdecimal`: the payload of the stored grammar's BIGDECIMAL without its
	/// tag.
	#[stored(n = 30)]
	BigDecimal,
	/// An object node: no slot, and a presence that says whether the object is present.
	#[stored(n = 34)]
	Object,
}

impl PhysicalKind {
	/// The slot width in bytes of a fixed-width kind. `None` for `Bool` (a bit), `Object` (no slot)
	/// and the variable kinds.
	pub const fn fixed_width(self) -> Option<u32> {
		match self {
			Self::I8 | Self::U8 => Some(1),
			Self::I16 | Self::U16 | Self::F16 => Some(2),
			Self::I32 | Self::U32 | Self::F32 => Some(4),
			Self::I64 | Self::U64 | Self::F64 | Self::Datetime | Self::Duration => Some(8),
			Self::DatetimeWide => Some(12),
			Self::Decimal16 | Self::Uuid | Self::Point => Some(16),
			Self::Fixed(width) | Self::LiteralCode(width) => Some(width),
			Self::Bool
			| Self::String
			| Self::Bytes
			| Self::RecordKey
			| Self::DecimalVar
			| Self::BigDecimal
			| Self::Value
			| Self::Object => None,
		}
	}

	/// Whether the kind is held in a variable-width slot.
	pub const fn is_variable(self) -> bool {
		match self {
			Self::String
			| Self::Bytes
			| Self::RecordKey
			| Self::DecimalVar
			| Self::BigDecimal
			| Self::Value => true,
			Self::Bool
			| Self::I8
			| Self::I16
			| Self::I32
			| Self::I64
			| Self::U8
			| Self::U16
			| Self::U32
			| Self::U64
			| Self::F16
			| Self::F32
			| Self::F64
			| Self::Decimal16
			| Self::Fixed(_)
			| Self::Uuid
			| Self::Datetime
			| Self::DatetimeWide
			| Self::Duration
			| Self::Point
			| Self::LiteralCode(_)
			| Self::Object => false,
		}
	}
}

/// Why a generation's descriptor is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LayoutRefusal {
	/// A descriptor of a newer release, a malformed one, or one that does not pass
	/// [`LayoutDescriptor::validate`].
	#[error("{0}")]
	Entry(EntryError),
	/// A readable descriptor that names another table.
	#[error("its layout names table '{named}'")]
	NamesTable {
		named: TableName,
	},
}

/// The layout of one generation, as a build reads it.
#[derive(Clone, Debug)]
pub enum GenerationLayout {
	/// A descriptor this build reads.
	Readable(Arc<LayoutDescriptor>),
	/// A descriptor this build refuses.
	Refused(LayoutRefusal),
}

/// The layout of the generation whose `!lg` value is `bytes`, as a build reads it for `table`.
///
/// The descriptor is readable when it decodes, which runs [`LayoutDescriptor::validate`], and
/// names `table`. A descriptor that fails to decode is refused with its entry error, and one that
/// names another table with [`LayoutRefusal::NamesTable`]. Every failure of the bytes is a refusal
/// of that generation alone: none is an error of the caller.
pub fn decode_generation(table: &TableName, bytes: &[u8]) -> GenerationLayout {
	let descriptor = match decode_entry::<LayoutDescriptor>(bytes) {
		Ok(descriptor) => descriptor,
		Err(refusal) => return GenerationLayout::Refused(LayoutRefusal::Entry(refusal)),
	};
	if descriptor.table == *table {
		GenerationLayout::Readable(Arc::new(descriptor))
	} else {
		GenerationLayout::Refused(LayoutRefusal::NamesTable {
			named: descriptor.table,
		})
	}
}

/// One listed generation's descriptor: decoded when its layouts were built, or held as its
/// stored bytes and decoded, once, the first time it is read.
///
/// The decode of a `Stored` entry runs under the cell's initialisation, so a second reader that
/// names the same undecoded generation waits for the first one's decode and then reads its
/// result. The wait is bounded by one descriptor decode and never spans an `.await`.
#[derive(Debug)]
enum GenerationEntry {
	Decoded(GenerationLayout),
	Stored {
		bytes: Box<[u8]>,
		layout: OnceLock<GenerationLayout>,
	},
}

impl GenerationEntry {
	/// The entry's layout for `table`, decoding it first when it is held as stored bytes.
	#[inline]
	fn layout(&self, table: &TableName) -> &GenerationLayout {
		match self {
			Self::Decoded(layout) => layout,
			Self::Stored {
				bytes,
				layout,
			} => layout.get_or_init(|| decode_generation(table, bytes)),
		}
	}

	/// Whether the entry's descriptor has been decoded.
	fn is_decoded(&self) -> bool {
		match self {
			Self::Decoded(_) => true,
			Self::Stored {
				layout,
				..
			} => layout.get().is_some(),
		}
	}
}

/// A refusal to read or write a record because of its generation's layout.
///
/// Messages name only tables and generation numbers, never record data.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecordLayoutError {
	/// The row names a generation the table's layouts do not list.
	#[error(
		"A record of table '{table}' names generation {generation}, which the table's layouts do not list"
	)]
	UnknownGeneration {
		table: TableName,
		generation: Generation,
	},
	/// The row names a generation whose descriptor this build refuses.
	#[error(
		"A record of table '{table}' is in generation {generation}, whose layout this build refuses: {cause}"
	)]
	RefusedGeneration {
		table: TableName,
		generation: Generation,
		cause: LayoutRefusal,
	},
	/// A row of a nonzero generation was read without its table's layouts.
	#[error(
		"A stored record names generation {generation} but was read without its table's layouts"
	)]
	NoLayouts {
		generation: u64,
	},
	/// The layouts of one table were given for a read of another.
	#[error("The layouts of table '{layouts}' were given for a read of table '{table}'")]
	ForeignLayouts {
		layouts: TableName,
		table: TableName,
	},
	/// The table's current generation has a layout this build refuses, so it cannot be written.
	#[error(
		"Table '{table}' cannot be written: its current generation {generation} has a layout this build refuses: {cause}"
	)]
	CurrentRefused {
		table: TableName,
		generation: Generation,
		cause: LayoutRefusal,
	},
	/// A set of layouts was built with a generation outside its ring, or with generations out of
	/// ring order.
	#[error(
		"The layouts of table '{table}' list a generation outside the table's ring or out of ring order"
	)]
	Misnumbered {
		table: TableName,
	},
	/// The row names a generation above the highest a table can mint.
	#[error(
		"A record of table '{table}' names generation {generation}, above the highest generation a table can mint"
	)]
	GenerationOutOfRange {
		table: TableName,
		generation: u64,
	},
	/// The row names a generation outside the table's ring.
	#[error(
		"A record of table '{table}' names generation {generation}, outside the table's generations {base} to {head}"
	)]
	OutsideWindow {
		table: TableName,
		generation: Generation,
		base: Generation,
		head: Generation,
	},
	/// A packed row of a generation whose descriptor lays rows out as documents.
	#[error(
		"A record of table '{table}' is packed, but its generation {generation} lays rows out as documents"
	)]
	LayoutMismatch {
		table: TableName,
		generation: Generation,
	},
	/// A packed row that does not read as its generation's descriptor says.
	#[error(
		"A record of table '{table}' in generation {generation} is a malformed packed row: {cause}"
	)]
	MalformedRow {
		table: TableName,
		generation: Generation,
		cause: crate::packed::PackedError,
	},
	/// A packed format was given to a writer without its table's layouts.
	#[error(
		"A record cannot be written in the packed layout of generation {generation} without its table's layouts: rows of a packed table are written through the table's record writer"
	)]
	WriteWithoutLayouts {
		generation: Generation,
	},
	/// The table's ring cannot be read by this build, so none of its generations can be used.
	#[error("The generations of table '{table}' cannot be read by this build: {cause}")]
	RingRefused {
		table: TableName,
		cause: LayoutRefusal,
	},
	/// The table's current generation has no layout, so the table cannot be written.
	#[error("Table '{table}' cannot be written: its current generation {generation} has no layout")]
	CurrentMissing {
		table: TableName,
		generation: Generation,
	},
}

/// The layouts of one table, as one snapshot of its ring and descriptor keys lists them.
///
/// The ring says which generations the table holds and which is current. The listed generations
/// are the ring's descriptors that exist, in ring order from its base, each at a strictly higher
/// position than the one before. A table without a ring lists nothing, and a table whose ring this
/// build refuses is [refused](Self::refused): it lists nothing and answers every row, every write
/// format and every mint with the ring's refusal. A row of stamp 0 needs no descriptor and never
/// reaches these layouts; a stamp that is not a generation is refused by [`Self::stamp_refusal`].
///
/// A listed generation's descriptor is decoded when the layouts are built, or the first time a
/// row, a lookup or an iteration reads it ([`Self::from_stored`]). So the work of reading a table's
/// layouts is the newest listed descriptor plus the ones rows name.
#[derive(Debug)]
pub struct TableLayouts {
	ns: NamespaceId,
	db: DatabaseId,
	table: TableName,
	ring: GenerationRing,
	/// Why the ring cannot be read, when it cannot. The ring is then empty.
	ring_refusal: Option<LayoutRefusal>,
	/// The listed generations in ring order from the ring's base; each lies in the ring. Entries
	/// are shared between a set of layouts and the sets minted from it, so a descriptor is decoded
	/// once for all of them.
	generations: Box<[(Generation, Arc<GenerationEntry>)]>,
	damaged_keys: u32,
}

impl TableLayouts {
	/// The layouts of `table` as the given generations list them under `ring`, with the count of
	/// keys under the table's descriptor prefix that named no generation of the ring.
	///
	/// Refuses a generation outside the ring, a repeat and descending ring order, and any
	/// generation under an empty ring: that is a caller's mistake, never stored content, because a
	/// loader counts a key that lies outside the ring or does not decode in `damaged_keys` instead
	/// of listing it.
	pub fn new(
		ns: NamespaceId,
		db: DatabaseId,
		table: TableName,
		ring: GenerationRing,
		generations: Vec<(Generation, GenerationLayout)>,
		damaged_keys: u32,
	) -> Result<Self, RecordLayoutError> {
		check_ring_order(&table, ring, generations.iter().map(|(generation, _)| *generation))?;
		let generations: Vec<(Generation, Arc<GenerationEntry>)> = generations
			.into_iter()
			.map(|(generation, layout)| (generation, Arc::new(GenerationEntry::Decoded(layout))))
			.collect();
		Ok(Self {
			ns,
			db,
			table,
			ring,
			ring_refusal: None,
			generations: generations.into_boxed_slice(),
			damaged_keys,
		})
	}

	/// The layouts of `table` from its `!lg` values, in ring order under `ring`: the last listed
	/// generation's descriptor is decoded here, and every other one is decoded the first time a
	/// row names it.
	///
	/// The last listed generation is the one [`Self::admit`] compares a row's generation with
	/// first, and is the head's whenever the head is listed. It is the newest readable generation
	/// when the head is not listed. Whatever the ring holds, one descriptor is decoded here. The
	/// result is the layouts [`Self::new`] builds from the same generations, with the same
	/// refusals: a generation outside the ring, a repeat, descending ring order, and any generation
	/// under an empty ring.
	pub fn from_stored(
		ns: NamespaceId,
		db: DatabaseId,
		table: TableName,
		ring: GenerationRing,
		generations: Vec<(Generation, Box<[u8]>)>,
		damaged_keys: u32,
	) -> Result<Self, RecordLayoutError> {
		check_ring_order(&table, ring, generations.iter().map(|(generation, _)| *generation))?;
		let count = generations.len();
		let entries: Vec<(Generation, Arc<GenerationEntry>)> = generations
			.into_iter()
			.enumerate()
			.map(|(at, (generation, bytes))| {
				let entry = if at.saturating_add(1) == count {
					GenerationEntry::Decoded(decode_generation(&table, &bytes))
				} else {
					GenerationEntry::Stored {
						bytes,
						layout: OnceLock::new(),
					}
				};
				(generation, Arc::new(entry))
			})
			.collect();
		Ok(Self {
			ns,
			db,
			table,
			ring,
			ring_refusal: None,
			generations: entries.into_boxed_slice(),
			damaged_keys,
		})
	}

	/// How many listed generations have a decoded descriptor, for tests and diagnostics.
	pub fn decoded_generations(&self) -> usize {
		self.generations.iter().filter(|(_, entry)| entry.is_decoded()).count()
	}

	/// The layouts of a table whose ring this build cannot read, for `cause`.
	pub fn refused(
		ns: NamespaceId,
		db: DatabaseId,
		table: TableName,
		cause: LayoutRefusal,
		damaged_keys: u32,
	) -> Self {
		Self {
			ns,
			db,
			table,
			ring: GenerationRing::NONE,
			ring_refusal: Some(cause),
			generations: Box::default(),
			damaged_keys,
		}
	}

	/// The layouts of a table with no ring, and so no generation.
	pub fn empty(ns: NamespaceId, db: DatabaseId, table: TableName) -> Self {
		Self {
			ns,
			db,
			table,
			ring: GenerationRing::NONE,
			ring_refusal: None,
			generations: Box::default(),
			damaged_keys: 0,
		}
	}

	/// The table's namespace.
	pub fn namespace(&self) -> NamespaceId {
		self.ns
	}

	/// The table's database.
	pub fn database(&self) -> DatabaseId {
		self.db
	}

	/// The table.
	pub fn table(&self) -> &TableName {
		&self.table
	}

	/// Whether these are the layouts of the table `table` of database `db` in namespace `ns`.
	pub fn belongs_to(&self, ns: NamespaceId, db: DatabaseId, table: &TableName) -> bool {
		self.ns == ns && self.db == db && self.table == *table
	}

	/// The keys under the table's descriptor prefix that named no generation of the ring: counted,
	/// never listed.
	pub fn damaged_keys(&self) -> u32 {
		self.damaged_keys
	}

	/// The table's generation ring: [`GenerationRing::NONE`] for a table with no ring and for one
	/// whose ring is refused.
	pub fn ring(&self) -> GenerationRing {
		self.ring
	}

	/// Why the table's ring cannot be read, when it cannot.
	pub fn ring_refusal(&self) -> Option<&LayoutRefusal> {
		self.ring_refusal.as_ref()
	}

	/// Whether the table has a ring, or has one this build refuses: a table that is written in the
	/// v1 envelope.
	pub fn has_generation(&self) -> bool {
		!self.ring.is_empty() || self.ring_refusal.is_some()
	}

	/// The newest generation of the ring, or `None` when the table has none or its ring is refused.
	pub fn head(&self) -> Option<Generation> {
		self.ring.head()
	}

	/// The generation at the head and its layout, when the table lists it.
	pub fn current(&self) -> Option<(Generation, &GenerationLayout)> {
		match self.generations.last() {
			Some((generation, entry)) if Some(*generation) == self.ring.head() => {
				Some((*generation, entry.layout(&self.table)))
			}
			_ => None,
		}
	}

	/// The layout of `generation`, when the table lists it. A descriptor not yet decoded is decoded
	/// by the call.
	pub fn get(&self, generation: Generation) -> Option<&GenerationLayout> {
		let position = self.ring.position(generation)?;
		self.generations
			.binary_search_by_key(&position, |(listed, _)| {
				self.ring.position(*listed).unwrap_or(u16::MAX)
			})
			.ok()
			.and_then(|at| self.generations.get(at))
			.map(|(_, entry)| entry.layout(&self.table))
	}

	/// The listed generations, in ring order from the ring's base. The iterator decodes each
	/// descriptor it yields that is not decoded yet.
	pub fn generations(&self) -> impl Iterator<Item = (Generation, &GenerationLayout)> {
		self.generations.iter().map(|(generation, entry)| (*generation, entry.layout(&self.table)))
	}

	/// The migrations a row stamped `stamp` has not passed through, oldest first, each with the
	/// generation that carries it: the migration of every generation after `stamp` in ring order,
	/// up to the head. `None` is a row with no generation, which is older than every generation
	/// of the ring and passes through them all.
	///
	/// The listed generations are held in ring order, so the walk follows the ring across the
	/// wrap of the generation number rather than the numeric order.
	///
	/// Every generation after `stamp` must be listed with a readable descriptor, because a gap
	/// could hide a migration the row has not passed through: a refused descriptor answers
	/// [`RecordLayoutError::RefusedGeneration`] and an unlisted generation answers
	/// [`RecordLayoutError::UnknownGeneration`]. A stamp the ring does not hold is refused as
	/// [`Self::admit`] refuses it.
	pub fn pending_migrations(
		&self,
		stamp: Option<Generation>,
	) -> Result<Vec<(Generation, &Migration)>, RecordLayoutError> {
		let after = match stamp {
			None => None,
			Some(stamp) => {
				self.admit(stamp)?;
				self.ring.position(stamp)
			}
		};
		let (Some(base), Some(head)) = (self.ring.base(), self.ring.head()) else {
			return Ok(Vec::new());
		};
		let last = base.distance_to(head);
		let mut next = after.map_or(0, |after| after + 1);
		let mut out = Vec::new();
		for (generation, entry) in self.generations.iter() {
			let Some(position) = self.ring.position(*generation) else {
				continue;
			};
			if position < next {
				continue;
			}
			if position != next {
				return Err(self.unlisted(base, next));
			}
			match entry.layout(&self.table) {
				GenerationLayout::Readable(descriptor) => {
					if let Some(migration) = &descriptor.migration {
						out.push((*generation, migration));
					}
				}
				GenerationLayout::Refused(cause) => {
					return Err(RecordLayoutError::RefusedGeneration {
						table: self.table.clone(),
						generation: *generation,
						cause: cause.clone(),
					});
				}
			}
			next = position.saturating_add(1);
		}
		if next <= last {
			// The ring holds generations past the last listed one.
			return Err(self.unlisted(base, next));
		}
		Ok(out)
	}

	/// The newest generation of the ring whose descriptor may carry a migration, `None` when no
	/// generation does or the table has no ring. A row stamped at or after it has passed through
	/// every migration of the table, so no row needs a step unless it is stamped before it.
	///
	/// It is read from the head's descriptor ([`LayoutDescriptor::migrated`]) with no other
	/// descriptor decoded; the head is the last generation listed, since they are held in ring
	/// order. A head the layouts do not list, or list as refused, may carry a
	/// migration this build cannot see, so the head itself answers. A generation the ring has
	/// retired is not one.
	pub fn newest_migration(&self) -> Option<Generation> {
		let head = self.ring.head()?;
		match self.generations.last() {
			Some((generation, entry)) if *generation == head => match entry.layout(&self.table) {
				GenerationLayout::Readable(descriptor) => {
					descriptor.migrated.filter(|migrated| self.ring.contains(*migrated))
				}
				GenerationLayout::Refused(_) => Some(head),
			},
			_ => Some(head),
		}
	}

	/// Whether a row stamped `stamp` has migration steps to pass through: `stamp` is before
	/// [`Self::newest_migration`] in the ring, or names no position of it, in which case
	/// [`Self::pending_migrations`] refuses it. A row with no generation is before every
	/// generation.
	pub fn behind_migration(&self, stamp: Option<Generation>) -> bool {
		let Some(newest) = self.newest_migration() else {
			return false;
		};
		let Some(stamp) = stamp else {
			return true;
		};
		match (self.ring.position(stamp), self.ring.position(newest)) {
			(Some(at), Some(newest)) => at < newest,
			_ => true,
		}
	}

	/// The refusal of the generation `distance` positions after `base`, which the ring holds and no
	/// descriptor is listed for.
	fn unlisted(&self, base: Generation, distance: u16) -> RecordLayoutError {
		let mut generation = base;
		for _ in 0..distance {
			generation = generation.next();
		}
		RecordLayoutError::UnknownGeneration {
			table: self.table.clone(),
			generation,
		}
	}

	/// Admits a row of `generation`: the descriptor of a listed readable generation, and a typed
	/// refusal for every other generation.
	///
	/// The cases are decided in this order:
	///
	/// 1. The newest listed generation, when it is a readable one, is admitted.
	/// 2. A refused ring answers [`RecordLayoutError::RingRefused`].
	/// 3. A generation outside the ring answers [`RecordLayoutError::OutsideWindow`].
	/// 4. A generation inside the ring answers its descriptor,
	///    [`RecordLayoutError::RefusedGeneration`] when that descriptor is refused, and
	///    [`RecordLayoutError::UnknownGeneration`] when the ring holds the generation but no
	///    descriptor is listed for it. A table with no ring answers `UnknownGeneration` as well.
	#[inline]
	pub fn admit(
		&self,
		generation: Generation,
	) -> Result<&Arc<LayoutDescriptor>, RecordLayoutError> {
		if let Some((last, entry)) = self.generations.last()
			&& *last == generation
			&& let GenerationLayout::Readable(descriptor) = entry.layout(&self.table)
		{
			return Ok(descriptor);
		}
		self.lookup(generation)
	}

	/// The cases of [`Self::admit`] after its fast path.
	#[cold]
	fn lookup(&self, generation: Generation) -> Result<&Arc<LayoutDescriptor>, RecordLayoutError> {
		if let Some(cause) = &self.ring_refusal {
			return Err(RecordLayoutError::RingRefused {
				table: self.table.clone(),
				cause: cause.clone(),
			});
		}
		if let (Some(base), Some(head)) = (self.ring.base(), self.ring.head())
			&& !self.ring.contains(generation)
		{
			return Err(RecordLayoutError::OutsideWindow {
				table: self.table.clone(),
				generation,
				base,
				head,
			});
		}
		match self.get(generation) {
			Some(GenerationLayout::Readable(descriptor)) => Ok(descriptor),
			Some(GenerationLayout::Refused(cause)) => Err(RecordLayoutError::RefusedGeneration {
				table: self.table.clone(),
				generation,
				cause: cause.clone(),
			}),
			None => Err(RecordLayoutError::UnknownGeneration {
				table: self.table.clone(),
				generation,
			}),
		}
	}

	/// Whether the rows stamped `stamp` are vouched for as rows the whole decode accepts: `stamp`
	/// names a listed generation whose readable descriptor carries a verification epoch this build
	/// trusts ([`LayoutDescriptor::is_verified`]). Stamp 0 and every stamp the layouts do not list
	/// readable are not vouched for.
	///
	/// [`StoredRecord::admit_document`](crate::record::StoredRecord::admit_document) asks this for
	/// a document's stamp, and admits a vouched-for document without walking it. The newest listed
	/// generation, which most rows of a table carry, is answered without a search.
	#[inline]
	pub fn vouches_for(&self, stamp: u64) -> bool {
		let Ok(Some(generation)) = Generation::of_stamp(stamp) else {
			return false;
		};
		let layout = match self.generations.last() {
			Some((last, entry)) if *last == generation => Some(entry.layout(&self.table)),
			_ => self.get(generation),
		};
		matches!(layout, Some(GenerationLayout::Readable(descriptor)) if descriptor.is_verified())
	}

	/// These layouts as a reader that trusts no verification reads them: every listed generation's
	/// descriptor with its verification dropped, so that no generation is vouched for and every row
	/// is walked before it is read in place. For a reader that holds the walk to the whole decode
	/// over rows of any generation, such as a test that damages them.
	pub fn without_verification(&self) -> Self {
		let generations = self
			.generations
			.iter()
			.map(|(generation, entry)| {
				let layout = match entry.layout(&self.table) {
					GenerationLayout::Readable(descriptor) => {
						GenerationLayout::Readable(Arc::new(LayoutDescriptor {
							verified: None,
							..LayoutDescriptor::clone(descriptor)
						}))
					}
					refused => refused.clone(),
				};
				(*generation, Arc::new(GenerationEntry::Decoded(layout)))
			})
			.collect();
		Self {
			ns: self.ns,
			db: self.db,
			table: self.table.clone(),
			ring: self.ring,
			ring_refusal: self.ring_refusal.clone(),
			generations,
			damaged_keys: self.damaged_keys,
		}
	}

	/// [`Self::admit`] for a packed row: the descriptor of `generation` when it lays rows out as
	/// packed, and [`RecordLayoutError::LayoutMismatch`] when it lays them out as documents.
	pub fn admit_packed(
		&self,
		generation: Generation,
	) -> Result<&PackedDescriptor, RecordLayoutError> {
		match &self.admit(generation)?.layout {
			RowLayout::Packed(packed) => Ok(packed),
			RowLayout::Document => Err(RecordLayoutError::LayoutMismatch {
				table: self.table.clone(),
				generation,
			}),
		}
	}

	/// The refusal of a row whose stamp names no generation of the table: the ring's refusal when
	/// the ring is refused, and otherwise [`RecordLayoutError::GenerationOutOfRange`] for `stamp`.
	pub fn stamp_refusal(&self, stamp: u64) -> RecordLayoutError {
		match &self.ring_refusal {
			Some(cause) => RecordLayoutError::RingRefused {
				table: self.table.clone(),
				cause: cause.clone(),
			},
			None => RecordLayoutError::GenerationOutOfRange {
				table: self.table.clone(),
				generation: stamp,
			},
		}
	}

	/// The format a new row of the table is written in: the layout of the head of the ring's
	/// descriptor, or [`RecordFormat::Document`] without a generation when the table has no ring.
	///
	/// A readable head is written in its descriptor's layout: a document descriptor gives
	/// [`RecordFormat::Document`] and a packed one [`RecordFormat::Packed`]. A record a packed
	/// layout cannot hold is written by the encoder as a document at the same generation. A refused
	/// ring is [`RecordLayoutError::RingRefused`], a current generation this build refuses is
	/// [`RecordLayoutError::CurrentRefused`], and a head with no listed descriptor is
	/// [`RecordLayoutError::CurrentMissing`].
	pub fn write_format(&self) -> Result<RecordFormat, RecordLayoutError> {
		if let Some(cause) = &self.ring_refusal {
			return Err(RecordLayoutError::RingRefused {
				table: self.table.clone(),
				cause: cause.clone(),
			});
		}
		let Some(head) = self.ring.head() else {
			return Ok(RecordFormat::Document {
				generation: None,
			});
		};
		match self.current() {
			Some((generation, GenerationLayout::Readable(descriptor))) => {
				Ok(match descriptor.layout {
					RowLayout::Document => RecordFormat::Document {
						generation: Some(generation),
					},
					RowLayout::Packed(_) => RecordFormat::Packed {
						generation,
					},
				})
			}
			Some((generation, GenerationLayout::Refused(cause))) => {
				Err(RecordLayoutError::CurrentRefused {
					table: self.table.clone(),
					generation,
					cause: cause.clone(),
				})
			}
			None => Err(RecordLayoutError::CurrentMissing {
				table: self.table.clone(),
				generation: head,
			}),
		}
	}

	/// These layouts after one mint: `ring` is the ring the mint wrote and `descriptor` the
	/// descriptor of its head.
	///
	/// The listed generations are shared with these layouts, so a descriptor decoded through
	/// either set is decoded for both, and the head's is the new last entry, already decoded.
	///
	/// Refuses, as [`RecordLayoutError::Misnumbered`], a refused ring and a ring with no head. A
	/// first mint (these layouts have no ring) needs a ring of one generation. Any other mint needs
	/// the ring these layouts hold advanced by one: the same base and floor, a head one generation
	/// later, and no full ring.
	pub fn with_minted(
		&self,
		ring: GenerationRing,
		descriptor: Arc<LayoutDescriptor>,
	) -> Result<Self, RecordLayoutError> {
		let misnumbered = || RecordLayoutError::Misnumbered {
			table: self.table.clone(),
		};
		let (Some(base), Some(head)) = (ring.base(), ring.head()) else {
			return Err(misnumbered());
		};
		let follows = if self.ring_refusal.is_some() {
			false
		} else if self.ring.is_empty() {
			base == head
		} else {
			Some(base) == self.ring.base()
				&& ring.floor() == self.ring.floor()
				&& !self.ring.is_full()
				&& Some(head) == self.ring.head().map(Generation::next)
		};
		if !follows {
			return Err(misnumbered());
		}
		let mut generations = Vec::with_capacity(self.generations.len().saturating_add(1));
		generations.extend(self.generations.iter().cloned());
		generations.push((
			head,
			Arc::new(GenerationEntry::Decoded(GenerationLayout::Readable(descriptor))),
		));
		Ok(Self {
			ns: self.ns,
			db: self.db,
			table: self.table.clone(),
			ring,
			ring_refusal: None,
			generations: generations.into_boxed_slice(),
			damaged_keys: self.damaged_keys,
		})
	}
}

/// Checks that `listed` are generations of `ring` in strictly ascending ring order, as the layouts
/// of `table` require.
fn check_ring_order(
	table: &TableName,
	ring: GenerationRing,
	listed: impl Iterator<Item = Generation>,
) -> Result<(), RecordLayoutError> {
	let mut previous: Option<u16> = None;
	for generation in listed {
		match ring.position(generation) {
			Some(position) if previous.is_none_or(|before| position > before) => {
				previous = Some(position);
			}
			_ => {
				return Err(RecordLayoutError::Misnumbered {
					table: table.clone(),
				});
			}
		}
	}
	Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
	use surrealdb_kvs::Release;
	use surrealdb_strand::Strand;

	use super::*;
	use crate::IdiomText;
	use crate::generation::tests::{check_frozen, g, refusal, with_floor};
	use crate::migration::MigrationStep;
	use crate::val::Array;

	fn t() -> TableName {
		TableName::new("t")
	}

	fn string(text: &str) -> Value {
		Value::String(Strand::new(text))
	}

	fn column(path: &[&str], kind: PhysicalKind, presence: Presence, location: Location) -> Column {
		Column {
			path: path.iter().map(|segment| (*segment).to_owned()).collect(),
			kind,
			presence,
			location,
			role: ColumnRole::Field,
			literals: Vec::new(),
			tables: Vec::new(),
		}
	}

	/// A canonical packed descriptor with a root remainder, a bool, an object node with a remainder
	/// and a child, a nullable fixed column, a key column and a literal column.
	pub(crate) fn packed() -> PackedDescriptor {
		let mut root = column(
			&[],
			PhysicalKind::Value,
			Presence::Optional {
				bit: 0,
			},
			Location::Var(0),
		);
		root.role = ColumnRole::Remainder;
		let mut rest = column(
			&["address"],
			PhysicalKind::Value,
			Presence::Optional {
				bit: 2,
			},
			Location::Var(1),
		);
		rest.role = ColumnRole::Remainder;
		let mut owner =
			column(&["owner"], PhysicalKind::RecordKey, Presence::Required, Location::Var(3));
		owner.tables = vec![TableName::new("user"), TableName::new("org")];
		let mut status = column(
			&["status"],
			PhysicalKind::LiteralCode(1),
			Presence::OptionalNullable {
				bit: 4,
			},
			Location::Fixed(10),
		);
		status.literals = vec![string("a"), string("b")];
		PackedDescriptor {
			columns: vec![
				root,
				column(&["active"], PhysicalKind::Bool, Presence::Required, Location::Bit(0)),
				column(
					&["address"],
					PhysicalKind::Object,
					Presence::Optional {
						bit: 1,
					},
					Location::None,
				),
				rest,
				column(
					&["address", "city"],
					PhysicalKind::String,
					Presence::Required,
					Location::Var(2),
				),
				column(
					&["age"],
					PhysicalKind::I64,
					Presence::Nullable {
						bit: 3,
					},
					Location::Fixed(2),
				),
				owner,
				status,
			],
			presence_bits: 6,
			bool_bits: 1,
			fixed_size: 11,
			var_count: 4,
		}
	}

	/// The refusal of the packed descriptor after `change`.
	fn validated(change: impl FnOnce(&mut PackedDescriptor)) -> Result<(), SchemaError> {
		let mut descriptor = packed();
		change(&mut descriptor);
		descriptor.validate()
	}

	fn of_column(n: u32) -> Result<(), SchemaError> {
		Err(SchemaError::OutOfRange {
			ty: <Column as StoredType>::SCHEMA.name,
			n,
		})
	}

	fn of_packed(n: u32) -> Result<(), SchemaError> {
		Err(SchemaError::OutOfRange {
			ty: <PackedDescriptor as StoredType>::SCHEMA.name,
			n,
		})
	}

	fn of_presence(n: u32) -> Result<(), SchemaError> {
		Err(SchemaError::OutOfRange {
			ty: <Presence as StoredType>::SCHEMA.name,
			n,
		})
	}

	#[test]
	fn the_fixture_and_an_empty_packed_layout_are_valid() {
		assert_eq!(packed().validate(), Ok(()));
		let empty = PackedDescriptor {
			columns: Vec::new(),
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 0,
			var_count: 0,
		};
		assert_eq!(empty.validate(), Ok(()));
		assert_eq!(LayoutDescriptor::document(t()).validate(), Ok(()));
	}

	// Rule 1: at most MAX_COLUMNS columns, strictly ascending by (path, role).

	#[test]
	fn columns_out_of_order_or_repeated_are_refused() {
		assert_eq!(validated(|d| d.columns.swap(5, 6)), of_column(2));
		assert_eq!(validated(|d| d.columns.swap(2, 3)), of_column(2));
		assert_eq!(
			validated(|d| {
				let last = d.columns.last().cloned().expect("a column");
				d.columns.push(last);
			}),
			of_column(2)
		);
	}

	#[test]
	fn too_many_columns_are_refused() {
		let columns: Vec<Column> = (0..=MAX_COLUMNS)
			.map(|i| {
				column(
					&[&format!("c{i:06}")],
					PhysicalKind::Value,
					Presence::Required,
					Location::Var(i as u32),
				)
			})
			.collect();
		let descriptor = PackedDescriptor {
			var_count: columns.len() as u32,
			columns,
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 0,
		};
		assert_eq!(descriptor.validate(), of_packed(2));
	}

	// Rules 2 and 3: segments, roles, and the object nodes above a column.

	#[test]
	fn an_empty_segment_or_an_empty_field_path_is_refused() {
		assert_eq!(validated(|d| d.columns[4].path[1].clear()), of_column(2));
		assert_eq!(validated(|d| d.columns[0].role = ColumnRole::Field), of_column(2));
	}

	#[test]
	fn a_remainder_is_a_value_with_an_optional_presence() {
		assert_eq!(validated(|d| d.columns[3].kind = PhysicalKind::String), of_column(4));
		assert_eq!(
			validated(|d| d.columns[3].presence = Presence::Nullable {
				bit: 2
			}),
			of_column(6)
		);
		assert_eq!(validated(|d| d.columns[0].presence = Presence::Required), of_column(6));
	}

	#[test]
	fn a_column_under_no_object_node_is_refused() {
		// Without the node, the remainder and the child of `address` have no parent.
		assert_eq!(
			validated(|d| {
				d.columns.remove(2);
			}),
			of_column(2)
		);
		let orphan = PackedDescriptor {
			columns: vec![column(
				&["a", "b"],
				PhysicalKind::String,
				Presence::Required,
				Location::Var(0),
			)],
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 0,
			var_count: 1,
		};
		assert_eq!(orphan.validate(), of_column(2));
	}

	// Rule 4: location, fixed widths and literal code widths.

	#[test]
	fn a_location_that_disagrees_with_the_kind_is_refused() {
		assert_eq!(validated(|d| d.columns[1].location = Location::Fixed(2)), of_column(8));
		assert_eq!(validated(|d| d.columns[2].location = Location::Var(1)), of_column(8));
		assert_eq!(validated(|d| d.columns[4].location = Location::Fixed(2)), of_column(8));
		assert_eq!(validated(|d| d.columns[5].location = Location::Var(2)), of_column(8));
	}

	#[test]
	fn a_fixed_or_literal_width_out_of_range_is_refused() {
		let width = |kind| validated(move |d| d.columns[5].kind = kind);
		assert_eq!(width(PhysicalKind::Fixed(0)), of_column(4));
		assert_eq!(width(PhysicalKind::Fixed(MAX_FIXED_SLOT + 1)), of_column(4));
		assert_eq!(validated(|d| d.columns[7].kind = PhysicalKind::LiteralCode(0)), of_column(4));
		assert_eq!(validated(|d| d.columns[7].kind = PhysicalKind::LiteralCode(3)), of_column(4));
	}

	// Rules 5 to 8: presence bits, bool bits, fixed slots and var indexes are dense.

	#[test]
	fn presence_bits_out_of_place_or_miscounted_are_refused() {
		assert_eq!(
			validated(|d| d.columns[5].presence = Presence::Nullable {
				bit: 4
			}),
			of_presence(2)
		);
		assert_eq!(
			validated(|d| d.columns[2].presence = Presence::Optional {
				bit: 0
			}),
			of_presence(2)
		);
		assert_eq!(validated(|d| d.presence_bits = 7), of_packed(4));
		assert_eq!(validated(|d| d.presence_bits = 5), of_packed(4));
	}

	#[test]
	fn bool_bits_out_of_place_or_miscounted_are_refused() {
		assert_eq!(validated(|d| d.columns[1].location = Location::Bit(1)), of_column(8));
		assert_eq!(validated(|d| d.bool_bits = 2), of_packed(6));
	}

	#[test]
	fn fixed_slots_out_of_place_or_missized_are_refused() {
		assert_eq!(validated(|d| d.columns[5].location = Location::Fixed(3)), of_column(8));
		assert_eq!(validated(|d| d.columns[7].location = Location::Fixed(11)), of_column(8));
		assert_eq!(validated(|d| d.fixed_size = 12), of_packed(8));
	}

	#[test]
	fn a_fixed_region_above_its_limit_is_refused() {
		let slots = |count: u32| {
			let columns: Vec<Column> = (0..count)
				.map(|i| {
					column(
						&[&format!("c{i:05}")],
						PhysicalKind::Fixed(MAX_FIXED_SLOT),
						Presence::Required,
						Location::Fixed(i * MAX_FIXED_SLOT),
					)
				})
				.collect();
			PackedDescriptor {
				columns,
				presence_bits: 0,
				bool_bits: 0,
				fixed_size: count * MAX_FIXED_SLOT,
				var_count: 0,
			}
		};
		let at_limit = MAX_FIXED_SIZE / MAX_FIXED_SLOT;
		assert_eq!(slots(at_limit).validate(), Ok(()));
		assert_eq!(slots(at_limit + 1).validate(), of_packed(8));
	}

	#[test]
	fn var_indexes_out_of_place_or_miscounted_are_refused() {
		assert_eq!(validated(|d| d.columns[4].location = Location::Var(3)), of_column(8));
		assert_eq!(validated(|d| d.var_count = 5), of_packed(10));
		assert_eq!(validated(|d| d.var_count = 3), of_packed(10));
	}

	// Rules 9 and 10: literal and table lists, path depth, the root id.

	#[test]
	fn literals_belong_to_a_literal_column_and_are_distinct_scalars() {
		assert_eq!(validated(|d| d.columns[5].literals = vec![string("a")]), of_column(12));
		assert_eq!(validated(|d| d.columns[7].literals.clear()), of_column(12));
		assert_eq!(
			validated(|d| d.columns[7].literals = vec![string("a"), string("a")]),
			of_column(12)
		);
		assert_eq!(
			validated(
				|d| d.columns[7].literals = vec![string("a"), Value::Array(Array(Vec::new()))]
			),
			of_column(12)
		);
		assert_eq!(
			validated(
				|d| d.columns[7].literals = (0..=256).map(|i| string(&i.to_string())).collect()
			),
			of_column(12)
		);
	}

	#[test]
	fn tables_belong_to_a_key_column_and_are_distinct() {
		assert_eq!(validated(|d| d.columns[5].tables = vec![t()]), of_column(14));
		assert_eq!(validated(|d| d.columns[6].tables.clear()), of_column(14));
		assert_eq!(validated(|d| d.columns[6].tables = vec![t(), t()]), of_column(14));
		assert_eq!(
			validated(|d| {
				d.columns[6].tables =
					(0..=MAX_KEY_TABLES).map(|i| TableName::new(format!("t{i}"))).collect();
			}),
			of_column(14)
		);
	}

	#[test]
	fn a_path_too_deep_or_a_root_id_column_is_refused() {
		let deep: Vec<String> = (0..=MAX_PATH_DEPTH).map(|i| format!("p{i}")).collect();
		let deep: Vec<&str> = deep.iter().map(String::as_str).collect();
		let too_deep = PackedDescriptor {
			columns: vec![column(&deep, PhysicalKind::Value, Presence::Required, Location::Var(0))],
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 0,
			var_count: 1,
		};
		assert_eq!(too_deep.validate(), of_column(2));
		let id = PackedDescriptor {
			columns: vec![column(
				&["id"],
				PhysicalKind::Value,
				Presence::Required,
				Location::Var(0),
			)],
			presence_bits: 0,
			bool_bits: 0,
			fixed_size: 0,
			var_count: 1,
		};
		assert_eq!(id.validate(), of_column(2));
	}

	#[test]
	fn every_kind_has_exactly_one_kind_of_slot() {
		let kinds = [
			PhysicalKind::Bool,
			PhysicalKind::I8,
			PhysicalKind::I16,
			PhysicalKind::I32,
			PhysicalKind::I64,
			PhysicalKind::U8,
			PhysicalKind::U16,
			PhysicalKind::U32,
			PhysicalKind::U64,
			PhysicalKind::F16,
			PhysicalKind::F32,
			PhysicalKind::F64,
			PhysicalKind::Decimal16,
			PhysicalKind::Fixed(3),
			PhysicalKind::Uuid,
			PhysicalKind::Datetime,
			PhysicalKind::DatetimeWide,
			PhysicalKind::Duration,
			PhysicalKind::Point,
			PhysicalKind::LiteralCode(2),
			PhysicalKind::String,
			PhysicalKind::Bytes,
			PhysicalKind::RecordKey,
			PhysicalKind::DecimalVar,
			PhysicalKind::BigDecimal,
			PhysicalKind::Value,
			PhysicalKind::Object,
		];
		for kind in kinds {
			let slots = [
				kind == PhysicalKind::Bool,
				kind == PhysicalKind::Object,
				kind.is_variable(),
				kind.fixed_width().is_some(),
			];
			assert_eq!(slots.iter().filter(|slot| **slot).count(), 1, "{kind:?}");
		}
		assert_eq!(PhysicalKind::Fixed(3).fixed_width(), Some(3));
		assert_eq!(PhysicalKind::LiteralCode(2).fixed_width(), Some(2));
		assert_eq!(PhysicalKind::DatetimeWide.fixed_width(), Some(12));
	}

	#[test]
	fn column_at_finds_a_column_by_path_and_role() {
		let descriptor = packed();
		let none: [&str; 0] = [];
		assert_eq!(descriptor.column_at(&none, ColumnRole::Remainder), Some(0));
		assert_eq!(descriptor.column_at(&none, ColumnRole::Field), None);
		assert_eq!(descriptor.column_at(&["address"], ColumnRole::Field), Some(2));
		assert_eq!(descriptor.column_at(&["address"], ColumnRole::Remainder), Some(3));
		assert_eq!(descriptor.column_at(&["address", "city"], ColumnRole::Field), Some(4));
		assert_eq!(descriptor.column_at(&["status"], ColumnRole::Field), Some(7));
		assert_eq!(descriptor.column_at(&["missing"], ColumnRole::Field), None);
	}

	// The stored form.

	fn packed_descriptor() -> LayoutDescriptor {
		LayoutDescriptor {
			table: t(),
			layout: RowLayout::Packed(packed()),
			migration: None,
			migrated: None,
			verified: None,
		}
	}

	#[test]
	fn a_document_descriptor_is_its_frozen_bytes() {
		check_frozen(
			&LayoutDescriptor::document(t()),
			&[254, 0, 128, 128, 16, 38, 2, 0, 3, 2, 7, 116, 4, 38, 1, 0, 1, 0],
		);
	}

	/// The bytes of [`packed_descriptor`].
	const FROZEN_PACKED: &[u8] = &[
		254, 0, 128, 128, 16, 39, 2, 0, 0, 0, 3, 0, 2, 7, 116, 4, 39, 1, 0, 0, 0, 2, 39, 5, 0, 0,
		0, 142, 1, 144, 1, 146, 1, 148, 1, 2, 21, 8, 0, 0, 0, 38, 0, 80, 0, 127, 0, 174, 0, 223, 0,
		10, 1, 66, 1, 38, 5, 0, 3, 9, 19, 25, 2, 20, 0, 4, 38, 1, 0, 27, 0, 6, 38, 1, 0, 2, 38, 1,
		0, 2, 128, 8, 38, 1, 0, 3, 128, 10, 38, 1, 0, 2, 0, 38, 5, 0, 11, 17, 23, 29, 2, 20, 1, 0,
		7, 97, 99, 116, 105, 118, 101, 4, 38, 1, 0, 1, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 1, 128,
		10, 38, 1, 0, 1, 0, 38, 5, 0, 12, 18, 28, 34, 2, 20, 1, 0, 7, 97, 100, 100, 114, 101, 115,
		115, 4, 38, 1, 0, 34, 0, 6, 38, 1, 0, 2, 38, 1, 0, 2, 129, 8, 38, 1, 0, 4, 0, 10, 38, 1, 0,
		1, 0, 38, 5, 0, 12, 18, 28, 34, 2, 20, 1, 0, 7, 97, 100, 100, 114, 101, 115, 115, 4, 38, 1,
		0, 27, 0, 6, 38, 1, 0, 2, 38, 1, 0, 2, 130, 8, 38, 1, 0, 3, 129, 10, 38, 1, 0, 2, 0, 38, 5,
		0, 18, 24, 30, 36, 2, 20, 2, 0, 8, 7, 97, 100, 100, 114, 101, 115, 115, 7, 99, 105, 116,
		121, 4, 38, 1, 0, 23, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 130, 10, 38, 1, 0, 1, 0, 38, 5,
		0, 8, 14, 24, 30, 2, 20, 1, 0, 7, 97, 103, 101, 4, 38, 1, 0, 5, 0, 6, 38, 1, 0, 3, 38, 1,
		0, 2, 131, 8, 38, 1, 0, 2, 130, 10, 38, 1, 0, 1, 0, 38, 6, 0, 10, 16, 22, 28, 34, 2, 20, 1,
		0, 7, 111, 119, 110, 101, 114, 4, 38, 1, 0, 25, 0, 6, 38, 1, 0, 1, 0, 8, 38, 1, 0, 3, 131,
		10, 38, 1, 0, 1, 0, 14, 20, 2, 0, 5, 7, 117, 115, 101, 114, 7, 111, 114, 103, 38, 6, 0, 11,
		17, 27, 33, 39, 2, 20, 1, 0, 7, 115, 116, 97, 116, 117, 115, 4, 38, 1, 0, 22, 129, 6, 38,
		1, 0, 4, 38, 1, 0, 2, 132, 8, 38, 1, 0, 2, 138, 10, 38, 1, 0, 1, 0, 12, 20, 2, 0, 2, 7, 97,
		7, 98, 4, 134, 6, 129, 8, 139, 10, 132,
	];

	#[test]
	fn a_packed_descriptor_is_its_frozen_bytes() {
		check_frozen(&packed_descriptor(), FROZEN_PACKED);
	}

	#[test]
	fn an_invalid_descriptor_is_refused_in_either_direction() {
		let mut descriptor = packed_descriptor();
		if let RowLayout::Packed(packed) = &mut descriptor.layout {
			packed.var_count = 9;
		}
		// The descriptor's entry names its own type, and the field refused is the packed one's.
		let expected = EntryError::body(
			<LayoutDescriptor as StoredType>::SCHEMA.name,
			SchemaError::OutOfRange {
				ty: <PackedDescriptor as StoredType>::SCHEMA.name,
				n: 10,
			},
		);
		assert_eq!(refusal(&descriptor.kv_canonical().unwrap_err()), expected);
		assert_eq!(
			refusal(&descriptor.kv_encode_value_at(&EncodeContext::CURRENT).unwrap_err()),
			expected
		);
		let bytes = canonical(&descriptor).expect("the grammar holds an invalid descriptor");
		assert_eq!(refusal(&LayoutDescriptor::kv_decode_value(&bytes, ()).unwrap_err()), expected);
		assert!(matches!(
			decode_generation(&t(), &bytes),
			GenerationLayout::Refused(LayoutRefusal::Entry(refused)) if refused == expected
		));
	}

	#[test]
	fn decode_generation_reads_its_own_table_and_refuses_everything_else() {
		let bytes = packed_descriptor().kv_canonical().expect("bytes");
		match decode_generation(&t(), &bytes) {
			GenerationLayout::Readable(descriptor) => assert_eq!(*descriptor, packed_descriptor()),
			GenerationLayout::Refused(cause) => panic!("refused: {cause}"),
		}
		assert!(matches!(
			decode_generation(&TableName::new("other"), &bytes),
			GenerationLayout::Refused(LayoutRefusal::NamesTable { named }) if named == t()
		));
		assert!(matches!(
			decode_generation(&t(), &bytes[..bytes.len() - 1]),
			GenerationLayout::Refused(LayoutRefusal::Entry(EntryError::Body { .. }))
		));
		assert!(matches!(
			decode_generation(&t(), &[]),
			GenerationLayout::Refused(LayoutRefusal::Entry(EntryError::NoHeader))
		));
		assert!(matches!(
			decode_generation(&t(), &[0x00, 0x01]),
			GenerationLayout::Refused(LayoutRefusal::Entry(EntryError::Marker(0x00)))
		));
		let later = Release::new(Release::CURRENT.major() + 1, 0);
		assert!(matches!(
			decode_generation(&t(), &with_floor(&bytes, later)),
			GenerationLayout::Refused(LayoutRefusal::Entry(EntryError::NeedsRelease { needed, .. }))
				if needed == later
		));
	}

	// The layouts of a table.

	fn ns() -> NamespaceId {
		NamespaceId(1)
	}

	fn db() -> DatabaseId {
		DatabaseId(1)
	}

	fn readable(layout: RowLayout) -> GenerationLayout {
		GenerationLayout::Readable(Arc::new(LayoutDescriptor {
			table: t(),
			layout,
			migration: None,
			migrated: None,
			verified: None,
		}))
	}

	fn refused() -> GenerationLayout {
		GenerationLayout::Refused(LayoutRefusal::NamesTable {
			named: TableName::new("other"),
		})
	}

	fn ring(base: u16, head: u16) -> GenerationRing {
		GenerationRing::new(g(base), g(head), None).expect("a ring")
	}

	fn layouts(ring: GenerationRing, generations: Vec<(u16, GenerationLayout)>) -> TableLayouts {
		let generations = generations.into_iter().map(|(n, layout)| (g(n), layout)).collect();
		TableLayouts::new(ns(), db(), t(), ring, generations, 0).expect("layouts")
	}

	fn document_at(verified: Option<u32>) -> GenerationLayout {
		GenerationLayout::Readable(Arc::new(LayoutDescriptor {
			verified,
			..LayoutDescriptor::document(t())
		}))
	}

	/// A document stamped `stamp` whose one string field holds bytes that are not UTF-8: the
	/// envelope parses, and the whole decode refuses the body.
	fn damaged_document(stamp: Option<u16>) -> Vec<u8> {
		use crate::record::{Record, RecordFormat, encode_whole_record};
		let data = crate::val::Object::from(std::collections::BTreeMap::from([(
			"a",
			Value::String(Strand::new("~~~~")),
		)]));
		let format = RecordFormat::Document {
			generation: stamp.map(g),
		};
		let mut bytes =
			encode_whole_record(format, &Record::new(Value::Object(data)), None).expect("encodes");
		let at = bytes.windows(4).position(|w| w == b"~~~~").expect("the string's bytes");
		bytes[at] = 0xFF;
		bytes
	}

	#[test]
	fn a_generation_is_vouched_for_only_by_a_trusted_epoch() {
		let table = layouts(
			ring(1, 3),
			vec![
				(1, document_at(Some(VERIFIED_EPOCH))),
				(2, document_at(Some(0))),
				(3, document_at(None)),
			],
		);
		assert!(table.vouches_for(1), "a trusted epoch vouches");
		assert!(!table.vouches_for(2), "an epoch below the trusted one does not");
		assert!(!table.vouches_for(3), "no epoch does not");
		assert!(!table.vouches_for(0), "stamp 0 has no descriptor to vouch");
		assert!(!table.vouches_for(4), "an unlisted generation is not vouched for");
		assert!(!table.vouches_for(u64::MAX), "a stamp that names no generation is not");
		let head = layouts(
			ring(1, 2),
			vec![(1, document_at(None)), (2, document_at(Some(VERIFIED_EPOCH)))],
		);
		assert!(head.vouches_for(2), "the head is answered on the fast path");
		assert!(!head.vouches_for(1), "an older generation is looked up");
		let refused = layouts(ring(1, 1), vec![(1, refused())]);
		assert!(!refused.vouches_for(1), "a refused descriptor vouches for nothing");
	}

	#[test]
	fn a_document_is_walked_unless_its_generation_is_verified() {
		use crate::record::StoredRecord;
		let verified = layouts(ring(1, 1), vec![(1, document_at(Some(VERIFIED_EPOCH)))]);
		let unverified = layouts(ring(1, 1), vec![(1, document_at(None))]);
		let bytes = damaged_document(Some(1));
		let record = StoredRecord::parse(&bytes).expect("parses").expect("a v1 record");
		// The walk refuses the damaged body: no layouts, or layouts that do not vouch.
		assert!(record.checked_document().is_err());
		assert!(record.admit_document(None).is_err());
		assert!(record.admit_document(Some(&unverified)).is_err());
		// A verified generation admits its document as it is.
		let document =
			record.admit_document(Some(&verified)).expect("admitted").expect("a document");
		assert!(document.is_verified());
		// Stamp 0 is never vouched for, whatever the layouts say of generation 1.
		let bytes = damaged_document(None);
		let record = StoredRecord::parse(&bytes).expect("parses").expect("a v1 record");
		assert!(record.admit_document(Some(&verified)).is_err());
		// A sound document is admitted either way, walked where it is not vouched for.
		let data = crate::val::Object::from(std::collections::BTreeMap::from([(
			"a",
			Value::String(Strand::new("~~~~")),
		)]));
		let sound = crate::record::encode_whole_record(
			crate::record::RecordFormat::Document {
				generation: Some(g(1)),
			},
			&crate::record::Record::new(Value::Object(data)),
			None,
		)
		.expect("encodes");
		let record = StoredRecord::parse(&sound).expect("parses").expect("a v1 record");
		let walked =
			record.admit_document(Some(&unverified)).expect("admitted").expect("a document");
		assert!(!walked.is_verified());
		assert_eq!(walked.bytes(), record.checked_document().unwrap().unwrap().bytes());
	}

	#[test]
	fn admit_answers_each_generation_in_its_documented_order() {
		let table = layouts(
			ring(65_534, 3),
			vec![
				(65_534, readable(RowLayout::Document)),
				(1, refused()),
				(3, readable(RowLayout::Packed(packed()))),
			],
		);
		assert!(matches!(
			table.admit(g(3)),
			Ok(descriptor) if descriptor.layout == RowLayout::Packed(packed())
		));
		assert!(matches!(
			table.admit(g(65_534)),
			Ok(descriptor) if descriptor.layout == RowLayout::Document
		));
		assert!(matches!(
			table.admit(g(1)),
			Err(RecordLayoutError::RefusedGeneration { generation, .. }) if generation == g(1)
		));
		assert!(matches!(
			table.admit(g(65_535)),
			Err(RecordLayoutError::UnknownGeneration { generation, .. }) if generation == g(65_535)
		));
		assert!(matches!(
			table.admit(g(4)),
			Err(RecordLayoutError::OutsideWindow { generation, base, head, .. })
				if generation == g(4) && base == g(65_534) && head == g(3)
		));
		assert!(matches!(
			table.stamp_refusal(65_536),
			RecordLayoutError::GenerationOutOfRange {
				generation: 65_536,
				..
			}
		));
		let ring_refused = TableLayouts::refused(ns(), db(), t(), refusal_cause(), 0);
		assert!(matches!(ring_refused.admit(g(1)), Err(RecordLayoutError::RingRefused { .. })));
		assert!(matches!(
			ring_refused.stamp_refusal(u64::MAX),
			RecordLayoutError::RingRefused { .. }
		));
		assert!(ring_refused.has_generation());
		assert!(!TableLayouts::empty(ns(), db(), t()).has_generation());
	}

	fn refusal_cause() -> LayoutRefusal {
		LayoutRefusal::Entry(EntryError::NoHeader)
	}

	#[test]
	fn layouts_list_generations_of_their_ring_in_ring_order() {
		let misnumbered = |ring, listed: Vec<u16>| {
			let generations =
				listed.into_iter().map(|n| (g(n), readable(RowLayout::Document))).collect();
			matches!(
				TableLayouts::new(ns(), db(), t(), ring, generations, 0),
				Err(RecordLayoutError::Misnumbered { .. })
			)
		};
		assert!(misnumbered(ring(5, 9), vec![4]));
		assert!(misnumbered(ring(5, 9), vec![7, 6]));
		assert!(misnumbered(ring(5, 9), vec![6, 6]));
		assert!(misnumbered(ring(65_000, 2), vec![1, 65_001]));
		assert!(misnumbered(GenerationRing::NONE, vec![1]));
		assert!(!misnumbered(ring(65_000, 2), vec![65_001, 1, 2]));
		let bytes = |n: u16| {
			(
				g(n),
				Box::<[u8]>::from(LayoutDescriptor::document(t()).kv_canonical().expect("bytes")),
			)
		};
		assert!(matches!(
			TableLayouts::from_stored(ns(), db(), t(), ring(5, 9), vec![bytes(7), bytes(6)], 0),
			Err(RecordLayoutError::Misnumbered { .. })
		));
	}

	/// A descriptor carrying the migration `name`, as a minted generation holds it.
	fn stepped(name: &str, migrated: Option<u16>) -> GenerationLayout {
		GenerationLayout::Readable(Arc::new(LayoutDescriptor {
			table: t(),
			layout: RowLayout::Document,
			migration: Some(Migration {
				name: name.to_owned(),
				step: MigrationStep::Remove {
					path: IdiomText::from_raw("a"),
				},
			}),
			migrated: migrated.map(g),
			verified: None,
		}))
	}

	#[test]
	fn migrations_are_read_in_ring_order_across_the_wrap() {
		// The ring is 65_534, 65_535, 1, 2: numeric order (1, 2, 65_534, 65_535) is not ring order,
		// and the listed generations are held in ring order, so the head is the last one listed.
		let table = layouts(
			ring(65_534, 2),
			vec![
				(65_534, readable(RowLayout::Document)),
				(65_535, stepped("first", Some(65_535))),
				(1, readable(RowLayout::Document)),
				(2, stepped("second", Some(2))),
			],
		);
		assert_eq!(table.newest_migration(), Some(g(2)));
		let pending = |stamp: Option<u16>| -> Vec<(u16, String)> {
			table
				.pending_migrations(stamp.map(g))
				.expect("every generation after the stamp is listed")
				.into_iter()
				.map(|(generation, migration)| (generation.get(), migration.name.clone()))
				.collect()
		};
		let first = (65_535, "first".to_owned());
		let second = (2, "second".to_owned());
		assert_eq!(pending(None), [first.clone(), second.clone()]);
		assert_eq!(pending(Some(65_534)), [first, second.clone()]);
		assert_eq!(pending(Some(65_535)), std::slice::from_ref(&second));
		assert_eq!(pending(Some(1)), std::slice::from_ref(&second));
		assert_eq!(pending(Some(2)), []);
		assert!(table.behind_migration(Some(g(65_534))));
		assert!(table.behind_migration(Some(g(1))));
		assert!(!table.behind_migration(Some(g(2))));
		// A generation the ring holds and no descriptor is listed for is refused, not skipped.
		let gap = layouts(
			ring(65_534, 2),
			vec![(65_534, readable(RowLayout::Document)), (2, stepped("second", Some(2)))],
		);
		assert!(matches!(
			gap.pending_migrations(Some(g(65_534))),
			Err(RecordLayoutError::UnknownGeneration { generation, .. }) if generation == g(65_535)
		));
	}

	#[test]
	fn stored_layouts_decode_the_last_generation_first_and_the_others_when_read() {
		let document = LayoutDescriptor::document(t()).kv_canonical().expect("bytes");
		let packed = packed_descriptor().kv_canonical().expect("bytes");
		let table = TableLayouts::from_stored(
			ns(),
			db(),
			t(),
			ring(2, 4),
			vec![(g(2), document.into()), (g(3), b"junk".to_vec().into()), (g(4), packed.into())],
			1,
		)
		.expect("layouts");
		assert_eq!(table.decoded_generations(), 1);
		assert_eq!(table.damaged_keys(), 1);
		assert!(table.admit(g(4)).is_ok());
		assert_eq!(table.decoded_generations(), 1);
		assert!(matches!(
			table.admit(g(2)),
			Ok(descriptor) if descriptor.layout == RowLayout::Document
		));
		assert_eq!(table.decoded_generations(), 2);
		assert!(matches!(
			table.admit(g(3)),
			Err(RecordLayoutError::RefusedGeneration { generation, .. }) if generation == g(3)
		));
		assert_eq!(table.decoded_generations(), 3);
		let listed: Vec<Generation> =
			table.generations().map(|(generation, _)| generation).collect();
		assert_eq!(listed, [g(2), g(3), g(4)]);
	}

	#[test]
	fn the_write_format_is_the_head_layout() {
		use crate::record::RecordFormat;
		let empty = TableLayouts::empty(ns(), db(), t());
		assert_eq!(
			empty.write_format(),
			Ok(RecordFormat::Document {
				generation: None
			})
		);
		let document = layouts(ring(1, 2), vec![(2, readable(RowLayout::Document))]);
		assert_eq!(
			document.write_format(),
			Ok(RecordFormat::Document {
				generation: Some(g(2))
			})
		);
		let packed = layouts(
			ring(1, 2),
			vec![(1, readable(RowLayout::Document)), (2, readable(RowLayout::Packed(packed())))],
		);
		assert_eq!(
			packed.write_format(),
			Ok(RecordFormat::Packed {
				generation: g(2)
			})
		);
		let head_refused = layouts(ring(1, 2), vec![(2, refused())]);
		assert!(matches!(
			head_refused.write_format(),
			Err(RecordLayoutError::CurrentRefused { generation, .. }) if generation == g(2)
		));
		let head_missing = layouts(ring(1, 2), vec![(1, readable(RowLayout::Document))]);
		assert!(matches!(
			head_missing.write_format(),
			Err(RecordLayoutError::CurrentMissing { generation, .. }) if generation == g(2)
		));
		assert!(head_missing.current().is_none());
		let ring_refused = TableLayouts::refused(ns(), db(), t(), refusal_cause(), 0);
		assert!(matches!(ring_refused.write_format(), Err(RecordLayoutError::RingRefused { .. })));
	}

	#[test]
	fn a_mint_advances_the_ring_by_one_generation() {
		let descriptor = Arc::new(LayoutDescriptor::document(t()));
		let empty = TableLayouts::empty(ns(), db(), t());
		let first = empty.with_minted(ring(1, 1), Arc::clone(&descriptor)).expect("a first mint");
		assert_eq!(first.head(), Some(g(1)));
		assert!(first.admit(g(1)).is_ok());
		assert!(matches!(
			empty.with_minted(ring(1, 2), Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));
		assert!(matches!(
			empty.with_minted(GenerationRing::NONE, Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));

		let second = first.with_minted(ring(1, 2), Arc::clone(&descriptor)).expect("a second mint");
		assert_eq!(
			second.generations().map(|(generation, _)| generation).collect::<Vec<_>>(),
			[g(1), g(2)]
		);
		assert!(matches!(
			first.with_minted(ring(1, 3), Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));
		assert!(matches!(
			first.with_minted(ring(2, 2), Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));
		let floored = GenerationRing::new(g(1), g(2), Some(g(1))).expect("a ring");
		assert!(matches!(
			first.with_minted(floored, Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));

		let full = layouts(ring(2, 1), vec![(1, readable(RowLayout::Document))]);
		assert!(matches!(
			full.with_minted(ring(2, 2), Arc::clone(&descriptor)),
			Err(RecordLayoutError::Misnumbered { .. })
		));
		let ring_refused = TableLayouts::refused(ns(), db(), t(), refusal_cause(), 0);
		assert!(matches!(
			ring_refused.with_minted(ring(1, 1), descriptor),
			Err(RecordLayoutError::Misnumbered { .. })
		));
	}
}
