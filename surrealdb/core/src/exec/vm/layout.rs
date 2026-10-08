//! A strict row layout, as `doc/executor/RFC_RECORD_LAYOUT_AND_MIGRATE.md`
//! §3.1 describes it: every field of a table's layout at an offset the layout
//! assigns, typed by the schema, so a reader finds a field without parsing.
//!
//! ```text
//! header      generation (u16 LE) · flags (u8) · reserved (u8)
//! null map    two bits per field: bit 0 present, bit 1 null
//! fixed       every field's slot, widest first, 8-aligned from the header
//! var region  the bytes of variable-width values that do not fit their slot
//! ```
//!
//! A field that is absent is `NONE`, one present and `NULL` is `NULL`, and
//! only a present, non-null field's slot is meaningful. A string's slot takes
//! one of the [`StringEncoding`]s, which decide how often a read or a
//! comparison leaves the slot for the variable region.
//!
//! Nothing here is persisted: the layout is measured against the `revision`
//! encoding before a byte of it reaches storage.

use std::cmp::Ordering;

use anyhow::{Context, bail};

use super::row::{FieldName, RowReader};
use super::scalar::Scalar;
use crate::val::{Number, Object, Value};

/// The kind a column of a strict layout holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColumnKind {
	Int,
	Float,
	Bool,
	String,
}

/// How a string column's slot holds its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StringEncoding {
	/// An offset and a length into the variable region; every read and
	/// comparison goes there.
	OffsetTable,
	/// A length and `inline` bytes. A string of at most `inline` bytes is held
	/// whole in the slot; a longer one holds its first `inline - 4` bytes as a
	/// prefix and, in the slot's last four bytes, its offset into the variable
	/// region. `inline: 12` is a 16-byte string view.
	View {
		inline: u8,
	},
}

/// One column's place in the layout.
#[derive(Debug, Clone)]
pub(super) struct Column {
	name: String,
	kind: ColumnKind,
	offset: u32,
	width: u32,
}

/// The offsets a layout generation assigns a table's fields.
#[derive(Debug, Clone)]
pub(super) struct Layout {
	generation: u16,
	columns: Vec<Column>,
	strings: StringEncoding,
	/// Where the fixed region ends and the variable region begins.
	fixed_end: u32,
}

const HEADER: u32 = 4;

/// The high bit of a string's length word: set when every byte is ASCII, so
/// the string's character count is its byte length.
const ASCII: u32 = 1 << 31;

/// A string's byte length and whether it is ASCII, from its length word.
#[inline]
fn length_word(word: &[u8]) -> (usize, bool) {
	let word = u32::from_le_bytes(word.try_into().expect("four bytes"));
	((word & !ASCII) as usize, word & ASCII != 0)
}

impl Layout {
	/// Place `fields` for generation `generation`: widest slots first, so
	/// every 8-byte slot is 8-aligned without padding between slots.
	pub(super) fn plan(
		generation: u16,
		fields: &[(&str, ColumnKind)],
		strings: StringEncoding,
	) -> anyhow::Result<Self> {
		if let StringEncoding::View {
			inline,
		} = strings
			&& inline < 8
		{
			bail!("a string view needs at least eight inline bytes, for a prefix and an offset");
		}
		let width = |kind: ColumnKind| match (kind, strings) {
			(ColumnKind::Int | ColumnKind::Float, _) => 8,
			(ColumnKind::Bool, _) => 1,
			(ColumnKind::String, StringEncoding::OffsetTable) => 8,
			(
				ColumnKind::String,
				StringEncoding::View {
					inline,
				},
			) => 4 + u32::from(inline),
		};
		let null_map = (fields.len() as u32 * 2).div_ceil(8);
		let mut order: Vec<usize> = (0..fields.len()).collect();
		// Widest first; a view's width need not be a multiple of eight, so its
		// slots go after the 8-byte ones and before the bytes.
		order
			.sort_by_key(|&i| std::cmp::Reverse((width(fields[i].1) % 8 == 0, width(fields[i].1))));
		let mut offset = (HEADER + null_map).next_multiple_of(8);
		let mut columns = vec![None; fields.len()];
		for i in order {
			let (name, kind) = fields[i];
			let w = width(kind);
			columns[i] = Some(Column {
				name: name.to_owned(),
				kind,
				offset,
				width: w,
			});
			offset += w;
		}
		Ok(Self {
			generation,
			columns: columns.into_iter().map(|c| c.expect("every field placed")).collect(),
			strings,
			fixed_end: offset,
		})
	}

	/// The column named `name`, in declaration order.
	pub(super) fn column(&self, name: &str) -> Option<usize> {
		self.columns.iter().position(|c| c.name == name)
	}

	/// Encode `row` in this layout. A field the layout does not name, or one
	/// whose value is not of its column's kind, is refused: a strict layout
	/// holds only what its generation declares.
	pub(super) fn encode(&self, row: &Object) -> anyhow::Result<Vec<u8>> {
		for (name, _) in row.iter() {
			if name != "id" && self.column(name).is_none() {
				bail!("field `{name}` is not in layout generation {}", self.generation);
			}
		}
		let mut out = vec![0u8; self.fixed_end as usize];
		out[0..2].copy_from_slice(&self.generation.to_le_bytes());
		let mut var = Vec::new();
		for (i, column) in self.columns.iter().enumerate() {
			let bits = match row.get(column.name.as_str()) {
				None | Some(Value::None) => 0b00,
				Some(Value::Null) => 0b11,
				Some(value) => {
					self.write_slot(column, value, &mut out, &mut var)?;
					0b01
				}
			};
			out[HEADER as usize + i / 4] |= bits << ((i % 4) * 2);
		}
		out.extend_from_slice(&var);
		Ok(out)
	}

	fn write_slot(
		&self,
		column: &Column,
		value: &Value,
		out: &mut [u8],
		var: &mut Vec<u8>,
	) -> anyhow::Result<()> {
		let slot = &mut out[column.offset as usize..(column.offset + column.width) as usize];
		match (column.kind, value) {
			(ColumnKind::Int, Value::Number(Number::Int(v))) => {
				slot.copy_from_slice(&v.to_le_bytes())
			}
			(ColumnKind::Float, Value::Number(Number::Float(v))) => {
				slot.copy_from_slice(&v.to_le_bytes())
			}
			(ColumnKind::Bool, Value::Bool(v)) => slot[0] = u8::from(*v),
			(ColumnKind::String, Value::String(s)) => {
				let bytes = s.as_str().as_bytes();
				let len = u32::try_from(bytes.len())
					.ok()
					.filter(|len| len & ASCII == 0)
					.context("a string of 2GiB or more")?;
				let word = if bytes.is_ascii() {
					len | ASCII
				} else {
					len
				};
				let offset = u32::try_from(var.len()).context("a record longer than 4GiB")?;
				match self.strings {
					StringEncoding::OffsetTable => {
						slot[0..4].copy_from_slice(&offset.to_le_bytes());
						slot[4..8].copy_from_slice(&word.to_le_bytes());
						var.extend_from_slice(bytes);
					}
					StringEncoding::View {
						inline,
					} => {
						let inline = inline as usize;
						slot[0..4].copy_from_slice(&word.to_le_bytes());
						if bytes.len() <= inline {
							slot[4..4 + bytes.len()].copy_from_slice(bytes);
						} else {
							let prefix = inline - 4;
							slot[4..4 + prefix].copy_from_slice(&bytes[..prefix]);
							slot[4 + prefix..4 + inline].copy_from_slice(&offset.to_le_bytes());
							var.extend_from_slice(bytes);
						}
					}
				}
			}
			(kind, value) => bail!("column `{}` holds {kind:?}, not {value:?}", column.name),
		}
		Ok(())
	}
}

/// A record in a strict layout, borrowed as the storage cursor hands it over.
pub(super) struct StrictRow<'a> {
	layout: &'a Layout,
	bytes: &'a [u8],
}

impl<'a> StrictRow<'a> {
	pub(super) fn new(layout: &'a Layout, bytes: &'a [u8]) -> Self {
		Self {
			layout,
			bytes,
		}
	}

	/// The two null-map bits of column `i`.
	#[inline]
	fn bits(&self, i: usize) -> u8 {
		(self.bytes[HEADER as usize + i / 4] >> ((i % 4) * 2)) & 0b11
	}

	#[inline]
	fn slot(&self, column: &Column) -> &'a [u8] {
		&self.bytes[column.offset as usize..(column.offset + column.width) as usize]
	}

	#[inline]
	fn var(&self) -> &'a [u8] {
		&self.bytes[self.layout.fixed_end as usize..]
	}

	/// Column `i` of the row.
	#[inline]
	pub(super) fn read(&self, i: usize) -> Scalar<'a> {
		match self.bits(i) {
			0b01 => {}
			0b11 => return Scalar::Null,
			_ => return Scalar::None,
		}
		let column = &self.layout.columns[i];
		let slot = self.slot(column);
		match column.kind {
			ColumnKind::Int => Scalar::Number(Number::Int(i64::from_le_bytes(
				slot.try_into().expect("an 8-byte slot"),
			))),
			ColumnKind::Float => Scalar::Number(Number::Float(f64::from_le_bytes(
				slot.try_into().expect("an 8-byte slot"),
			))),
			ColumnKind::Bool => Scalar::Bool(slot[0] != 0),
			ColumnKind::String => Scalar::Str(self.string(column, slot)),
		}
	}

	/// Column `i` when it holds an integer: one aligned load, no tag.
	#[inline]
	pub(super) fn read_int(&self, i: usize) -> Option<i64> {
		let column = &self.layout.columns[i];
		(column.kind == ColumnKind::Int && self.bits(i) == 0b01)
			.then(|| i64::from_le_bytes(self.slot(column).try_into().expect("an 8-byte slot")))
	}

	fn string(&self, column: &Column, slot: &'a [u8]) -> &'a str {
		let bytes = match self.layout.strings {
			StringEncoding::OffsetTable => {
				let offset =
					u32::from_le_bytes(slot[0..4].try_into().expect("four bytes")) as usize;
				let (len, _) = length_word(&slot[4..8]);
				&self.var()[offset..offset + len]
			}
			StringEncoding::View {
				inline,
			} => {
				let inline = inline as usize;
				let (len, _) = length_word(&slot[0..4]);
				if len <= inline {
					&slot[4..4 + len]
				} else {
					let at = 4 + inline - 4;
					let offset =
						u32::from_le_bytes(slot[at..at + 4].try_into().expect("four bytes"))
							as usize;
					&self.var()[offset..offset + len]
				}
			}
		};
		debug_assert!(column.kind == ColumnKind::String);
		// The encoder wrote these bytes from a `str`.
		std::str::from_utf8(bytes).expect("a string column holds UTF-8")
	}

	/// Whether string column `i` holds `needle`, leaving the slot only when
	/// the lengths and the inline bytes agree and the rest must be compared.
	#[inline]
	pub(super) fn str_eq(&self, i: usize, needle: &str) -> bool {
		if self.bits(i) != 0b01 {
			return false;
		}
		let column = &self.layout.columns[i];
		let slot = self.slot(column);
		match self.layout.strings {
			StringEncoding::OffsetTable => self.string(column, slot) == needle,
			StringEncoding::View {
				inline,
			} => {
				let inline = inline as usize;
				let (len, _) = length_word(&slot[0..4]);
				let needle = needle.as_bytes();
				if len != needle.len() {
					return false;
				}
				if len <= inline {
					return &slot[4..4 + len] == needle;
				}
				let prefix = inline - 4;
				slot[4..4 + prefix] == needle[..prefix]
					&& self.string(column, slot).as_bytes() == needle
			}
		}
	}

	/// String column `i` against `needle`, bytewise, deciding from the inline
	/// prefix where it differs; `None` when the column holds no string.
	#[inline]
	pub(super) fn str_cmp(&self, i: usize, needle: &str) -> Option<Ordering> {
		if self.bits(i) != 0b01 {
			return None;
		}
		let column = &self.layout.columns[i];
		let slot = self.slot(column);
		if let StringEncoding::View {
			inline,
		} = self.layout.strings
		{
			let inline = inline as usize;
			let (len, _) = length_word(&slot[0..4]);
			let held = if len <= inline {
				len
			} else {
				inline - 4
			};
			let needle = needle.as_bytes();
			let n = held.min(needle.len());
			match slot[4..4 + n].cmp(&needle[..n]) {
				Ordering::Equal if len <= inline => return Some(len.cmp(&needle.len())),
				Ordering::Equal => {}
				decided => return Some(decided),
			}
		}
		Some(self.string(column, slot).as_bytes().cmp(needle.as_bytes()))
	}

	/// `string::len` of string column `i`: its character count, read from the
	/// length word when the string is ASCII, without touching the string.
	#[inline]
	pub(super) fn str_len(&self, i: usize) -> Option<i64> {
		if self.bits(i) != 0b01 {
			return None;
		}
		let column = &self.layout.columns[i];
		let slot = self.slot(column);
		let word = match self.layout.strings {
			StringEncoding::OffsetTable => &slot[4..8],
			StringEncoding::View {
				..
			} => &slot[0..4],
		};
		Some(match length_word(word) {
			(len, true) => len as i64,
			(_, false) => self.string(column, slot).chars().count() as i64,
		})
	}
}

impl RowReader for StrictRow<'_> {
	fn field(&self, name: &FieldName) -> anyhow::Result<Scalar<'_>> {
		Ok(match self.layout.column(name.as_str()) {
			Some(i) => self.read(i),
			None => Scalar::None,
		})
	}

	fn field_int(&self, name: &FieldName) -> anyhow::Result<Option<i64>> {
		Ok(self.layout.column(name.as_str()).and_then(|i| self.read_int(i)))
	}

	fn record(&self) -> anyhow::Result<Value> {
		let mut object = Object::default();
		for (i, column) in self.layout.columns.iter().enumerate() {
			match self.read(i) {
				Scalar::None => {}
				scalar => {
					object.insert(column.name.as_str(), scalar.to_value());
				}
			}
		}
		Ok(Value::Object(object))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn row(fields: &[(&str, Value)]) -> Object {
		let mut object = Object::default();
		for (name, value) in fields {
			object.insert(*name, value.clone());
		}
		object
	}

	const ENCODINGS: [StringEncoding; 4] = [
		StringEncoding::OffsetTable,
		StringEncoding::View {
			inline: 8,
		},
		StringEncoding::View {
			inline: 12,
		},
		StringEncoding::View {
			inline: 28,
		},
	];

	fn layout(strings: StringEncoding) -> Layout {
		Layout::plan(
			1,
			&[
				("ok", ColumnKind::Bool),
				("name", ColumnKind::String),
				("n", ColumnKind::Int),
				("x", ColumnKind::Float),
				("tag", ColumnKind::String),
			],
			strings,
		)
		.expect("plans")
	}

	#[test]
	fn a_record_reads_back_as_written_in_every_string_encoding() {
		let long = "a string long enough to spill past every inline width";
		for strings in ENCODINGS {
			let layout = layout(strings);
			for fields in [
				vec![
					("ok", Value::Bool(true)),
					("name", Value::from("ann")),
					("n", Value::Number(Number::Int(-9))),
					("x", Value::Number(Number::Float(1.5))),
					("tag", Value::from(long)),
				],
				vec![("name", Value::from("")), ("n", Value::Null)],
				vec![("tag", Value::from("exactly twelve!".get(..12).expect("ascii")))],
				vec![],
			] {
				let original = row(&fields);
				let bytes = layout.encode(&original).expect("encodes");
				let read = StrictRow::new(&layout, &bytes).record().expect("reads");
				let expected: Object = row(&fields
					.iter()
					.filter(|(_, v)| !matches!(v, Value::None))
					.cloned()
					.collect::<Vec<_>>());
				assert_eq!(read, Value::Object(expected), "{strings:?}");
			}
		}
	}

	#[test]
	fn absent_and_null_are_told_apart() {
		for strings in ENCODINGS {
			let layout = layout(strings);
			let bytes = layout.encode(&row(&[("n", Value::Null)])).expect("encodes");
			let r = StrictRow::new(&layout, &bytes);
			let n = layout.column("n").expect("n");
			let name = layout.column("name").expect("name");
			assert!(matches!(r.read(n), Scalar::Null));
			assert!(matches!(r.read(name), Scalar::None));
			assert_eq!(r.read_int(n), None);
		}
	}

	#[test]
	fn string_comparisons_agree_with_comparing_the_strings() {
		let words = ["", "a", "ab", "abcdefgh", "abcdefghi", "abcdefghijklmnop", "abd", "b", "zz"];
		for strings in ENCODINGS {
			let layout = layout(strings);
			let name = layout.column("name").expect("name");
			for held in words {
				let bytes = layout.encode(&row(&[("name", Value::from(held))])).expect("encodes");
				let r = StrictRow::new(&layout, &bytes);
				for needle in words {
					assert_eq!(
						r.str_eq(name, needle),
						held == needle,
						"{strings:?} {held} {needle}"
					);
					assert_eq!(
						r.str_cmp(name, needle),
						Some(held.as_bytes().cmp(needle.as_bytes())),
						"{strings:?} {held} {needle}"
					);
				}
			}
		}
	}

	#[test]
	fn string_len_counts_characters_from_the_slot_or_the_string() {
		for strings in ENCODINGS {
			let layout = layout(strings);
			let name = layout.column("name").expect("name");
			for held in [
				"",
				"abc",
				"héllo",
				"日本語の文字列はとても長いです",
				"a long ascii string past every inline width",
				"👍🏽",
			] {
				let bytes = layout.encode(&row(&[("name", Value::from(held))])).expect("encodes");
				let r = StrictRow::new(&layout, &bytes);
				assert_eq!(
					r.str_len(name),
					Some(held.chars().count() as i64),
					"{strings:?} {held}"
				);
				assert!(matches!(r.read(name), Scalar::Str(s) if s == held), "{strings:?} {held}");
			}
			let bytes = layout.encode(&row(&[])).expect("encodes");
			assert_eq!(StrictRow::new(&layout, &bytes).str_len(name), None);
		}
	}

	#[test]
	fn a_value_outside_the_layout_is_refused() {
		let layout = layout(StringEncoding::OffsetTable);
		assert!(layout.encode(&row(&[("other", Value::Bool(true))])).is_err());
		assert!(layout.encode(&row(&[("n", Value::from("3"))])).is_err());
	}

	#[test]
	fn eight_byte_slots_are_aligned() {
		for strings in ENCODINGS {
			let layout = layout(strings);
			for column in &layout.columns {
				if column.width % 8 == 0 {
					assert_eq!(column.offset % 8, 0, "{strings:?} {}", column.name);
				}
			}
		}
	}
}
