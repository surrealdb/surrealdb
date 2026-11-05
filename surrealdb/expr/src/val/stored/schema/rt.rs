//! The runtime the derive's expansion and hand-written schema impls share.
//!
//! These are the building blocks of the generated code. Each refuses malformed bytes with a typed
//! error, and none indexes stored bytes without checking them first.

use surrealdb_kvs::release::Release;

use super::{MAX_SCHEMA_DEPTH, SchemaError, StoredField};
use crate::val::stored::decode::strict;
use crate::val::stored::{Encoder, Error, Item, MapRef, Open};

/// The map `item`, entered at schema depth `depth`.
pub fn map<'a>(item: Item<'a>, depth: u32, ty: &'static str) -> Result<MapRef<'a>, SchemaError> {
	if depth > MAX_SCHEMA_DEPTH {
		return Err(SchemaError::TooDeep {
			ty,
		});
	}
	item.map()?.ok_or(SchemaError::WrongKind {
		ty,
		n: 0,
	})
}

/// Entry `i` of `map`, which must hold a number greater than `prev`, the number of the entry read
/// before it (0 before the first). Updates `prev`.
pub fn entry<'a>(
	map: &MapRef<'a>,
	i: usize,
	prev: &mut u32,
	ty: &'static str,
) -> Result<(u32, Item<'a>), SchemaError> {
	let (n, item) = map.entry(i)?;
	if n <= *prev {
		return Err(SchemaError::FieldOrder {
			ty,
		});
	}
	*prev = n;
	Ok((n, item))
}

/// Reads field `n` of `ty`, one schema level below `depth`.
pub fn field<T: StoredField>(
	item: Item<'_>,
	depth: u32,
	ty: &'static str,
	n: u32,
) -> Result<T, SchemaError> {
	T::decode_item(item, depth.saturating_add(1)).map_err(|e| e.at(ty, n))
}

/// Reads field `n` of `ty` as the type the `narrow` function of a superseding field returns.
pub fn field_via<T, O: StoredField>(
	item: Item<'_>,
	depth: u32,
	ty: &'static str,
	n: u32,
	narrow: fn(&T) -> Option<O>,
) -> Result<O, SchemaError> {
	let _ = narrow;
	field::<O>(item, depth, ty, n)
}

/// The type name writer of the narrow type a `narrow` function returns.
pub const fn narrow_type_name<T, O: StoredField>(narrow: fn(&T) -> Option<O>) -> fn(&mut String) {
	let _ = narrow;
	O::write_type_name
}

/// An entry number the type does not read as a field: a reserved or ignorable one is skipped, a
/// critical one is refused.
pub fn unknown(n: u32, ty: &'static str, reserved: &[u32]) -> Result<(), SchemaError> {
	if reserved.contains(&n) || n % 2 == 1 {
		Ok(())
	} else {
		Err(SchemaError::UnknownCriticalField {
			ty,
			n,
		})
	}
}

pub const fn missing(ty: &'static str, n: u32) -> SchemaError {
	SchemaError::MissingField {
		ty,
		n,
	}
}

pub const fn order(ty: &'static str) -> SchemaError {
	SchemaError::FieldOrder {
		ty,
	}
}

pub const fn legacy(ty: &'static str) -> SchemaError {
	SchemaError::LegacyVariant {
		ty,
	}
}

/// The newer of two optional releases.
pub fn max_release(a: Option<Release>, b: Option<Release>) -> Option<Release> {
	match (a, b) {
		(Some(a), Some(b)) => Some(a.max(b)),
		(a, None) => a,
		(None, b) => b,
	}
}

/// The single entry of the enum map `item`: the variant number and its payload.
pub fn one_variant<'a>(
	item: Item<'a>,
	depth: u32,
	ty: &'static str,
) -> Result<(u32, Item<'a>), SchemaError> {
	let map = map(item, depth, ty)?;
	if map.len() != 1 {
		return Err(SchemaError::NotOneVariant {
			ty,
		});
	}
	Ok(map.entry(0)?)
}

/// Checks that the payload of a unit variant is exactly NONE.
pub fn unit(payload: Item<'_>, ty: &'static str, n: u32) -> Result<(), SchemaError> {
	let (&tag, rest) = payload.bytes().split_first().ok_or(Error::Truncated)?;
	match strict::none(tag, rest)? {
		Some(()) => Ok(()),
		None => Err(SchemaError::WrongKind {
			ty,
			n,
		}),
	}
}

/// The payload a prior enum item holds for variant `n`: the prior must hold exactly that one
/// variant.
pub fn variant_prior(prior: Option<Item<'_>>, n: u32) -> Option<Item<'_>> {
	let map = prior?.map().ok().flatten()?;
	if map.len() != 1 {
		return None;
	}
	let (k, item) = map.entry(0).ok()?;
	(k == n).then_some(item)
}

/// A prior map being merged into a new one, entry by entry from the highest number down.
///
/// The prior is used only when it is a well-formed map: canonical numbers, strictly ascending,
/// every item non-empty. Otherwise it contributes nothing, so a prior is never partly carried.
pub struct Entries<'p> {
	map: Option<MapRef<'p>>,
	/// How many entries remain below the ones consumed.
	left: usize,
}

impl<'p> Entries<'p> {
	pub fn new(prior: Option<Item<'p>>) -> Self {
		let map =
			prior.and_then(|item| item.map().ok().flatten()).filter(|map| map.validate().is_ok());
		Self {
			left: map.as_ref().map_or(0, MapRef::len),
			map,
		}
	}

	/// Consumes the prior entries numbered `n` and above. Entries above `n` that the writer does
	/// not know (`ours` is false) and that are ignorable (odd) are copied into `open`; the rest
	/// are dropped. Returns the item under `n`, when the prior has one.
	pub fn take(
		&mut self,
		enc: &mut Encoder,
		open: &mut Open,
		n: u32,
		ours: fn(u32) -> bool,
	) -> Result<Option<Item<'p>>, SchemaError> {
		let Some(map) = self.map else {
			return Ok(None);
		};
		while let Some(last) = self.left.checked_sub(1) {
			let (k, item) = map.entry(last)?;
			if k < n {
				return Ok(None);
			}
			self.left = last;
			if k == n {
				return Ok(Some(item));
			}
			if k % 2 == 1 && !ours(k) {
				enc.raw_entry(open, k, item.bytes())?;
			}
		}
		Ok(None)
	}
}

/// Writes one map: the fields of a struct, in descending number order, with the unknown ignorable
/// entries of the prior carried over.
///
/// Call [`MapWriter::put`] or [`MapWriter::skip`] for every number the type reads, highest first,
/// then [`MapWriter::finish`].
pub struct MapWriter<'e, 'p> {
	enc: &'e mut Encoder,
	open: Open,
	entries: Entries<'p>,
	ty: &'static str,
	ours: fn(u32) -> bool,
}

impl<'e, 'p> MapWriter<'e, 'p> {
	/// Opens a map. `ours` is true for every number the type reads or has reserved.
	pub fn new(
		enc: &'e mut Encoder,
		prior: Option<Item<'p>>,
		ty: &'static str,
		ours: fn(u32) -> bool,
	) -> Result<Self, SchemaError> {
		let open = enc.open()?;
		Ok(Self {
			enc,
			open,
			entries: Entries::new(prior),
			ty,
			ours,
		})
	}

	/// Writes `value` under `n`, passing it the prior item under `n`.
	pub fn put<T: StoredField>(&mut self, value: &T, n: u32) -> Result<(), SchemaError> {
		let prior = self.entries.take(self.enc, &mut self.open, n, self.ours)?;
		value.encode_item(self.enc, prior).map_err(|e| e.at(self.ty, n))?;
		self.enc.end_entry(&mut self.open, n)?;
		Ok(())
	}

	/// Writes nothing under `n`: the prior entry under `n`, if any, is dropped.
	pub fn skip(&mut self, n: u32) -> Result<(), SchemaError> {
		self.entries.take(self.enc, &mut self.open, n, self.ours)?;
		Ok(())
	}

	/// Carries over the prior entries below the last number written, and closes the map.
	pub fn finish(mut self) -> Result<(), SchemaError> {
		self.entries.take(self.enc, &mut self.open, 0, self.ours)?;
		self.enc.close_map(self.open)?;
		Ok(())
	}
}
