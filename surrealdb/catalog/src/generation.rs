//! Generation numbers of a table, and the ring they are allocated in.
//!
//! A record of a table is stamped with a generation, which names the [`LayoutDescriptor`] its
//! bytes are read against. A [`Generation`] is a nonzero `u16`. The envelope stamp is a `u64`, and
//! stamp `0` is the stamp of a `revision` row and of a v1 row written before its table had
//! generations: it names no generation, and [`Generation::of_stamp`] classifies every stamp.
//!
//! # The ring
//!
//! A table's generations are allocated in a ring. The next generation is the head plus one,
//! wrapping from [`Generation::MAX`] to [`Generation::FIRST`]. The order of two generations of a
//! table is their distance along the ring from the table's base ([`Generation::distance_to`]),
//! never their numeric order.
//!
//! A [`GenerationRing`] is the window `base..=head` of a table:
//!
//! - `head` is the newest generation, the one new rows are written with.
//! - `base` is the oldest generation whose descriptor the table still holds.
//! - `floor` is `base` when every row of the table is known to carry a generation of the ring, and
//!   unknown (`None`, stored as `0`) otherwise. A floor is never anywhere else, so the window's
//!   tail is `base` whether or not the floor is known.
//!
//! A stamp outside the window is corrupt, with the exception of stamp `0`. The ring is full when
//! the generation after the head is the base; minting into a full ring is refused until the base
//! advances.
//!
//! # Stored form
//!
//! [`TableMigrations`] stores a table's ring under the table's `!mi` key and [`GenerationMark`]
//! stores a table name's head under the database's `!gn` key. The mark is keyed by name, outside
//! the table's subtree, so it survives the table's removal: a number names exactly one descriptor
//! for the life of a table name. Both types are `floor_fixed`, because every release that reads a
//! v1 row of the table needs its ring: no release may be unable to decode them.
//!
//! [`LayoutDescriptor`]: crate::layout::LayoutDescriptor

use std::fmt;
use std::num::NonZeroU16;

use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, Encoded};

use crate::Error;
use crate::layout::out_of_range;
use crate::stored::{
	EntryError, SchemaError, Stored, StoredField, StoredType, canonical, decode_entry,
	encode_stored,
};
use crate::val::Value;
use crate::val::stored::{Encoder, Item};

/// A generation of a table: a nonzero `u16`, which names one descriptor of the table's ring.
///
/// Its stored form is the `u16` it holds, so the wire type name is `int:u16`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Generation(NonZeroU16);

impl Generation {
	/// The first generation a table mints.
	pub const FIRST: Self = Self(NonZeroU16::MIN);

	/// The highest generation. The mint after it wraps to [`Self::FIRST`].
	pub const MAX: Self = Self(NonZeroU16::MAX);

	/// The generation `n`, or `None` for 0, which names no generation.
	pub const fn new(n: u16) -> Option<Self> {
		match NonZeroU16::new(n) {
			Some(n) => Some(Self(n)),
			None => None,
		}
	}

	/// The number the generation names, in `1..=65535`.
	pub const fn get(self) -> u16 {
		self.0.get()
	}

	/// The stamp an envelope carries for this generation: its value as a varint.
	pub const fn stamp(self) -> u64 {
		self.get() as u64
	}

	/// The generation minted after this one: this plus one, wrapping from [`Self::MAX`] to
	/// [`Self::FIRST`].
	pub const fn next(self) -> Self {
		match Self::new(self.get().wrapping_add(1)) {
			Some(next) => next,
			None => Self::FIRST,
		}
	}

	/// How many mints after `self` the generation `to` comes, in `0..=65534`. The distance from a
	/// generation to itself is 0, and from [`Self::MAX`] to [`Self::FIRST`] is 1.
	pub const fn distance_to(self, to: Self) -> u16 {
		// `to + MAX - from` is at least 1 and below `2 * MAX`, so the sum and difference are exact
		// in `u32`, and the remainder is below `MAX`.
		let modulus = u16::MAX as u32;
		((to.get() as u32 + modulus - self.get() as u32) % modulus) as u16
	}

	/// The generation an envelope stamp names: `Ok(None)` for stamp 0, which names no generation,
	/// `Ok(Some)` for stamps `1..=65535`, and `Err(stamp)` for a stamp above them.
	pub const fn of_stamp(stamp: u64) -> Result<Option<Self>, u64> {
		if stamp > u16::MAX as u64 {
			return Err(stamp);
		}
		Ok(Self::new(stamp as u16))
	}
}

impl fmt::Display for Generation {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Display::fmt(&self.get(), f)
	}
}

/// A generation is stored as the `u16` it holds, so it is written and read exactly as a `u16` is.
/// A stored 0 names no generation and is refused as out of range.
impl StoredField for Generation {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		<u16 as StoredField>::encode_item(&self.get(), enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		let n = <u16 as StoredField>::decode_item(item, depth)?;
		Self::new(n).ok_or_else(SchemaError::out_of_range)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		<u16 as StoredField>::into_vm_value(self.get())
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		let n = <u16 as StoredField>::from_vm_value(value)?;
		Self::new(n).ok_or_else(|| SchemaError::vm("int:u16", "a generation of 0"))
	}

	fn write_type_name(out: &mut String) {
		<u16 as StoredField>::write_type_name(out);
	}
}

/// A table's generation ring: the generations from `base` to `head` in mint order, and the floor.
///
/// A ring is either [empty](Self::NONE), the state of a table that has no generation, or has a
/// `base` and a `head`, and a `floor` that is unknown or equal to `base`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GenerationRing {
	base: Option<Generation>,
	head: Option<Generation>,
	floor: Option<Generation>,
}

impl GenerationRing {
	/// The ring of a table that has no generation.
	pub const NONE: Self = Self {
		base: None,
		head: None,
		floor: None,
	};

	/// The ring `base..=head` with `floor`, or `None` unless `floor` is unknown or equal to `base`.
	pub const fn new(
		base: Generation,
		head: Generation,
		floor: Option<Generation>,
	) -> Option<Self> {
		let floor_is_base = match floor {
			None => true,
			Some(floor) => floor.get() == base.get(),
		};
		if !floor_is_base {
			return None;
		}
		Some(Self {
			base: Some(base),
			head: Some(head),
			floor,
		})
	}

	/// Whether the table has no generation.
	pub const fn is_empty(self) -> bool {
		self.base.is_none()
	}

	/// The oldest generation the table holds, or `None` for an empty ring.
	pub const fn base(self) -> Option<Generation> {
		self.base
	}

	/// The newest generation, or `None` for an empty ring.
	pub const fn head(self) -> Option<Generation> {
		self.head
	}

	/// The floor: `base` when every row is known to carry a generation of the ring, `None` when
	/// that is unknown.
	pub const fn floor(self) -> Option<Generation> {
		self.floor
	}

	/// Whether `generation` lies in `base..=head`, in ring order.
	pub const fn contains(self, generation: Generation) -> bool {
		match (self.base, self.head) {
			(Some(base), Some(head)) => base.distance_to(generation) <= base.distance_to(head),
			_ => false,
		}
	}

	/// The distance of `generation` from the base, when the ring contains it.
	pub const fn position(self, generation: Generation) -> Option<u16> {
		match self.base {
			Some(base) if self.contains(generation) => Some(base.distance_to(generation)),
			_ => None,
		}
	}

	/// Whether the generation after the head is the base, so that no generation can be minted.
	/// An empty ring is not full.
	pub const fn is_full(self) -> bool {
		match (self.base, self.head) {
			(Some(base), Some(head)) => head.next().get() == base.get(),
			_ => false,
		}
	}

	/// The ring after one mint: the head moves to the next generation. `None` for an empty ring,
	/// which a mint creates rather than advances, and for a full one.
	pub const fn advanced(self) -> Option<Self> {
		match (self.base, self.head) {
			(Some(_), Some(head)) if !self.is_full() => Some(Self {
				base: self.base,
				head: Some(head.next()),
				floor: self.floor,
			}),
			_ => None,
		}
	}

	/// The ring with `floor` as both its base and its known floor, when the ring contains
	/// `floor`. The head is unchanged, so a floor never moves past it.
	pub const fn with_floor(self, floor: Generation) -> Option<Self> {
		if !self.contains(floor) {
			return None;
		}
		Some(Self {
			base: Some(floor),
			head: self.head,
			floor: Some(floor),
		})
	}
}

/// The highest generation minted for a table name of a database, stored under the database's
/// `!gn` key for that name.
///
/// Every mint writes it and nothing deletes it with the table, so a table defined again under the
/// name continues the numbering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0", floor_fixed)]
pub struct GenerationMark {
	/// The highest generation minted for the name.
	#[stored(n = 2)]
	head: Generation,
}

impl GenerationMark {
	/// The mark naming `head` as the highest generation minted for a table name.
	pub const fn new(head: Generation) -> Self {
		Self {
			head,
		}
	}

	/// The highest generation minted for the name.
	pub const fn head(&self) -> Generation {
		self.head
	}
}

/// A table's generation ring, stored under the table's `!mi` key.
///
/// `base` and `head` are generations, and `floor` is 0 or `base`. The fields are private: the
/// entry is built by [`TableMigrations::of`] from a [`GenerationRing`], or decoded, and decoding
/// runs [`TableMigrations::validate`], which checks the three fields together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0", floor_fixed, validate)]
pub struct TableMigrations {
	/// The floor: `base` when every row is known to carry a generation of the ring, 0 when that is
	/// unknown.
	#[stored(n = 2)]
	floor: u16,
	/// The oldest generation whose descriptor the table holds.
	#[stored(n = 4)]
	base: Generation,
	/// The newest generation, which new rows are stamped with.
	#[stored(n = 6)]
	head: Generation,
}

impl TableMigrations {
	/// The oldest generation whose descriptor the table holds.
	pub const fn base(&self) -> Generation {
		self.base
	}

	/// The newest generation, which new rows are stamped with.
	pub const fn head(&self) -> Generation {
		self.head
	}

	/// The floor: the base when every row is known to carry a generation of the ring, `None` when
	/// that is unknown.
	pub const fn floor(&self) -> Option<Generation> {
		Generation::new(self.floor)
	}

	/// The ring the entry states. An entry that decodes always states one.
	pub fn ring(self) -> Result<GenerationRing, EntryError> {
		GenerationRing::new(self.base, self.head, self.floor()).ok_or_else(|| {
			EntryError::body(<Self as StoredType>::SCHEMA.name, out_of_range::<Self>(2))
		})
	}

	/// The entry that states `ring`, or `None` for an empty ring, which has no entry. The entry
	/// is refused, as `None`, if the fields it would hold do not validate.
	pub fn of(ring: GenerationRing) -> Option<Self> {
		match (ring.base(), ring.head()) {
			(Some(base), Some(head)) => Some(Self {
				floor: ring.floor().map_or(0, Generation::get),
				base,
				head,
			})
			.filter(|entry| entry.validate().is_ok()),
			_ => None,
		}
	}

	/// Checks the three fields together: the floor is 0 or the base.
	pub fn validate(&self) -> Result<(), SchemaError> {
		GenerationRing::new(self.base, self.head, self.floor())
			.map(|_| ())
			.ok_or_else(|| out_of_range::<Self>(2))
	}
}

/// Written and read at the store's feature level.
impl KVValue for GenerationMark {
	type KeyContext = ();

	const LEVELLED: bool = true;

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		Err(Error::Entry(EntryError::Unlevelled {
			ty: <Self as StoredType>::SCHEMA.name,
		})
		.into())
	}

	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded> {
		Ok(encode_stored(self, ctx).map_err(Error::Entry)?)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}

/// Written and read at the store's feature level, and refused when it does not pass
/// [`TableMigrations::validate`], in either direction.
impl KVValue for TableMigrations {
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

#[cfg(test)]
pub(crate) mod tests {
	use surrealdb_kvs::{FeatureLevel, Release};

	use super::*;

	/// The generation `n`, which a test names as a nonzero number.
	pub(crate) fn g(n: u16) -> Generation {
		Generation::new(n).expect("a nonzero generation")
	}

	/// The entry refusal `error` carries.
	pub(crate) fn refusal(error: &anyhow::Error) -> EntryError {
		let cause: &(dyn std::error::Error + 'static) = error.as_ref();
		EntryError::of_cause(cause).unwrap_or_else(|| panic!("not an entry refusal: {error:#}"))
	}

	/// The refusal of field `n` of the stored type named `ty`.
	pub(crate) fn out_of(ty: &'static str, n: u32) -> EntryError {
		EntryError::body(
			ty,
			SchemaError::OutOfRange {
				ty,
				n,
			},
		)
	}

	/// `bytes` with the release floor replaced by `floor`.
	pub(crate) fn with_floor(bytes: &[u8], floor: Release) -> Vec<u8> {
		let (_, body) = crate::stored::parse_header("test", bytes).expect("a readable entry");
		let mut out = vec![crate::stored::CATALOG_MARKER, 0];
		let mut value = u64::from(floor.to_u32());
		loop {
			let byte = (value & 0x7f) as u8;
			value >>= 7;
			if value == 0 {
				out.push(byte);
				break;
			}
			out.push(byte | 0x80);
		}
		out.extend_from_slice(body);
		out
	}

	/// The checks every levelled, validated catalog type shares: the frozen bytes of `value` are
	/// its canonical bytes and decode to it, every prefix of them is refused, and a floor above
	/// this build is refused before the body is read.
	pub(crate) fn check_frozen<T>(value: &T, frozen: &[u8])
	where
		T: KVValue<KeyContext = ()> + PartialEq + std::fmt::Debug,
	{
		let canonical = value.kv_canonical().expect("canonical bytes");
		assert_eq!(canonical, frozen, "{value:?} is written differently: {canonical:?}");
		let at_current = value.kv_encode_value_at(&EncodeContext::CURRENT).expect("encodes");
		assert_eq!(at_current.bytes, frozen, "{value:?} is written differently at this release");
		assert_eq!(at_current.floor, Some(Release::FIRST_STORED));
		assert_eq!(&T::kv_decode_value(frozen, ()).expect("decodes"), value);
		for end in 0..frozen.len() {
			assert!(
				T::kv_decode_value(&frozen[..end], ()).is_err(),
				"{value:?} decodes truncated at {end}"
			);
		}
		let later = Release::new(Release::CURRENT.major() + 1, 0);
		assert!(matches!(
			refusal(&T::kv_decode_value(&with_floor(frozen, later), ()).unwrap_err()),
			EntryError::NeedsRelease { needed, .. } if needed == later
		));
		let mut reserved = frozen.to_vec();
		reserved[1] = 0x01;
		assert_eq!(
			refusal(&T::kv_decode_value(&reserved, ()).unwrap_err()),
			EntryError::ReservedBits(1)
		);
		assert!(value.kv_encode_value().is_err(), "an unlevelled write of {value:?} is refused");
		assert!(
			matches!(
				refusal(&value.kv_encode_value_at(&EncodeContext::LEGACY).unwrap_err()),
				EntryError::AboveLevel {
					level: FeatureLevel::Legacy,
					..
				}
			),
			"{value:?} is written at the Legacy level"
		);
	}

	#[test]
	fn the_generation_after_the_head_wraps_to_the_first_and_never_reaches_zero() {
		assert_eq!(Generation::FIRST.next(), g(2));
		assert_eq!(g(u16::MAX - 1).next(), Generation::MAX);
		assert_eq!(Generation::MAX.next(), Generation::FIRST);
	}

	#[test]
	fn a_generation_is_a_nonzero_number_with_a_bare_display() {
		assert_eq!(Generation::new(0), None);
		assert_eq!(Generation::new(7), Some(g(7)));
		assert_eq!(Generation::MAX.get(), u16::MAX);
		assert_eq!(Generation::MAX.stamp(), 65_535);
		assert_eq!(g(7).to_string(), "7");
		assert_eq!(format!("{:>4}", g(7)), "   7");
	}

	#[test]
	fn a_stamp_names_no_generation_one_or_a_generation_or_is_above_the_ring() {
		assert_eq!(Generation::of_stamp(0), Ok(None));
		assert_eq!(Generation::of_stamp(1), Ok(Some(g(1))));
		assert_eq!(Generation::of_stamp(65_535), Ok(Some(Generation::MAX)));
		assert_eq!(Generation::of_stamp(65_536), Err(65_536));
		assert_eq!(Generation::of_stamp(u64::MAX), Err(u64::MAX));
	}

	#[test]
	fn distance_counts_mints_along_the_ring() {
		assert_eq!(g(7).distance_to(g(7)), 0);
		assert_eq!(g(1).distance_to(g(2)), 1);
		assert_eq!(Generation::MAX.distance_to(g(1)), 1);
		assert_eq!(g(1).distance_to(Generation::MAX), u16::MAX - 1);
		assert_eq!(g(2).distance_to(g(1)), u16::MAX - 1);
		// Every pair is at most the length of the ring less one apart, and distances add up.
		for (from, to) in [(1, 30_000), (30_000, 65_000), (65_000, 2), (65_535, 65_535)] {
			let there = g(from).distance_to(g(to));
			let back = g(to).distance_to(g(from));
			assert!(there < u16::MAX);
			assert_eq!((u32::from(there) + u32::from(back)) % u32::from(u16::MAX), 0);
		}
	}

	#[test]
	fn a_ring_has_a_floor_unknown_or_at_its_base() {
		assert_eq!(GenerationRing::new(g(2), g(5), Some(g(3))), None);
		assert_eq!(GenerationRing::new(g(2), g(5), Some(g(5))), None);
		let ring = GenerationRing::new(g(2), g(5), Some(g(2))).expect("a ring");
		assert_eq!((ring.base(), ring.head(), ring.floor()), (Some(g(2)), Some(g(5)), Some(g(2))));
		assert!(!ring.is_empty());
		let unknown = GenerationRing::new(g(2), g(5), None).expect("a ring");
		assert_eq!(unknown.floor(), None);
		assert!(GenerationRing::NONE.is_empty());
		assert_eq!(GenerationRing::NONE.base(), None);
		assert_eq!(GenerationRing::default(), GenerationRing::NONE);
	}

	#[test]
	fn a_wrapped_ring_holds_its_generations_in_ring_order() {
		let ring = GenerationRing::new(g(65_000), g(5), None).expect("a ring");
		for generation in [65_000, 65_535, 1, 5] {
			assert!(ring.contains(g(generation)), "{generation}");
		}
		for generation in [6, 64_999, 30_000] {
			assert!(!ring.contains(g(generation)), "{generation}");
			assert_eq!(ring.position(g(generation)), None);
		}
		assert_eq!(ring.position(g(65_000)), Some(0));
		assert_eq!(ring.position(g(1)), Some(536));
		assert_eq!(ring.position(g(5)), Some(540));
		assert!(!GenerationRing::NONE.contains(g(1)));
	}

	#[test]
	fn a_full_ring_cannot_advance() {
		let ring = GenerationRing::new(g(1), g(u16::MAX - 1), Some(g(1))).expect("a ring");
		let advanced = ring.advanced().expect("room for one more");
		assert_eq!(
			(advanced.base(), advanced.head(), advanced.floor()),
			(Some(g(1)), Some(Generation::MAX), Some(g(1)))
		);
		assert!(advanced.is_full());
		assert_eq!(advanced.advanced(), None);
		let wrapped = GenerationRing::new(g(3), Generation::MAX, None).expect("a ring").advanced();
		assert_eq!(wrapped, GenerationRing::new(g(3), Generation::FIRST, None));
		assert_eq!(GenerationRing::new(g(3), g(2), None).and_then(GenerationRing::advanced), None);
		assert_eq!(GenerationRing::NONE.advanced(), None);
		assert!(!GenerationRing::NONE.is_full());
	}

	#[test]
	fn a_floor_moves_inside_the_ring_and_takes_the_base_with_it() {
		let ring = GenerationRing::new(g(65_000), g(5), None).expect("a ring");
		assert_eq!(ring.with_floor(g(2)), GenerationRing::new(g(2), g(5), Some(g(2))));
		assert_eq!(ring.with_floor(g(5)), GenerationRing::new(g(5), g(5), Some(g(5))));
		assert_eq!(ring.with_floor(g(6)), None);
	}

	/// The stored bytes of a `TableMigrations` entry whose floor, base and head are `floor`, `base`
	/// and `head`, each below 128: a value below 128 is stored as itself plus 128.
	fn migrations_bytes(floor: u8, base: u8, head: u8) -> [u8; 16] {
		[254, 0, 128, 128, 16, 38, 3, 0, 2, 4, 2, floor | 128, 4, base | 128, 6, head | 128]
	}

	#[test]
	fn table_migrations_state_a_ring_or_name_the_field_that_breaks_it() {
		let ring = GenerationRing::new(g(4), g(9), Some(g(4))).expect("a ring");
		let entry = TableMigrations::of(ring).expect("an entry");
		assert_eq!(entry.ring(), Ok(ring));
		assert_eq!(TableMigrations::of(GenerationRing::NONE), None);
		let ty = <TableMigrations as StoredType>::SCHEMA.name;
		assert_eq!(
			TableMigrations::kv_decode_value(&migrations_bytes(5, 4, 9), ())
				.map_err(|error| refusal(&error)),
			Err(out_of(ty, 2))
		);
		assert!(TableMigrations::kv_decode_value(&migrations_bytes(0, 4, 9), ()).is_ok());
		assert!(TableMigrations::kv_decode_value(&migrations_bytes(4, 4, 9), ()).is_ok());
	}

	#[test]
	fn a_zero_generation_in_a_stored_entry_is_refused_at_its_field() {
		// The frozen bytes of `TableMigrations { floor: 0, base: 3, head: 9 }` and of
		// `GenerationMark { head: 7 }`, with one generation's value replaced by 0.
		let ty = <TableMigrations as StoredType>::SCHEMA.name;
		let base_zero: [u8; 16] = [254, 0, 128, 128, 16, 38, 3, 0, 2, 4, 2, 128, 4, 128, 6, 137];
		assert_eq!(
			refusal(&TableMigrations::kv_decode_value(&base_zero, ()).unwrap_err()),
			out_of(ty, 4)
		);
		let head_zero: [u8; 16] = [254, 0, 128, 128, 16, 38, 3, 0, 2, 4, 2, 128, 4, 131, 6, 128];
		assert_eq!(
			refusal(&TableMigrations::kv_decode_value(&head_zero, ()).unwrap_err()),
			out_of(ty, 6)
		);
		let ty = <GenerationMark as StoredType>::SCHEMA.name;
		let mark_zero: [u8; 10] = [254, 0, 128, 128, 16, 38, 1, 0, 2, 128];
		assert_eq!(
			refusal(&GenerationMark::kv_decode_value(&mark_zero, ()).unwrap_err()),
			out_of(ty, 2)
		);
	}

	#[test]
	fn an_invalid_ring_is_refused_in_either_direction() {
		let ty = <TableMigrations as StoredType>::SCHEMA.name;
		let bytes = migrations_bytes(3, 4, 9);
		assert_eq!(
			refusal(&TableMigrations::kv_decode_value(&bytes, ()).unwrap_err()),
			out_of(ty, 2)
		);
		// No constructor yields an invalid entry, so the encoding side is checked on the value
		// built directly from its private fields.
		let migrations = TableMigrations {
			floor: 3,
			base: g(4),
			head: g(9),
		};
		assert_eq!(refusal(&migrations.kv_canonical().unwrap_err()), out_of(ty, 2));
		assert!(canonical(&migrations).is_ok(), "the grammar itself holds the invalid ring");
	}

	#[test]
	fn a_generation_mark_is_its_frozen_bytes() {
		check_frozen(&GenerationMark::new(g(7)), &[254, 0, 128, 128, 16, 38, 1, 0, 2, 135]);
		check_frozen(
			&GenerationMark::new(Generation::MAX),
			&[254, 0, 128, 128, 16, 38, 1, 0, 2, 4, 255, 255, 0],
		);
	}

	#[test]
	fn table_migrations_are_their_frozen_bytes() {
		check_frozen(
			&TableMigrations {
				floor: 0,
				base: g(3),
				head: g(9),
			},
			&[254, 0, 128, 128, 16, 38, 3, 0, 2, 4, 2, 128, 4, 131, 6, 137],
		);
		check_frozen(
			&TableMigrations {
				floor: 65_000,
				base: g(65_000),
				head: g(5),
			},
			&[254, 0, 128, 128, 16, 38, 3, 0, 5, 10, 2, 4, 232, 253, 0, 4, 4, 232, 253, 0, 6, 133],
		);
	}
}
