//! Identifiers of the entries in the datastore's feature ledger, stored at `/!mg{id}`.
//!
//! An entry's identifier carries its compatibility class in its top two bits, so the class is
//! readable from the key alone, before the value is fetched or decoded. A release that meets an
//! identifier it does not know reads the class off the key and decides what it may still do:
//!
//! - [`LedgerClass::Incompatible`]: the entry records bytes this release cannot read or maintain,
//!   so it must refuse to open the store.
//! - [`LedgerClass::Compatible`]: the entry records bytes every release can ignore, so it may open
//!   the store read-write.
//! - [`LedgerClass::ReadOnlyCompatible`]: the entry records bytes this release can read but not
//!   maintain, so it may open the store read-only.
//! - [`LedgerClass::Reserved`]: the class bits `11` are not assigned. A reader treats an identifier
//!   with them as incompatible.
//!
//! The low thirty bits are a sequence number that is unique across classes, so an identifier
//! never changes class and two entries never share a sequence.

/// How a release that does not know a ledger entry may treat the store that holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LedgerClass {
	/// The release must not open the store.
	Incompatible,
	/// The release may open the store read-write.
	Compatible,
	/// The release may open the store read-only.
	ReadOnlyCompatible,
	/// An unassigned class; read as [`LedgerClass::Incompatible`].
	Reserved,
}

/// The number of low bits that hold the sequence.
const SEQUENCE_BITS: u32 = 30;
/// The mask of the sequence bits.
const SEQUENCE_MASK: u32 = (1 << SEQUENCE_BITS) - 1;

/// The identifier of a ledger entry: the class in bits 31-30, the sequence in bits 29-0.
///
/// The raw value is the `u32` that the key `/!mg{id}` carries as four big-endian bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LedgerId(u32);

impl LedgerId {
	/// The identifier with the given class and sequence, for code constants.
	///
	/// Panics in const evaluation when the sequence does not fit in 30 bits or the class is
	/// [`LedgerClass::Reserved`], so an invalid constant does not compile.
	pub const fn new(class: LedgerClass, sequence: u32) -> Self {
		assert!(sequence <= SEQUENCE_MASK, "a ledger sequence is at most 30 bits");
		let class_bits: u32 = match class {
			LedgerClass::Incompatible => 0b00,
			LedgerClass::Compatible => 0b01,
			LedgerClass::ReadOnlyCompatible => 0b10,
			LedgerClass::Reserved => panic!("the reserved ledger class cannot be minted"),
		};
		Self((class_bits << SEQUENCE_BITS) | sequence)
	}

	/// The identifier a stored key carries.
	pub const fn from_raw(raw: u32) -> Self {
		Self(raw)
	}

	/// The `u32` the key carries.
	pub const fn raw(self) -> u32 {
		self.0
	}

	/// The class in bits 31-30.
	pub const fn class(self) -> LedgerClass {
		match self.0 >> SEQUENCE_BITS {
			0b00 => LedgerClass::Incompatible,
			0b01 => LedgerClass::Compatible,
			0b10 => LedgerClass::ReadOnlyCompatible,
			_ => LedgerClass::Reserved,
		}
	}

	/// The sequence in bits 29-0.
	pub const fn sequence(self) -> u32 {
		self.0 & SEQUENCE_MASK
	}
}

/// The entry that records that records and catalog entries may be stored in the stored-value
/// grammar.
///
/// It is written in the transaction that first stores bytes a 3.x release cannot read, or in the
/// transaction that raises the feature level, whichever comes first. A release that does not know
/// it refuses the store as incompatible.
pub const RECORD_FORMAT_V1: LedgerId = LedgerId::new(LedgerClass::Incompatible, 7);

/// The name recorded in the [`RECORD_FORMAT_V1`] entry.
pub const RECORD_FORMAT_V1_NAME: &str =
	"records and catalog entries may be stored in the stored-value grammar";

/// The release that introduced the [`RECORD_FORMAT_V1`] entry, as recorded in it.
pub const RECORD_FORMAT_V1_VERSION: (u64, u64, u64) = (3, 4, 0);
