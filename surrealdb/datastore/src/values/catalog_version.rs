//! Catalog versions: the per-table version stored under `!cv{tb}`, and the
//! per-object version stored under `!ov{kind}{name}` for every other catalog
//! object compiled code relies on.
//!
//! A table's value is rewritten by every catalog write that changes that
//! table's definition, its fields, its indexes, its events or the views that
//! read it. An object's value is rewritten by every write or removal that
//! changes its definition key. Readers use a version as the identity of what it covers:
//! two transactions that read the same value see the same catalog state, so
//! what one built from it can be served to the other. Writes to different
//! objects touch different keys, so they never conflict with each other over
//! a version. What a version is *not* is a counter — see
//! [`CatalogVersion::fresh`].
//!
//! A build that predates these versions writes definitions without moving
//! them, and may share the store with this one during a rolling upgrade. So a
//! version also carries the print of the definition key it was stamped
//! beside ([`CatalogVersion::print`]), and names a state only while that key
//! still holds the bytes it was printed from ([`CatalogVersion::proven`]): a
//! definition rewritten or removed by an older build leaves the print
//! stale, and the version proves nothing until a write by this build stamps
//! it again.

use std::io;

use revision::revisioned;
use surrealdb_kvs::impl_kv_value_revisioned;
use surrealdb_strand::TableName;
use uuid::Uuid;

/// The identity of one catalog object's entry at one point in its history.
///
/// A fresh random identifier rather than an incremented count, because a
/// rolled-back transaction must not be able to leave behind a version a later
/// commit reuses: with a counter, an aborted `DEFINE` that bumped `n` to `n+1`
/// and a committed one that also bumped `n` to `n+1` would be told apart by
/// nothing, and a snapshot built under the aborted transaction's view would be
/// served for the committed catalog. Two writers can never mint the same uuid,
/// so a version identifies exactly the catalog state the transaction that wrote
/// it could see.
///
/// The second field is the [`print`](Self::print) of the definition key as it
/// stood when the version was stamped. A value with no print proves nothing.
#[revisioned(revision = 2)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CatalogVersion(
	pub Uuid,
	/// The print of the definition key the version was stamped beside.
	#[revision(start = 2)]
	pub Option<u128>,
);

impl_kv_value_revisioned!(CatalogVersion);

impl CatalogVersion {
	/// Mints the version a catalog write stamps on the object it changes,
	/// beside a definition key holding `stored` (`None` where it holds
	/// nothing).
	pub fn fresh(stored: Option<&[u8]>) -> Self {
		Self(Uuid::now_v7(), Some(Self::print(stored)))
	}

	/// `version`, stamped beside a definition key holding `stored`.
	pub fn stamped(version: Uuid, stored: Option<&[u8]>) -> Self {
		Self(version, Some(Self::print(stored)))
	}

	/// The print of a definition key's stored bytes, or of its absence: a
	/// 128-bit BLAKE3 digest, stable across builds and platforms. Two
	/// prints are equal only for the same bytes, or both for no value.
	pub fn print(stored: Option<&[u8]>) -> u128 {
		let mut hasher = blake3::Hasher::new();
		match stored {
			Some(bytes) => {
				hasher.update(&[1]);
				hasher.update(bytes);
			}
			None => {
				hasher.update(&[0]);
			}
		}
		let digest = hasher.finalize();
		let mut print = [0u8; 16];
		print.copy_from_slice(&digest.as_bytes()[..16]);
		u128::from_le_bytes(print)
	}

	/// The version, where the definition key it was stamped beside still
	/// holds `stored`: only then does it name the definition's state. `None`
	/// where the key has been written or removed since without a stamp, as
	/// a build that predates versions does, or where the value carries no
	/// print.
	pub fn proven(&self, stored: Option<&[u8]>) -> Option<Uuid> {
		(self.1 == Some(Self::print(stored))).then_some(self.0)
	}
}

/// The kind of a database-scoped catalog object that carries a version of its
/// own under `!ov` ([`CatalogObject`]).
///
/// Encodes itself, as one byte: the discriminants on disk are fixed here and
/// never renumbered, since they are part of the `!ov` key.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum CatalogObjectKind {
	Function,
	Param,
	Access,
	Model,
	Module,
	Analyzer,
	Sequence,
	Api,
}

impl CatalogObjectKind {
	/// Every kind, in discriminant order.
	pub const ALL: [CatalogObjectKind; 8] = [
		Self::Function,
		Self::Param,
		Self::Access,
		Self::Model,
		Self::Module,
		Self::Analyzer,
		Self::Sequence,
		Self::Api,
	];

	fn discriminant(self) -> u8 {
		match self {
			Self::Function => 0,
			Self::Param => 1,
			Self::Access => 2,
			Self::Model => 3,
			Self::Module => 4,
			Self::Analyzer => 5,
			Self::Sequence => 6,
			Self::Api => 7,
		}
	}
}

impl storekey::Encode for CatalogObjectKind {
	fn encode<W: io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		w.write_u8(self.discriminant())
	}
}

impl<'de> storekey::BorrowDecode<'de> for CatalogObjectKind {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		let w = r.read_u8()?;
		Self::ALL
			.into_iter()
			.find(|kind| kind.discriminant() == w)
			.ok_or(storekey::DecodeError::InvalidFormat)
	}
}

/// A catalog object whose version a reader can ask for
/// ([`crate::Transaction::catalog_object_version`]).
///
/// A table's version (`!cv`) covers its definition, fields, indexes, events
/// and the views that read it. Every other kind is one definition key, and is
/// versioned under `!ov` by kind and by the name its definition key spells:
/// the function name without `fn::`, a module's storage name, a model's name
/// and version. Access methods are the database's own (`DEFINE ACCESS … ON
/// DATABASE`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogObject<'a> {
	Table(&'a TableName),
	Function(&'a str),
	Param(&'a str),
	Access(&'a str),
	Model {
		name: &'a str,
		version: &'a str,
	},
	Module(&'a str),
	Analyzer(&'a str),
	Sequence(&'a str),
	Api(&'a str),
}

impl<'a> CatalogObject<'a> {
	/// The object's kind and the two name fields of its `!ov` key, or, for a
	/// table, which is versioned under `!cv` instead, the table's name. Only a
	/// model uses the second name field; every other kind leaves it empty.
	pub fn versioned_as(&self) -> Result<(CatalogObjectKind, &'a str, &'a str), &'a TableName> {
		Ok(match *self {
			Self::Table(tb) => return Err(tb),
			Self::Function(name) => (CatalogObjectKind::Function, name, ""),
			Self::Param(name) => (CatalogObjectKind::Param, name, ""),
			Self::Access(name) => (CatalogObjectKind::Access, name, ""),
			Self::Model {
				name,
				version,
			} => (CatalogObjectKind::Model, name, version),
			Self::Module(name) => (CatalogObjectKind::Module, name, ""),
			Self::Analyzer(name) => (CatalogObjectKind::Analyzer, name, ""),
			Self::Sequence(name) => (CatalogObjectKind::Sequence, name, ""),
			Self::Api(name) => (CatalogObjectKind::Api, name, ""),
		})
	}
}
