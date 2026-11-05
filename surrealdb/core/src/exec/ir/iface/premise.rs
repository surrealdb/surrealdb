//! Cache keys and premises.
//!
//! A program is cached under a [`ProgramKey`] and served only while every [`Premise`] it relied
//! on still holds. Nothing derived from a principal's values enters a key or a premise: a program
//! into which a principal-derived value was substituted is marked uncacheable and is never
//! stored.
//!
//! Premises are checked per statement unit, before the statement runs, and never inside a running
//! program. A failed check compiles the statement normally and replaces the entry.

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::region_kind::StatementCommit;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::vm::compile_cache::{BuildId, ObjectVersion};
use crate::exec::vm::program::UnitFrame;
use crate::expr::logical::EvalMode;

/// The canonical structural digest of an unbound statement, with parameters as slots.
///
/// Computed from the statement text before binding, so it never holds an id of one container.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ShapeDigest(pub(crate) [u8; 32]);

/// A catalog object, named independently of any container.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CatalogObjectKey {
	pub(crate) kind: CatalogKind,
	pub(crate) ns: Option<NamespaceId>,
	pub(crate) db: Option<DatabaseId>,
	pub(crate) table: Option<TableName>,
	pub(crate) name: Option<Strand>,
}

/// The versions of the catalog objects a program was planned against, sorted by object with each
/// object at most once.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct CatalogVersionVector(SmallVec<[(CatalogObjectKey, ObjectVersion); 4]>);

impl CatalogVersionVector {
	/// The vector holding no object.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Records `object` at `version`, replacing the version it already held.
	pub(crate) fn insert(&mut self, object: CatalogObjectKey, version: ObjectVersion) {
		let key = object.canonical();
		match self.0.binary_search_by(|(held, _)| held.canonical().cmp(&key)) {
			Ok(at) => self.0[at].1 = version,
			Err(at) => self.0.insert(at, (object, version)),
		}
	}

	/// The recorded objects and versions, in canonical order.
	pub(crate) fn iter(&self) -> impl Iterator<Item = &(CatalogObjectKey, ObjectVersion)> {
		self.0.iter()
	}
}

/// The namespace and database a program is planned in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CatalogScope {
	pub(crate) ns: Option<NamespaceId>,
	pub(crate) db: Option<DatabaseId>,
}

/// The compiled-unit frame and the evaluation mode a program is compiled for.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FrameClass {
	pub(crate) unit: UnitFrame,
	pub(crate) eval: EvalMode,
}

/// The transaction a statement is compiled inside.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TxnContext {
	None,
	Explicit,
	Session,
}

/// The regions an earlier batch of the same stream left open, and whether this batch continues
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpenRegionShape {
	pub(crate) open: TxnContext,
	pub(crate) continues: bool,
}

/// The region context a statement is compiled in. A program compiled outside a session
/// transaction is never served inside one, and the reverse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RegionContext {
	pub(crate) commit: StatementCommit,
	pub(crate) txn: TxnContext,
	pub(crate) stack_shape: OpenRegionShape,
}

/// What a cached program is keyed by.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ProgramKey {
	pub(crate) shape: ShapeDigest,
	pub(crate) scope: CatalogScope,
	pub(crate) catalog: CatalogVersionVector,
	pub(crate) build: BuildId,
	pub(crate) frame: FrameClass,
	pub(crate) context: RegionContext,
}

/// A parameter slot of a statement shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SlotId(pub(crate) u16);

/// A bounded bucket of a parameter's value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueBucket(pub(crate) u8);

/// A fact a program was planned on and is served only while it still holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Premise {
	/// A catalog object read at `version`.
	Catalog {
		object: CatalogObjectKey,
		version: ObjectVersion,
	},
	/// A catalog object that did not exist, such as a parameter substituted as `NONE`.
	CatalogAbsent {
		object: CatalogObjectKey,
	},
	/// A substituted bind, `LET` or session value that does not derive from the principal.
	ContextValue {
		name: Strand,
		value_digest: [u8; 16],
	},
	/// A bucketed parameter value.
	ValueBucket {
		slot: SlotId,
		bucket: ValueBucket,
	},
	/// A statistics epoch of the physical catalog.
	StatsEpoch(u64),
}

/// The premises of one program, sorted by canonical encoding and without duplicates, and whether
/// the program may be stored at all.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct PremiseSet {
	premises: SmallVec<[Premise; 8]>,
	uncacheable: bool,
}

impl PremiseSet {
	/// The set holding no premise, of a program that may be stored.
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Adds `premise`. A premise already held is not added twice.
	pub(crate) fn push(&mut self, premise: Premise) {
		let key = premise.canonical();
		if let Err(at) = self.premises.binary_search_by(|held| held.canonical().cmp(&key)) {
			self.premises.insert(at, premise);
		}
	}

	/// Marks the program as never to be stored: it holds a value no premise may name.
	pub(crate) fn mark_uncacheable(&mut self) {
		self.uncacheable = true;
	}

	/// Whether the program must not be stored.
	pub(crate) fn is_uncacheable(&self) -> bool {
		self.uncacheable
	}

	/// The premises, in canonical order.
	pub(crate) fn iter(&self) -> impl Iterator<Item = &Premise> {
		self.premises.iter()
	}
}

/// The outcome of checking a premise set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PremiseCheck {
	Holds,
	/// The first premise, in canonical order, that no longer holds.
	Fails {
		first: Premise,
	},
}

/// Checks premises against the current catalog without reading storage.
pub(crate) trait PremiseSource {
	fn check(&self, premises: &PremiseSet) -> PremiseCheck;
}

impl CatalogObjectKey {
	/// The canonical byte encoding the sorted collections of this module order by. Injective:
	/// equal encodings are equal keys.
	fn canonical(&self) -> Vec<u8> {
		let mut out = Vec::new();
		self.encode(&mut out);
		out
	}

	fn encode(&self, out: &mut Vec<u8>) {
		out.push(match self.kind {
			CatalogKind::Table => 0,
			CatalogKind::Field => 1,
			CatalogKind::Index => 2,
			CatalogKind::Event => 3,
			CatalogKind::View => 4,
			CatalogKind::Function => 5,
			CatalogKind::Param => 6,
			CatalogKind::Access => 7,
			CatalogKind::Other => 8,
		});
		encode_option(out, self.ns.map(|ns| ns.0), |out, id| {
			out.extend_from_slice(&id.to_le_bytes())
		});
		encode_option(out, self.db.map(|db| db.0), |out, id| {
			out.extend_from_slice(&id.to_le_bytes())
		});
		encode_option(out, self.table.as_ref().map(TableName::as_str), encode_str);
		encode_option(out, self.name.as_deref(), encode_str);
	}
}

impl Premise {
	/// The canonical byte encoding premises are ordered by. Injective: equal encodings are equal
	/// premises.
	fn canonical(&self) -> Vec<u8> {
		let mut out = Vec::new();
		match self {
			Self::Catalog {
				object,
				version,
			} => {
				out.push(0);
				object.encode(&mut out);
				encode_version(&mut out, version);
			}
			Self::CatalogAbsent {
				object,
			} => {
				out.push(1);
				object.encode(&mut out);
			}
			Self::ContextValue {
				name,
				value_digest,
			} => {
				out.push(2);
				encode_str(&mut out, name);
				out.extend_from_slice(value_digest);
			}
			Self::ValueBucket {
				slot,
				bucket,
			} => {
				out.push(3);
				out.extend_from_slice(&slot.0.to_le_bytes());
				out.push(bucket.0);
			}
			Self::StatsEpoch(epoch) => {
				out.push(4);
				out.extend_from_slice(&epoch.to_le_bytes());
			}
		}
		out
	}
}

fn encode_str(out: &mut Vec<u8>, s: &str) {
	out.extend_from_slice(&(s.len() as u32).to_le_bytes());
	out.extend_from_slice(s.as_bytes());
}

fn encode_option<T>(out: &mut Vec<u8>, value: Option<T>, encode: impl FnOnce(&mut Vec<u8>, T)) {
	match value {
		Some(value) => {
			out.push(1);
			encode(out, value);
		}
		None => out.push(0),
	}
}

fn encode_version(out: &mut Vec<u8>, version: &ObjectVersion) {
	match version {
		ObjectVersion::Stamped(stamp) => {
			out.push(0);
			out.extend_from_slice(stamp.as_bytes());
		}
		ObjectVersion::Content(hash) => {
			out.push(1);
			out.extend_from_slice(&hash.to_le_bytes());
		}
		ObjectVersion::Inline => out.push(2),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn object(name: &str) -> CatalogObjectKey {
		CatalogObjectKey {
			kind: CatalogKind::Function,
			ns: None,
			db: None,
			table: None,
			name: Some(Strand::from(name)),
		}
	}

	#[test]
	fn premises_are_sorted_and_deduplicated() {
		let mut set = PremiseSet::new();
		set.push(Premise::StatsEpoch(2));
		set.push(Premise::Catalog {
			object: object("b"),
			version: ObjectVersion::Content(1),
		});
		set.push(Premise::StatsEpoch(2));
		set.push(Premise::Catalog {
			object: object("a"),
			version: ObjectVersion::Content(1),
		});
		let held: Vec<_> = set.iter().cloned().collect();
		assert_eq!(held.len(), 3);
		let mut again = PremiseSet::new();
		for premise in held.iter().rev() {
			again.push(premise.clone());
		}
		assert_eq!(set, again);
	}

	#[test]
	fn uncacheable_is_sticky() {
		let mut set = PremiseSet::new();
		assert!(!set.is_uncacheable());
		set.mark_uncacheable();
		assert!(set.is_uncacheable());
	}

	#[test]
	fn version_vector_holds_each_object_once() {
		let mut vector = CatalogVersionVector::new();
		vector.insert(object("a"), ObjectVersion::Content(1));
		vector.insert(object("a"), ObjectVersion::Content(2));
		assert_eq!(vector.iter().count(), 1);
		assert_eq!(vector.iter().next().map(|(_, v)| *v), Some(ObjectVersion::Content(2)));
	}
}
