//! What an op does: its effects, failure classes, footprint, rollback behaviour and determinism.
//!
//! [`OpEffects`] is the one answer every consumer that moves, merges, folds or drops an op reads.
//! Its facts are conservative: a field a producer cannot name is widened to the strongest value,
//! never omitted.

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::{Strand, TableName};

use super::fallibility::{ContextDep, Fallibility};
use super::ids::SnapshotId;
use super::types::{TokenClass, TokenSet};
use crate::exec::logical::footprint::CatalogKind;
use crate::expr::logical::EffectSet;

/// The effects of one op.
///
/// A local answer does not fold the ops in the op's regions; the transitive answer is an
/// analysis over the container.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpEffects {
	pub(crate) set: EffectSet,
	pub(crate) fallibility: Fallibility,
	/// Each dependency a `CONTEXT` failure rests on.
	pub(crate) context_deps: SmallVec<[ContextDep; 1]>,
	pub(crate) footprint: IrFootprint,
	pub(crate) rollback: Rollback,
	pub(crate) determinism: Determinism,
}

impl OpEffects {
	/// The answer for an op nothing is known about: every effect ([`EffectSet::TOP`], so a test of
	/// any flag or predicate finds it, a write included), failure for every dependency, reads and
	/// writes of [`IrResource::Unknown`], effects a rollback does not undo, and no equality with
	/// another evaluation of itself.
	pub(crate) fn top() -> Self {
		Self {
			set: EffectSet::TOP,
			fallibility: Fallibility::TOP,
			context_deps: SmallVec::new(),
			footprint: IrFootprint {
				reads: smallvec![IrResource::Unknown],
				writes: smallvec![IrResource::Unknown],
			},
			rollback: Rollback::Survives,
			determinism: Determinism::NonDeterministic,
		}
	}
}

/// The parts of an [`OpEffects`] answer that are `Copy`: the effect set, the failure classes and
/// the determinism.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EffectSummary {
	pub(crate) set: EffectSet,
	pub(crate) fallibility: Fallibility,
	pub(crate) determinism: Determinism,
}

impl EffectSummary {
	/// The summary of `effects`.
	pub(crate) fn of(effects: &OpEffects) -> Self {
		Self {
			set: effects.set,
			fallibility: effects.fallibility,
			determinism: effects.determinism,
		}
	}
}

/// The effect-token classes an op takes and produces.
///
/// An op that produces a class takes the class's incoming token as well, so `produces` is a subset
/// of `consumes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct TokenUse {
	pub(crate) consumes: TokenSet,
	pub(crate) produces: TokenSet,
}

impl TokenUse {
	/// A class the op reads and leaves unchanged.
	const fn reads(self, class: TokenClass) -> Self {
		Self {
			consumes: self.consumes.with(class),
			produces: self.produces,
		}
	}

	/// A class the op reads and changes.
	const fn advances(self, class: TokenClass) -> Self {
		Self {
			consumes: self.consumes.with(class),
			produces: self.produces.with(class),
		}
	}

	pub(crate) const fn union(self, other: Self) -> Self {
		Self {
			consumes: self.consumes.union(other.consumes),
			produces: self.produces.union(other.produces),
		}
	}
}

/// The flags a non-deterministic answer is explained by: an answer that is non-deterministic with
/// none of them is not understood, and orders against every class.
const EXPLAINING_FLAGS: EffectSet = EffectSet::WRITES_DATA
	.union(EffectSet::WRITES_EXTERNAL)
	.union(EffectSet::READS_EXTERNAL)
	.union(EffectSet::ADVANCES_STATE)
	.union(EffectSet::ENTROPY)
	.union(EffectSet::BLOCKING)
	.union(EffectSet::WRITES_SESSION)
	.union(EffectSet::NOTIFY)
	.union(EffectSet::DEFERS)
	.union(EffectSet::CHANGES_CATALOG);

/// The effect-token classes an answer takes and produces: the one place a flag becomes a token.
///
/// - Reading stored data takes `Store`; writing it, notifying and deferring take and produce it.
/// - A catalog read takes `Catalog`; a catalog change takes and produces `Catalog` and `Store`.
/// - The session database is read through `Session` and the session written through it.
/// - Entropy is `Rng`, an advancing counter is `Seq`, and external reads and writes and waiting are
///   `Extern`.
/// - A body nothing is known about, and an answer that is non-deterministic for no flag it names,
///   takes and produces every class.
///
/// The clock, the row, the principal and a possible rejection take no token. The clock is the
/// statement timestamp an op reads as an operand, the row is a block argument, the principal is
/// constant for the request, and a rejection is a failure class, not state.
pub(crate) fn token_use(set: EffectSet, determinism: Determinism) -> TokenUse {
	let mut used = TokenUse::default();
	if set.contains(EffectSet::OPAQUE_BODY)
		|| (determinism == Determinism::NonDeterministic && !set.intersects(EXPLAINING_FLAGS))
	{
		return TokenUse {
			consumes: TokenSet::ALL,
			produces: TokenSet::ALL,
		};
	}
	if set.contains(EffectSet::READS_DATA) {
		used = used.reads(TokenClass::Store);
	}
	if set.intersects(EffectSet::WRITES_DATA.union(EffectSet::NOTIFY).union(EffectSet::DEFERS)) {
		used = used.advances(TokenClass::Store);
	}
	if set.contains(EffectSet::READS_CATALOG) {
		used = used.reads(TokenClass::Catalog);
	}
	if set.contains(EffectSet::CHANGES_CATALOG) {
		used = used.advances(TokenClass::Catalog).advances(TokenClass::Store);
	}
	if set.contains(EffectSet::READS_SESSION_DB) {
		used = used.reads(TokenClass::Session);
	}
	if set.contains(EffectSet::WRITES_SESSION) {
		used = used.advances(TokenClass::Session);
	}
	if set.contains(EffectSet::ENTROPY) {
		used = used.advances(TokenClass::Rng);
	}
	if set.contains(EffectSet::ADVANCES_STATE) {
		used = used.advances(TokenClass::Seq);
	}
	if set.intersects(
		EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL).union(EffectSet::BLOCKING),
	) {
		used = used.advances(TokenClass::Extern);
	}
	used
}

/// Whether the effects of an op are undone when the enclosing transaction or block rolls back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Rollback {
	NoEffects,
	/// Every effect is undone: KV writes, catalog writes, transactional async-event queue entries.
	Undone,
	/// Some effect survives: external calls, opaque bodies, notifications, sequence allocation.
	Survives,
}

/// How far two evaluations of an op may be treated as one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Determinism {
	Deterministic,
	/// Reads stored data: equal answers within one statement only where no write of the
	/// statement reaches the footprint.
	WithinStatementUnlessWritten,
	/// A clock read: one value per statement, whether the statement is a top-level one or one of a
	/// statement list.
	PerStatement,
	NonDeterministic,
}

/// The resources an op reads and writes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct IrFootprint {
	pub(crate) reads: SmallVec<[IrResource; 2]>,
	pub(crate) writes: SmallVec<[IrResource; 2]>,
}

/// Something an op may read or write.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum IrResource {
	/// Records of `table` (every table when `None`): the `fields` of the records `key` names.
	Data {
		table: Option<TableName>,
		fields: FieldSet,
		key: KeyOf,
	},
	/// A catalog definition of `kind`: on `table` where it belongs to one (every table when
	/// `None`), named `name` (every name when `None`).
	Catalog {
		kind: CatalogKind,
		table: Option<TableName>,
		name: Option<Strand>,
	},
	/// The snapshot a versioned read observes.
	Snapshot(SnapshotId),
	/// The writes a table's references cascade to.
	Cascade {
		table: TableName,
	},
	/// The rejection a table's constraints may raise.
	Reject {
		table: TableName,
	},
	/// The queue an event's async work is appended to.
	Queue {
		event: Strand,
	},
	/// A session variable.
	Session {
		name: Strand,
	},
	/// Anything at all.
	Unknown,
}

/// The fields of a record an access touches.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FieldSet {
	/// Every field. Used when a producer cannot name the fields it touches.
	All,
	Some(SmallVec<[Strand; 4]>),
}

/// The record key an iteration of a loop touches, as a path from the loop's element.
///
/// `Any` when no path is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum KeyOf {
	Any,
	Elem(ElemPath),
}

/// The path from a loop's element to its identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ElemPath {
	/// The `id` of a record stream: the rows of a table, index or range source.
	Id,
	/// The element itself, for a value stream: a set, an integer range, a deduplicated array,
	/// `SELECT VALUE id`.
	Elem,
}

/// Whether a delivered stream can hold one identity twice along its [`ElemPath`].
///
/// A fact about the delivered stream of a loop source, after every operator of the source. It is
/// derived by analysis and never stored on an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Multiplicity {
	Distinct,
	MayRepeat,
}

#[cfg(test)]
mod tests {
	use super::*;

	fn used(set: EffectSet) -> TokenUse {
		let determinism = if set.is_deterministic() {
			Determinism::Deterministic
		} else {
			Determinism::NonDeterministic
		};
		token_use(set, determinism)
	}

	fn classes(set: TokenSet) -> Vec<TokenClass> {
		set.iter().collect()
	}

	#[test]
	fn a_pure_set_takes_no_token() {
		assert_eq!(used(EffectSet::PURE), TokenUse::default());
	}

	#[test]
	fn the_clock_the_row_the_principal_and_a_rejection_take_no_token() {
		for flag in [
			EffectSet::CLOCK,
			EffectSet::READS_ROW,
			EffectSet::READS_PRINCIPAL,
			EffectSet::MAY_REJECT,
		] {
			let determinism = if flag == EffectSet::CLOCK {
				Determinism::PerStatement
			} else {
				Determinism::Deterministic
			};
			assert_eq!(token_use(flag, determinism), TokenUse::default(), "{flag:?}");
		}
	}

	#[test]
	fn a_read_takes_its_class_and_leaves_it() {
		let data = used(EffectSet::READS_DATA);
		assert_eq!(classes(data.consumes), [TokenClass::Store]);
		assert!(data.produces.is_empty());
		let catalog = used(EffectSet::READS_CATALOG);
		assert_eq!(classes(catalog.consumes), [TokenClass::Catalog]);
		assert!(catalog.produces.is_empty());
		let session = used(EffectSet::READS_SESSION_DB);
		assert_eq!(classes(session.consumes), [TokenClass::Session]);
		assert!(session.produces.is_empty());
	}

	#[test]
	fn a_free_parameter_orders_against_the_catalog_alone() {
		let free = used(EffectSet::READS_CATALOG | EffectSet::READS_PRINCIPAL);
		assert_eq!(classes(free.consumes), [TokenClass::Catalog]);
		assert!(free.produces.is_empty());
	}

	#[test]
	fn a_change_takes_and_produces_its_class() {
		for (flag, class) in [
			(EffectSet::WRITES_DATA, TokenClass::Store),
			(EffectSet::NOTIFY, TokenClass::Store),
			(EffectSet::DEFERS, TokenClass::Store),
			(EffectSet::WRITES_SESSION, TokenClass::Session),
			(EffectSet::ENTROPY, TokenClass::Rng),
			(EffectSet::ADVANCES_STATE, TokenClass::Seq),
			(EffectSet::READS_EXTERNAL, TokenClass::Extern),
			(EffectSet::WRITES_EXTERNAL, TokenClass::Extern),
			(EffectSet::BLOCKING, TokenClass::Extern),
		] {
			let answer = used(flag);
			assert_eq!(classes(answer.consumes), [class], "{flag:?}");
			assert_eq!(answer.consumes, answer.produces, "{flag:?}");
		}
	}

	#[test]
	fn a_catalog_change_takes_and_produces_the_catalog_and_the_store() {
		let answer = used(EffectSet::CHANGES_CATALOG);
		assert_eq!(classes(answer.consumes), [TokenClass::Store, TokenClass::Catalog]);
		assert_eq!(answer.consumes, answer.produces);
	}

	#[test]
	fn an_opaque_or_unexplained_answer_orders_against_every_class() {
		let every = TokenUse {
			consumes: TokenSet::ALL,
			produces: TokenSet::ALL,
		};
		assert_eq!(used(EffectSet::OPAQUE_BODY), every);
		assert_eq!(token_use(EffectSet::PURE, Determinism::NonDeterministic), every);
		assert_eq!(token_use(EffectSet::TOP, Determinism::NonDeterministic), every);
	}

	#[test]
	fn a_produced_class_is_always_taken() {
		for flag in [
			EffectSet::WRITES_DATA,
			EffectSet::WRITES_EXTERNAL,
			EffectSet::READS_EXTERNAL,
			EffectSet::ADVANCES_STATE,
			EffectSet::ENTROPY,
			EffectSet::CLOCK,
			EffectSet::BLOCKING,
			EffectSet::OPAQUE_BODY,
			EffectSet::READS_DATA,
			EffectSet::READS_CATALOG,
			EffectSet::READS_ROW,
			EffectSet::READS_PRINCIPAL,
			EffectSet::READS_SESSION_DB,
			EffectSet::WRITES_SESSION,
			EffectSet::NOTIFY,
			EffectSet::DEFERS,
			EffectSet::MAY_REJECT,
			EffectSet::CHANGES_CATALOG,
		] {
			let answer = used(flag);
			assert_eq!(answer.produces.union(answer.consumes), answer.consumes, "{flag:?}");
		}
	}

	#[test]
	fn the_sets_of_flags_union_their_classes() {
		let answer = used(EffectSet::READS_DATA | EffectSet::ENTROPY | EffectSet::READS_CATALOG);
		assert_eq!(
			classes(answer.consumes),
			[TokenClass::Store, TokenClass::Catalog, TokenClass::Rng]
		);
		assert_eq!(classes(answer.produces), [TokenClass::Rng]);
	}
}
