//! What is known about the type of a value.
//!
//! A [`TypeKnowledge::Fact`] is guaranteed by the language or a definition, and a consumer may
//! specialise on it. A [`TypeKnowledge::Hint`] is declared but not guaranteed: a consumer
//! specialises only behind a check and prepares the generic fallback. Knowledge a value holds in
//! its own data is language-level only (a literal, a function parameter, the result kind an op
//! guarantees); knowledge sourced from storage is derived by analysis, pinned to the bound
//! catalog version and a premise of every program that relies on it.

use surrealdb_strand::TableName;

use crate::expr::Kind;

/// The type knowledge of one value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TypeKnowledge {
	/// The language or a definition guarantees the kind.
	Fact {
		kind: Kind,
		source: FactSource,
	},
	/// The kind is declared but not guaranteed.
	Hint {
		kind: Kind,
		source: HintSource,
	},
	Unknown,
}

impl TypeKnowledge {
	/// Combines two descriptions of the same value. Each side is true of the value, so the result
	/// keeps what either side knows.
	///
	/// `Unknown` gives way to the other side, and a `Hint` to a `Fact`. Two facts of the same kind
	/// keep the first. Two facts of different kinds answer `Unknown`, so a consumer never
	/// specialises on a kind the other side does not state. Two different hints keep the first,
	/// which a consumer checks before relying on.
	pub(crate) fn meet(&self, other: &Self) -> Self {
		match (self, other) {
			(Self::Unknown, known) | (known, Self::Unknown) => known.clone(),
			(
				Self::Fact {
					kind,
					..
				},
				Self::Fact {
					kind: other_kind,
					..
				},
			) => {
				if kind == other_kind {
					self.clone()
				} else {
					Self::Unknown
				}
			}
			(
				Self::Fact {
					..
				},
				Self::Hint {
					..
				},
			) => self.clone(),
			(
				Self::Hint {
					..
				},
				Self::Fact {
					..
				},
			) => other.clone(),
			(
				Self::Hint {
					..
				},
				Self::Hint {
					..
				},
			) => self.clone(),
		}
	}
}

/// Why a kind is guaranteed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FactSource {
	Literal,
	FunctionParam,
	Definition,
	/// A strict table's rows, all migrated to the current definition.
	StrictRowMigrated {
		table: TableName,
	},
	Derived,
	/// The value is a record id of `table` because the loop source is a distinct scan of `table`
	/// that keeps `id`. Derived by analysis; holds without a catalog premise.
	Provenance {
		table: TableName,
	},
}

/// Why a kind is only declared.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum HintSource {
	/// A field definition of `table`, whose stored rows may predate it.
	DeclaredField {
		table: TableName,
	},
	/// A field of `table` while a migration holds more than one generation of its type.
	MidMigration {
		table: TableName,
	},
	Annotation,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn meet_keeps_what_either_side_knows() {
		let int = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Literal,
		};
		let hinted = TypeKnowledge::Hint {
			kind: Kind::String,
			source: HintSource::Annotation,
		};
		assert_eq!(int.meet(&int), int);
		assert_eq!(TypeKnowledge::Unknown.meet(&int), int);
		assert_eq!(int.meet(&TypeKnowledge::Unknown), int);
		assert_eq!(hinted.meet(&int), int, "a fact outranks a hint");
		assert_eq!(int.meet(&hinted), int, "a fact outranks a hint");
		assert_eq!(hinted.meet(&TypeKnowledge::Unknown), hinted);
		assert_eq!(TypeKnowledge::Unknown.meet(&TypeKnowledge::Unknown), TypeKnowledge::Unknown);
	}

	#[test]
	fn meet_of_two_facts_of_different_kinds_is_unknown() {
		let int = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Literal,
		};
		let derived_int = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Derived,
		};
		let string = TypeKnowledge::Fact {
			kind: Kind::String,
			source: FactSource::Definition,
		};
		assert_eq!(int.meet(&string), TypeKnowledge::Unknown);
		assert_eq!(string.meet(&int), TypeKnowledge::Unknown);
		assert_eq!(int.meet(&derived_int), int, "facts of one kind keep the first");
	}
}
