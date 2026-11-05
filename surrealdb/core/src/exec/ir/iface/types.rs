//! What is known about the type of a value.
//!
//! A [`TypeKnowledge::Fact`] is guaranteed by the language or a definition, and a consumer may
//! specialise on it. A [`TypeKnowledge::Hint`] is declared but not guaranteed: a consumer
//! specialises only behind a check and prepares the generic fallback. Knowledge a value holds in
//! its own data is language-level only (a literal, a function parameter, the result kind an op
//! guarantees); knowledge sourced from storage is derived by analysis, pinned to the bound
//! catalog version and a premise of every program that relies on it.
//!
//! A [`TypeKnowledge::Token`] is the type of an effect token: a compile-time value that orders
//! the ops touching one [`TokenClass`] of world state. It has no run-time representation.

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
	/// An effect token of one class. It exists only in the planner's containers and is erased
	/// before a program is lowered, so no register holds it.
	Token(TokenClass),
	Unknown,
}

impl TypeKnowledge {
	/// The class of the effect token this describes, or `None` for a value that is not one.
	pub(crate) fn token_class(&self) -> Option<TokenClass> {
		match self {
			Self::Token(class) => Some(*class),
			Self::Fact {
				..
			}
			| Self::Hint {
				..
			}
			| Self::Unknown => None,
		}
	}

	/// Combines two descriptions of the same value. Each side is true of the value, so the result
	/// keeps what either side knows.
	///
	/// `Unknown` gives way to the other side, and a `Hint` to a `Fact`. Two facts of the same kind
	/// keep the first. Two facts of different kinds answer `Unknown`, so a consumer never
	/// specialises on a kind the other side does not state. Two different hints keep the first,
	/// which a consumer checks before relying on.
	///
	/// Two tokens of one class are that token; a token and a kind describe no one value, so they
	/// answer `Unknown`.
	pub(crate) fn meet(&self, other: &Self) -> Self {
		match (self, other) {
			(Self::Unknown, known) | (known, Self::Unknown) => known.clone(),
			(Self::Token(a), Self::Token(b)) => {
				if a == b {
					self.clone()
				} else {
					Self::Unknown
				}
			}
			(
				Self::Token(_),
				Self::Fact {
					..
				}
				| Self::Hint {
					..
				},
			)
			| (
				Self::Fact {
					..
				}
				| Self::Hint {
					..
				},
				Self::Token(_),
			) => Self::Unknown,
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

/// One class of world state an effect token orders.
///
/// The clock is not a class: it is the statement-timestamp operand an op already takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum TokenClass {
	/// Stored data, including notifications and deferred work.
	Store,
	Catalog,
	Session,
	/// Randomness.
	Rng,
	/// Sequences and other state an evaluation advances.
	Seq,
	/// External I/O and blocking.
	Extern,
}

impl TokenClass {
	/// Every class, in the order tokens are threaded.
	pub(crate) const ALL: [TokenClass; 6] = [
		TokenClass::Store,
		TokenClass::Catalog,
		TokenClass::Session,
		TokenClass::Rng,
		TokenClass::Seq,
		TokenClass::Extern,
	];

	const fn bit(self) -> u8 {
		1 << self as u8
	}

	/// The class named `text` by [`TokenClass::name`].
	pub(crate) fn named(text: &str) -> Option<Self> {
		Self::ALL.into_iter().find(|class| class.name() == text)
	}

	/// The name the printer writes and the parser reads.
	pub(crate) const fn name(self) -> &'static str {
		match self {
			TokenClass::Store => "store",
			TokenClass::Catalog => "catalog",
			TokenClass::Session => "session",
			TokenClass::Rng => "rng",
			TokenClass::Seq => "seq",
			TokenClass::Extern => "extern",
		}
	}
}

/// A set of token classes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct TokenSet(u8);

impl TokenSet {
	pub(crate) const NONE: Self = Self(0);
	pub(crate) const ALL: Self = Self(
		TokenClass::Store.bit()
			| TokenClass::Catalog.bit()
			| TokenClass::Session.bit()
			| TokenClass::Rng.bit()
			| TokenClass::Seq.bit()
			| TokenClass::Extern.bit(),
	);

	pub(crate) const fn of(class: TokenClass) -> Self {
		Self(class.bit())
	}

	pub(crate) const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	pub(crate) const fn with(self, class: TokenClass) -> Self {
		Self(self.0 | class.bit())
	}

	pub(crate) const fn intersection(self, other: Self) -> Self {
		Self(self.0 & other.0)
	}

	pub(crate) const fn contains(self, class: TokenClass) -> bool {
		self.0 & class.bit() != 0
	}

	pub(crate) const fn intersects(self, other: Self) -> bool {
		self.0 & other.0 != 0
	}

	pub(crate) const fn is_empty(self) -> bool {
		self.0 == 0
	}

	/// The classes of the set, in the order tokens are threaded.
	pub(crate) fn iter(self) -> impl Iterator<Item = TokenClass> {
		TokenClass::ALL.into_iter().filter(move |class| self.contains(*class))
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

	#[test]
	fn a_token_meets_only_its_own_class() {
		let store = TypeKnowledge::Token(TokenClass::Store);
		let rng = TypeKnowledge::Token(TokenClass::Rng);
		let int = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Literal,
		};
		assert_eq!(store.meet(&store), store);
		assert_eq!(store.meet(&rng), TypeKnowledge::Unknown);
		assert_eq!(store.meet(&int), TypeKnowledge::Unknown);
		assert_eq!(int.meet(&store), TypeKnowledge::Unknown);
		assert_eq!(TypeKnowledge::Unknown.meet(&store), store);
		assert_eq!(store.token_class(), Some(TokenClass::Store));
		assert_eq!(int.token_class(), None);
	}

	#[test]
	fn the_classes_are_named_and_ordered_as_they_are_threaded() {
		for class in TokenClass::ALL {
			assert_eq!(TokenClass::named(class.name()), Some(class));
		}
		assert!(TokenClass::ALL.windows(2).all(|pair| pair[0] < pair[1]));
		let set = TokenSet::of(TokenClass::Seq).with(TokenClass::Store);
		assert_eq!(set.iter().collect::<Vec<_>>(), [TokenClass::Store, TokenClass::Seq]);
		assert!(TokenSet::ALL.iter().eq(TokenClass::ALL));
		assert!(TokenSet::NONE.is_empty());
	}
}
