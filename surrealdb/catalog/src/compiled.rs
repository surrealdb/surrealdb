//! The [`FromStored`] bridge between stored and runtime catalog forms.
//!
//! Stored definitions hold user intent in public-stable forms: scalars,
//! vocabulary enums, and canonical SurrealQL text ([`SurqlText`](super::SurqlText))
//! for everything the user wrote as an expression, type, idiom, or clause. The
//! engine, however, works on parsed ASTs. [`FromStored`] is the ONLY sanctioned
//! bridge between the two: every runtime definition builds from its stored
//! counterpart through it, its impl is a flat field-by-field map (each text field
//! compiled through its [`SurqlTarget`](super::SurqlTarget) impl, scalars
//! cloned), and no other code may parse stored definition text.
//!
//! Compiled forms are FULL-FIDELITY equivalents of their stored forms: every
//! stored field is carried over (display metadata like comments and GraphQL
//! aliases included), with text fields parsed to ASTs and everything else
//! cloned. A compiled definition can therefore serve every reader, including
//! INFO/export rendering, and can be rendered back to its stored form without
//! loss.
//!
//! Each runtime definition type, its inherent helpers, and its
//! `FromStored`/`to_stored` impls live in the same file as its `Stored*` twin.
//! This module holds only the shared machinery: the [`FromStored`] trait, the
//! [`from_stored_all`] cache-fill helper, and the `compile_as_self!` impls for
//! the definitions that carry no SurrealQL text (their runtime and stored forms
//! are identical).

use std::sync::Arc;

/// Builds a runtime catalog form from its stored (canonical-text) counterpart.
///
/// The stored-to-runtime direction is the only sanctioned way to interpret
/// stored SurrealQL text; consumers must never parse definition text ad hoc.
/// A parse failure indicates a stored-catalog invariant violation, so callers
/// either propagate the error or choose a locally safe fallback.
pub trait FromStored: Sized {
	/// The stored counterpart this runtime form is built from.
	type Stored;
	/// Parse every stored-text field back to its AST form; clone scalars.
	fn from_stored(stored: &Self::Stored) -> anyhow::Result<Self>;
}

/// Build every runtime definition in a stored slice.
///
/// Convenience for cache-fill sites that hold `Arc<[T::Stored]>` collections of
/// stored definitions and cache the runtime forms alongside them.
pub fn from_stored_all<T: FromStored>(stored: &[T::Stored]) -> anyhow::Result<Arc<[T]>> {
	stored.iter().map(T::from_stored).collect::<anyhow::Result<Vec<_>>>().map(Arc::from)
}

/// The remaining stored definitions carry no SurrealQL text; their runtime
/// form is themselves. Implemented so the trait's contract ("every catalog
/// item builds from its stored form") is universal rather than per-type
/// folklore.
macro_rules! compile_as_self {
	($($ty:ty),+ $(,)?) => {
		$(
			impl FromStored for $ty {
				type Stored = $ty;

				fn from_stored(stored: &$ty) -> anyhow::Result<$ty> {
					Ok(stored.clone())
				}
			}
		)+
	};
}

compile_as_self!(
	crate::catalog::NamespaceDefinition,
	crate::catalog::DatabaseDefinition,
	crate::catalog::UserDefinition,
	crate::catalog::AnalyzerDefinition,
	crate::catalog::SequenceDefinition,
	crate::catalog::AccessGrant,
	crate::catalog::DefaultConfig,
);
