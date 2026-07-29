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
pub(crate) trait FromStored: Sized {
	/// The stored counterpart this runtime form is built from.
	type Stored;
	/// Parse every stored-text field back to its AST form; clone scalars.
	fn from_stored(stored: &Self::Stored) -> anyhow::Result<Self>;
}

/// Build every runtime definition in a stored slice.
///
/// Convenience for cache-fill sites that hold `Arc<[T::Stored]>` collections of
/// stored definitions and cache the runtime forms alongside them.
pub(crate) fn from_stored_all<T: FromStored>(stored: &[T::Stored]) -> anyhow::Result<Arc<[T]>> {
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

#[cfg(test)]
mod tests {
	use crate::catalog::compat::fixtures as fix;
	use crate::catalog::{
		AccessDefinition, ApiDefinition, BucketDefinition, ConfigDefinition, EventDefinition,
		FieldDefinition, FromStored, FunctionDefinition, IndexDefinition, MlModelDefinition,
		ModuleDefinition, ParamDefinition, SubscriptionDefinition, TableDefinition,
	};

	/// The reverse render must invert `from_stored` exactly: building a runtime
	/// definition, rendering it back to its stored form, and building from that
	/// again yields the same runtime value. (Byte-equality of the stored forms
	/// themselves additionally requires the input text to be canonical, which
	/// decode convert_fns guarantee but hand-written fixtures need not.)
	#[test]
	fn compile_of_to_stored_is_identity() {
		macro_rules! check {
			($($ty:ty : $fixture:expr),+ $(,)?) => {
				$(
					let compiled = <$ty>::from_stored(&$fixture).unwrap();
					assert_eq!(
						<$ty>::from_stored(&compiled.to_stored()).unwrap(),
						compiled,
						"from_stored ∘ to_stored diverged for {}",
						stringify!($fixture),
					);
				)+
			};
		}
		check!(
			TableDefinition: fix::table_basic(),
			TableDefinition: fix::table_with_view(),
			TableDefinition: fix::table_relation(),
			TableDefinition: fix::table_with_materialized_view(),
			FieldDefinition: fix::field_basic(),
			FieldDefinition: fix::field_with_type(),
			FieldDefinition: fix::field_readonly(),
			FieldDefinition: fix::field_flexible_with_reference(),
			EventDefinition: fix::event_basic(),
			EventDefinition: fix::event_async(),
			FunctionDefinition: fix::function_basic(),
			FunctionDefinition: fix::function_with_args(),
			IndexDefinition: fix::index_basic(),
			IndexDefinition: fix::index_unique(),
			IndexDefinition: fix::index_hnsw(),
			IndexDefinition: fix::index_fulltext(),
			IndexDefinition: fix::index_count(),
			SubscriptionDefinition: fix::subscription_basic(),
			SubscriptionDefinition: fix::subscription_with_filters(),
			SubscriptionDefinition: fix::subscription_with_vars(),
			AccessDefinition: fix::access_bearer(),
			AccessDefinition: fix::access_with_authenticate(),
			AccessDefinition: fix::access_record(),
			AccessDefinition: fix::access_jwt_jwks(),
			ApiDefinition: fix::api_basic(),
			ApiDefinition: fix::api_with_middleware(),
			BucketDefinition: fix::bucket_basic(),
			BucketDefinition: fix::bucket_readonly(),
			ParamDefinition: fix::param_bool(),
			ParamDefinition: fix::param_string(),
			MlModelDefinition: fix::model_basic(),
			ModuleDefinition: fix::module_surrealism(),
			ModuleDefinition: fix::module_no_name(),
			ConfigDefinition: fix::config_graphql(),
			ConfigDefinition: fix::config_default(),
			ConfigDefinition: fix::config_api(),
		);
	}

	/// A stored definition whose text no longer parses must fail with an error
	/// that names the definition, not just the parser's position.
	///
	/// The parse error alone points at nothing a user or operator can find:
	/// the compile happens on a cache fill or provider read far from any
	/// statement, so the definition's own identity is the only usable pointer.
	/// The parser's detail must survive underneath it, as the error source.
	#[test]
	fn a_corrupt_stored_definition_names_itself() {
		use crate::catalog::ExprText;

		let mut field = fix::field_basic();
		field.value = Some(ExprText::from_raw("((( not surrealql"));
		let err = FieldDefinition::from_stored(&field).unwrap_err();
		assert_eq!(
			err.to_string(),
			"the stored definition of field `name` on table `users` no longer compiles"
		);
		assert!(
			format!("{err:#}").contains("Parse error"),
			"the parser's detail must remain in the error chain, got: {err:#}"
		);

		let mut event = fix::event_basic();
		event.when = ExprText::from_raw("((( not surrealql");
		let err = EventDefinition::from_stored(&event).unwrap_err();
		assert_eq!(
			err.to_string(),
			"the stored definition of event `on_create` on table `users` no longer compiles"
		);

		let mut function = fix::function_basic();
		function.block = crate::catalog::BlockText::from_raw("{ ((( not surrealql }");
		let err = FunctionDefinition::from_stored(&function).unwrap_err();
		assert!(
			err.to_string().starts_with("the stored definition of function `fn::"),
			"got: {err}"
		);
	}
}
