//! Round-trip coverage: every fixture definition compiles from its stored
//! form and renders back to it.

mod tests {
	use crate::catalog::{
		AccessDefinition, ApiDefinition, BucketDefinition, ConfigDefinition, EventDefinition,
		FieldDefinition, FromStored, FunctionDefinition, IndexDefinition, MlModelDefinition,
		ModuleDefinition, ParamDefinition, SubscriptionDefinition, TableDefinition,
	};
	use crate::kvs::compat::fixtures as fix;

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
