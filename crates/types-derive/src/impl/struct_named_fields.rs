use proc_macro::TokenStream;
use syn::{Generics, Ident, WhereClause};
use quote::quote;

use crate::NamedFields;

/// Generate implementation for a struct with named fields
pub fn impl_struct_named_fields(
	name: &Ident,
	generics: &Generics,
	where_clause: &Option<WhereClause>,
	fields: NamedFields,
) -> TokenStream {
    let map_assignments = fields.map_assignments();
    let map_retrievals = fields.map_retrievals(name);
    let field_names = fields.field_names();
    let field_checks = fields.field_checks();
    let map_types = fields.map_types();

	quote! {
		impl #generics SurrealValue for #name #generics #where_clause {
			fn into_value(self) -> surrealdb_types::Value {
				let Self { #(#field_names),* } = self;
				let mut map = std::collections::BTreeMap::new();
				#(#map_assignments)*
				surrealdb_types::Value::Object(surrealdb_types::Object::from(map))
			}

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				if let surrealdb_types::Value::Object(mut map) = value {
					#(#map_retrievals)*
					Ok(Self { #(#field_names),* })
				} else {
					Err(surrealdb_types::anyhow::anyhow!("Expected object value"))
				}
			}

			fn is_value(value: &surrealdb_types::Value) -> bool {
				if let surrealdb_types::Value::Object(map) = value {
					#(#field_checks)*
					true
				} else {
					false
				}
			}

			fn kind_of() -> surrealdb_types::Kind {
				let mut map = std::collections::BTreeMap::new();
				#(#map_types)*
				surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
			}
		}
	}.into()
}
