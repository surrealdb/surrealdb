use proc_macro::TokenStream;
use quote::quote;
use syn::{Generics, Ident, WhereClause};

use crate::{Fields, Strategy, With};

pub fn impl_struct(
	name: &Ident,
	generics: &Generics,
	where_clause: &Option<WhereClause>,
	fields: Fields,
) -> TokenStream {
	let strategy = Strategy::for_struct();
	let match_fields = fields.match_fields();
	let from_ok = quote!(Ok(Self #match_fields));

	let into_value = fields.into_value(&strategy);
	let from_value = match fields.from_value(&name.to_string(), &strategy, from_ok) {
		With::Map(x) => quote! {
			if let surrealdb_types::Value::Object(mut map) = value {
				#x
			} else {
				Err(surrealdb_types::anyhow::anyhow!("Expected object value"))
			}
		},
		With::Arr(x) => quote! {
			if let surrealdb_types::Value::Array(mut arr) = value {
				#x
			} else {
				Err(surrealdb_types::anyhow::anyhow!("Expected array value"))
			}
		},
		With::String(x) => quote! {
			if let surrealdb_types::Value::String(string) = value {
				#x
			} else {
				Err(surrealdb_types::anyhow::anyhow!("Expected string value"))
			}
		},
		With::Value(x) => x,
	};

	let is_value = match fields.is_value(&strategy) {
		With::Map(x) => quote! {
			if let surrealdb_types::Value::Object(map) = value {
				#x
			}

			false
		},
		With::Arr(x) => quote! {
			if let surrealdb_types::Value::Array(arr) = value {
				#x
			}

			false
		},
		With::String(x) => quote! {
			if let surrealdb_types::Value::String(string) = value {
				#x
			}

			false
		},
		With::Value(x) => x,
	};
	let kind_of = fields.kind_of(&strategy);

	let let_fields = if fields.has_fields() {
		quote!( let Self #match_fields = self; )
	} else {
		quote!()
	};

	quote! {
		impl #generics SurrealValue for #name #generics #where_clause {
			fn into_value(self) -> surrealdb_types::Value {
				#let_fields
				#into_value
			}

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				#from_value
			}

			fn is_value(value: &surrealdb_types::Value) -> bool {
				#is_value;

				false
			}

			fn kind_of() -> surrealdb_types::Kind {
				#kind_of
			}
		}
	}
	.into()
}
