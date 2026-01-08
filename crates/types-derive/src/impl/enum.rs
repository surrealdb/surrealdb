use proc_macro::TokenStream;
use quote::quote;
use syn::{Generics, Ident};

use crate::Enum;

pub fn impl_enum(name: &Ident, generics: &Generics, r#enum: Enum) -> TokenStream {
	let into_value = r#enum.into_value(&r#enum.attrs);
	let from_value = r#enum.from_value(&name.to_string(), &r#enum.attrs);
	let is_value = r#enum.is_value(&r#enum.attrs);
	let kind_of = r#enum.kind_of(&r#enum.attrs);

	let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

	quote! {
		impl #impl_generics SurrealValue for #name #type_generics #where_clause {
			fn into_value(self) -> surrealdb_types::Value {
				#into_value
			}

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				#from_value
			}

			fn is_value(value: &surrealdb_types::Value) -> bool {
				#is_value
			}

			fn kind_of() -> surrealdb_types::Kind {
				#kind_of
			}
		}
	}
	.into()
}
