use proc_macro::TokenStream;
use syn::{Generics, Ident, WhereClause};
use quote::quote;

use crate::UnnamedFields;

/// Generate implementation for a struct with named fields
pub fn impl_struct_unnamed_fields(
	name: &Ident,
	generics: &Generics,
	where_clause: &Option<WhereClause>,
	x: UnnamedFields,
) -> TokenStream {
	let name_str = name.to_string();
	
	if !x.tuple && x.fields.len() == 1 {
		let ty = &x.fields[0];

		quote! {
			impl #generics SurrealValue for #name #generics #where_clause {
				fn into_value(self) -> surrealdb_types::Value {
					self.0.into_value()
				}

				fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
					Ok(Self(<#ty as SurrealValue>::from_value(value)?))
				}

				fn is_value(value: &surrealdb_types::Value) -> bool {
					<#ty as SurrealValue>::is_value(value)
				}

				fn kind_of() -> surrealdb_types::Kind {
					<#ty as SurrealValue>::kind_of()
				}
			}
		}.into()
	} else {
		let len = x.fields.len();
		let arr_assignments = x.arr_assignments();
		let arr_retrievals = x.arr_retrievals();
		let field_names = &x.field_names;
		let field_checks = x.field_checks();
		let arr_types = x.arr_types();

		quote! {
			impl #generics SurrealValue for #name #generics #where_clause {
				fn into_value(self) -> surrealdb_types::Value {
					let Self(#(#field_names),*) = self;
					let mut arr = Vec::new();
					#(#arr_assignments)*
					surrealdb_types::Value::Array(surrealdb_types::Array::from_values(arr))
				}

				fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
					let surrealdb_types::Value::Array(mut arr) = value else {
						return Err(surrealdb_types::anyhow::anyhow!("Expected array value for {}", #name_str));
					};

					if arr.len() != #len {
						return Err(surrealdb_types::anyhow::anyhow!("Expected array of length {}, got {}", #len, arr.len()));
					}

					Ok(Self(#(#arr_retrievals),*))
				}

				fn is_value(value: &surrealdb_types::Value) -> bool {
					if let surrealdb_types::Value::Array(arr) = value {
						if arr.len() != #len {
							return false;
						}

						#(#field_checks)*
						true
					} else {
						false
					}
				}

				fn kind_of() -> surrealdb_types::Kind {
					let mut arr = Vec::new();
					#(#arr_types)*
					surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Array(arr))
				}
			}
		}.into()
	}
}
