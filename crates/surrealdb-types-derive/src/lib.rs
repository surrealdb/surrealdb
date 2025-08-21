use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, Type, parse_macro_input};

#[proc_macro_derive(SurrealValue)]
pub fn surreal_value(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	let name = &input.ident;
	let generics = &input.generics;
	let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

	// Extract fields from the struct
	let fields = match &input.data {
		Data::Struct(data) => &data.fields,
		_ => panic!("SurrealValue can only be derived for structs"),
	};

	match fields {
		Fields::Named(named_fields) => {
			let field_names: Vec<&Ident> =
				named_fields.named.iter().filter_map(|f| f.ident.as_ref()).collect();
			let field_types: Vec<&Type> = named_fields.named.iter().map(|f| &f.ty).collect();

			let is_value_checks: Vec<_> = field_names.iter().zip(field_types.iter()).map(|(field_name, field_type)| {
				let field_name_str = field_name.to_string();
				quote! {
					obj.get(#field_name_str).map_or(false, |v| <#field_type as surrealdb_types::SurrealValue>::is_value(v))
				}
			}).collect();

			let into_value_fields: Vec<_> = field_names
				.iter()
				.zip(field_types.iter())
				.map(|(field_name, _field_type)| {
					let field_name_str = field_name.to_string();
					quote! {
						(#field_name_str.to_string(), self.#field_name.into_value())
					}
				})
				.collect();

			let from_value_fields: Vec<_> = field_names
				.iter()
				.zip(field_types.iter())
				.map(|(field_name, field_type)| {
					let field_name_str = field_name.to_string();
					quote! {
						let #field_name = obj.get(#field_name_str)
							.and_then(|v| <#field_type as surrealdb_types::SurrealValue>::from_value(v.clone()))?;
					}
				})
				.collect();

			let field_kinds: Vec<_> = field_names
				.iter()
				.zip(field_types.iter())
				.map(|(field_name, field_type)| {
					let field_name_str = field_name.to_string();
					quote! {
						(#field_name_str.to_string(), <#field_type as surrealdb_types::SurrealValue>::kind_of())
					}
				})
				.collect();

			let output = quote! {
				impl #impl_generics surrealdb_types::SurrealValue for #name #ty_generics #where_clause {
					fn kind_of() -> surrealdb_types::Kind {
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::from([
							#(#field_kinds),*
						])))
					}

					fn is_value(value: &surrealdb_types::Value) -> bool {
						if let surrealdb_types::Value::Object(surrealdb_types::Object(obj)) = value {
							#(#is_value_checks)&&*
						} else {
							false
						}
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Object(surrealdb_types::Object(std::collections::BTreeMap::from([
							#(#into_value_fields),*
						])))
					}

					fn from_value(value: surrealdb_types::Value) -> Option<Self> {
						let surrealdb_types::Value::Object(surrealdb_types::Object(obj)) = value else {
							return None;
						};

						#(#from_value_fields)*

						Some(Self {
							#(#field_names),*
						})
					}
				}
			};

			output.into()
		}
		Fields::Unnamed(unnamed_fields) => {
			let field_types: Vec<&Type> = unnamed_fields.unnamed.iter().map(|f| &f.ty).collect();
			let field_count = field_types.len();

			let is_value_checks: Vec<_> = (0..field_count)
				.map(|i| {
					let field_type = &field_types[i];
					quote! {
						values.get(#i).map_or(false, |v| <#field_type as surrealdb_types::SurrealValue>::is_value(v))
					}
				})
				.collect();

			let into_value_fields: Vec<_> = (0..field_count)
				.map(|i| {
					let i_lit = syn::Index::from(i);
					quote! {
						self.#i_lit.into_value()
					}
				})
				.collect();

			let from_value_fields: Vec<_> = (0..field_count)
				.map(|i| {
					let _i_lit = syn::Index::from(i);
					let field_type = &field_types[i];
					let field_ident =
						syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site());
					quote! {
						let #field_ident = values.get(#i)
							.and_then(|v| <#field_type as surrealdb_types::SurrealValue>::from_value(v.clone()))?;
					}
				})
				.collect();

			let field_assignments: Vec<_> = (0..field_count)
				.map(|i| {
					let field_ident =
						syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site());
					quote! { #field_ident }
				})
				.collect();

			let field_kinds: Vec<_> = field_types
				.iter()
				.map(|field_type| {
					quote! {
						<#field_type as surrealdb_types::SurrealValue>::kind_of()
					}
				})
				.collect();

			let output = quote! {
				impl #impl_generics surrealdb_types::SurrealValue for #name #ty_generics #where_clause {
					fn kind_of() -> surrealdb_types::Kind {
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Array(vec![
							#(#field_kinds),*
						]))
					}

					fn is_value(value: &surrealdb_types::Value) -> bool {
						if let surrealdb_types::Value::Array(surrealdb_types::Array(values)) = value {
							values.len() == #field_count && #(#is_value_checks)&&*
						} else {
							false
						}
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Array(surrealdb_types::Array(vec![
							#(#into_value_fields),*
						]))
					}

					fn from_value(value: surrealdb_types::Value) -> Option<Self> {
						let surrealdb_types::Value::Array(surrealdb_types::Array(values)) = value else {
							return None;
						};

						if values.len() != #field_count {
							return None;
						}

						#(#from_value_fields)*

						Some(Self(
							#(#field_assignments),*
						))
					}
				}
			};

			output.into()
		}
		Fields::Unit => {
			let output = quote! {
				impl #impl_generics surrealdb_types::SurrealValue for #name #ty_generics #where_clause {
					fn kind_of() -> surrealdb_types::Kind {
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::new()))
					}

					fn is_value(value: &surrealdb_types::Value) -> bool {
						matches!(value, surrealdb_types::Value::Object(surrealdb_types::Object(obj)) if obj.is_empty())
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Object(surrealdb_types::Object(std::collections::BTreeMap::new()))
					}

					fn from_value(value: surrealdb_types::Value) -> Option<Self> {
						match value {
							surrealdb_types::Value::Object(surrealdb_types::Object(obj)) if obj.is_empty() => Some(Self),
							_ => None,
						}
					}
				}
			};

			output.into()
		}
	}
}
