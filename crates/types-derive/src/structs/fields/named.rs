use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, Type};

#[derive(Debug)]
pub struct NamedField {
	pub ident: Ident,
	pub ty: Type,
	pub rename: Option<String>,
}

#[derive(Debug)]
pub struct NamedFields {
	pub fields: Vec<NamedField>,
	pub default: bool,
}

impl NamedFields {
	pub fn map_assignments(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|field| {
				let field_name = &field.ident;
				let field_name_str = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&field_name_str);

				quote! {
					map.insert(#obj_key.to_string(), #field_name.into_value());
				}
			})
			.collect()
	}

	pub fn map_retrievals(&self, name: &String) -> Vec<TokenStream2> {
		if self.default {
			// When default is set, create a default instance and overlay present fields
			let field_assignments: Vec<TokenStream2> = self
				.fields
				.iter()
				.map(|field| {
					let field_name = &field.ident;
					let field_name_str = field.ident.to_string();
					let obj_key = field.rename.as_ref().unwrap_or(&field_name_str);
					let ty = &field.ty;

					quote! {
						if let Some(field_value) = map.remove(#obj_key) {
							result.#field_name = <#ty as SurrealValue>::from_value(field_value)
								.map_err(|e| {
									surrealdb_types::anyhow::anyhow!(
										"Failed to deserialize field '{}' on type '{}': {}",
										#field_name_str, #name, e
									)
								})?;
						}
					}
				})
				.collect();

			vec![quote! {
				let mut result = Self::default();
				#(#field_assignments)*
			}]
		} else {
			self.fields
				.iter()
				.map(|field| {
					let field_name = &field.ident;
					let field_name_str = field.ident.to_string();
					let obj_key = field.rename.as_ref().unwrap_or(&field_name_str);
					let ty = &field.ty;

					quote! {
						let field_value = map.remove(#obj_key).unwrap_or_default();
						let #field_name = <#ty as SurrealValue>::from_value(field_value)
							.map_err(|e| {
								surrealdb_types::anyhow::anyhow!(
									"Failed to deserialize field '{}' on type '{}': {}",
									#field_name_str, #name, e
								)
							})?;
					}
				})
				.collect()
		}
	}

	/// Requires a mutable variable `valid` which defaults to true
	pub fn field_checks(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|field| {
				let struct_name = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&struct_name);
				let ty = &field.ty;

				// Fields with container-level default are optional
				if self.default {
					quote! {
						if valid {
							if let Some(v) = map.get(#obj_key) {
								if !<#ty as SurrealValue>::is_value(v) {
									valid = false;
								}
							}
						}
					}
				} else {
					quote! {
						if valid {
							if let Some(v) = map.get(#obj_key) {
								if !<#ty as SurrealValue>::is_value(v) {
									valid = false;
								}
							} else {
								valid = false;
							}
						}
					}
				}
			})
			.collect()
	}

	pub fn map_types(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|field| {
				let ty = &field.ty;
				let struct_name = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&struct_name);

				quote! {
					map.insert(#obj_key.to_string(), <#ty as SurrealValue>::kind_of());
				}
			})
			.collect()
	}

	pub fn contains_key(&self, key: &str) -> bool {
		self.fields
			.iter()
			.any(|field| field.rename.as_ref().unwrap_or(&field.ident.to_string()) == key)
	}
}
