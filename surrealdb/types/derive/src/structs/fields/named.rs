use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, Type};

use crate::CratePath;
use crate::attr::FieldDefault;

#[derive(Debug)]
pub struct NamedField {
	pub ident: Ident,
	pub ty: Type,
	pub rename: Option<String>,
	pub default: Option<FieldDefault>,
	/// When true, this field's serialized object is merged into the parent.
	pub flatten: bool,
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

				if field.flatten {
					quote! {
						if let Value::Object(inner) = SurrealValue::into_value(#field_name) {
							for (k, v) in inner.into_iter() {
								map.insert(k, v);
							}
						}
					}
				} else {
					quote! {
						map.insert(#obj_key.to_string(), #field_name.into_value());
					}
				}
			})
			.collect()
	}

	pub fn map_retrievals(&self, name: &String, crate_path: &CratePath) -> Vec<TokenStream2> {
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
					let error_internal = crate_path.error_internal(quote! {
						format!("Failed to deserialize field '{}' on type '{}': {}", #field_name_str, #name, e)
					});

					if field.flatten {
						// Flatten: pass the remaining map to the field's from_value
						quote! {
							result.#field_name = <#ty as SurrealValue>::from_value(
								Value::Object(map.clone())
							).map_err(|e| #error_internal)?;
						}
					} else {
						quote! {
							if let Some(field_value) = map.remove(#obj_key) {
								result.#field_name = <#ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?;
							}
						}
					}
				})
				.collect();

			vec![quote! {
				let mut result = Self::default();
				#(#field_assignments)*
			}]
		} else {
			// Process regular fields first, then flatten fields with remaining map
			let mut regular: Vec<TokenStream2> = Vec::new();
			let mut flattened: Vec<TokenStream2> = Vec::new();

			for field in &self.fields {
				let field_name = &field.ident;
				let field_name_str = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&field_name_str);
				let ty = &field.ty;
				let error_internal = crate_path.error_internal(quote! {
					format!("Failed to deserialize field '{}' on type '{}': {}", #field_name_str, #name, e)
				});

				if field.flatten {
					// Flatten: pass the remaining map (after regular field extraction)
					flattened.push(quote! {
						let #field_name = <#ty as SurrealValue>::from_value(
							Value::Object(map.clone())
						).map_err(|e| #error_internal)?;
					});
				} else {
					let retrieval = match &field.default {
						Some(FieldDefault::UseDefault) => quote! {
							let #field_name = if let Some(field_value) = map.remove(#obj_key) {
								<#ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?
							} else {
								<#ty>::default()
							};
						},
						Some(FieldDefault::Path(path)) => quote! {
							let #field_name = if let Some(field_value) = map.remove(#obj_key) {
								<#ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?
							} else {
								#path()
							};
						},
						None => quote! {
							let field_value = map.remove(#obj_key).unwrap_or_default();
							let #field_name = <#ty as SurrealValue>::from_value(field_value)
								.map_err(|e| #error_internal)?;
						},
					};
					regular.push(retrieval);
				}
			}

			// Regular fields first (extracting from map), then flattened fields (using remaining
			// map)
			regular.extend(flattened);
			regular
		}
	}

	/// Generates `let field_name = Default::default();` for each field.
	/// Used as a fallback when content is missing during deserialization with `skip_content_if`.
	pub fn default_initializers(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|field| {
				let field_name = &field.ident;
				let ty = &field.ty;
				quote! {
					let #field_name = <#ty as Default>::default();
				}
			})
			.collect()
	}

	/// Requires a mutable variable `valid` which defaults to true
	pub fn field_checks(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.filter(|field| !field.flatten) // Flatten fields don't have a specific key to check
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
			.filter(|field| !field.flatten) // Flatten fields don't have a specific key
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
