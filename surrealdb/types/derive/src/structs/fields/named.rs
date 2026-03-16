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
	/// When true, the type will be wrapped in the SerdeWrapper type to provide interop with
	/// serde-only types
	pub wrap: bool,
}

#[derive(Debug)]
pub struct NamedFields {
	pub fields: Vec<NamedField>,
	pub default: bool,
	pub skip_content: Option<crate::SkipContent>,
}

impl NamedFields {
	pub fn map_assignments(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|field| {
				let field_name = &field.ident;
				let potentially_wrapped_field = if field.wrap {
					let crate_path = crate_path.wrapper();
					quote! {
						#crate_path(#field_name)
					}
				} else {
					quote! {#field_name}
				};
				let field_name_str = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&field_name_str);

				if field.flatten {
					quote! {
						if let Value::Object(inner) = SurrealValue::into_value(#potentially_wrapped_field) {
							for (k, v) in inner.into_iter() {
								map.insert(k, v);
							}
						}
					}
				} else {
					quote! {
						map.insert(#obj_key.to_string(), #potentially_wrapped_field.into_value());
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
					let (potentially_wrapped_ty, val_access) = if field.wrap {
						let crate_path = crate_path.wrapper();
						(quote! { #crate_path::<#ty> }, quote! {.0})
					} else {
						(quote! { #ty }, quote! {})
					};
					let error_internal = crate_path.error_internal(quote! {
						format!("Failed to deserialize field '{}' on type '{}': {}", #field_name_str, #name, e)
					});

					if field.flatten {
						// Flatten: pass the remaining map to the field's from_value
						quote! {
							result.#field_name = <#potentially_wrapped_ty as SurrealValue>::from_value(
								Value::Object(map.clone())
							).map_err(|e| #error_internal)?#val_access;
						}
					} else {
						quote! {
							if let Some(field_value) = map.remove(#obj_key) {
								result.#field_name = <#potentially_wrapped_ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?#val_access;
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
				let (potentially_wrapped_ty, val_access) = if field.wrap {
					let crate_path = crate_path.wrapper();
					(quote! { #crate_path::<#ty> }, quote! {.0})
				} else {
					(quote! { #ty }, quote! {})
				};
				let error_internal = crate_path.error_internal(quote! {
					format!("Failed to deserialize field '{}' on type '{}': {}", #field_name_str, #name, e)
				});

				if field.flatten {
					// Flatten: pass the remaining map (after regular field extraction)
					flattened.push(quote! {
						let #field_name = <#potentially_wrapped_ty as SurrealValue>::from_value(
							Value::Object(map.clone())
						).map_err(|e| #error_internal)?#val_access;
					});
				} else {
					let retrieval = match &field.default {
						Some(FieldDefault::UseDefault) => quote! {
							let #field_name = if let Some(field_value) = map.remove(#obj_key) {
								<#potentially_wrapped_ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?#val_access
							} else {
								<#ty>::default()
							};
						},
						Some(FieldDefault::Path(path)) => quote! {
							let #field_name = if let Some(field_value) = map.remove(#obj_key) {
								<#potentially_wrapped_ty as SurrealValue>::from_value(field_value)
									.map_err(|e| #error_internal)?#val_access
							} else {
								#path()
							};
						},
						None => quote! {
							let field_value = map.remove(#obj_key).unwrap_or_default();
							let #field_name = <#potentially_wrapped_ty as SurrealValue>::from_value(field_value)
								.map_err(|e| #error_internal)?#val_access;
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
	/// Used as a fallback when content is missing during deserialization with `skip_content`.
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
	pub fn field_checks(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.filter(|field| !field.flatten) // Flatten fields don't have a specific key to check
			.map(|field| {
				let struct_name = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&struct_name);
				let ty = &field.ty;
				let potentially_wrapped_ty = if field.wrap {
					let crate_path = crate_path.wrapper();
					quote! { #crate_path::<#ty> }
				} else {
					quote! { #ty }
				};

				// Fields with container-level default are optional
				if self.default {
					quote! {
						if valid {
							if let Some(v) = map.get(#obj_key) {
								if !<#potentially_wrapped_ty as SurrealValue>::is_value(v) {
									valid = false;
								}
							}
						}
					}
				} else {
					quote! {
						if valid {
							if let Some(v) = map.get(#obj_key) {
								if !<#potentially_wrapped_ty as SurrealValue>::is_value(v) {
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

	pub fn map_types(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.filter(|field| !field.flatten) // Flatten fields don't have a specific key
			.map(|field| {
				let ty = &field.ty;
				let struct_name = field.ident.to_string();
				let obj_key = field.rename.as_ref().unwrap_or(&struct_name);
				let potentially_wrapped_ty = if field.wrap {
					let crate_path = crate_path.wrapper();
					quote! { #crate_path::<#ty> }
				} else {
					quote! { #ty }
				};

				quote! {
					map.insert(#obj_key.to_string(), <#potentially_wrapped_ty as SurrealValue>::kind_of());
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
