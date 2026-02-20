mod named;
pub use named::*;

mod unnamed;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::Attribute;
pub use unnamed::*;

use crate::{
	CratePath, FieldAttributes, NamedFieldsAttributes, SkipContent, Strategy, UnitAttributes,
	UnitValue, UnnamedFieldsAttributes, With,
};

#[derive(Debug)]
pub enum Fields {
	Named(NamedFields),
	Unnamed(UnnamedFields),
	Unit(UnitAttributes),
}

impl Fields {
	pub fn parse(fields: &syn::Fields, attrs: &[Attribute]) -> Self {
		match fields {
			syn::Fields::Named(named_fields) => {
				let container_attrs = NamedFieldsAttributes::parse(attrs);
				let fields = named_fields
					.named
					.iter()
					.map(|field| {
						let field_name =
							field.ident.as_ref().expect("Named field must have an identifier");
						let field_attrs = FieldAttributes::parse(field);
						NamedField {
							ident: field_name.clone(),
							ty: field.ty.clone(),
							rename: field_attrs.rename,
							default: field_attrs.default,
							flatten: field_attrs.flatten,
						}
					})
					.collect();

				Fields::Named(NamedFields {
					fields,
					default: container_attrs.default,
					skip_content: container_attrs.skip_content,
				})
			}
			syn::Fields::Unnamed(unnamed_fields) => {
				let unnamed_field_attrs = UnnamedFieldsAttributes::parse(attrs);
				let fields = unnamed_fields.unnamed.iter().map(|field| field.ty.clone()).collect();

				Fields::Unnamed(UnnamedFields::new(
					fields,
					unnamed_field_attrs.tuple,
					unnamed_field_attrs.skip_content,
				))
			}
			syn::Fields::Unit => Fields::Unit(UnitAttributes::parse(attrs)),
		}
	}

	pub fn has_fields(&self) -> bool {
		match self {
			Fields::Named(_) => true,
			Fields::Unnamed(_) => true,
			Fields::Unit(_) => false,
		}
	}

	/// Returns the per-variant skip_content setting, if any.
	pub fn skip_content(&self) -> Option<&SkipContent> {
		match self {
			Fields::Named(f) => f.skip_content.as_ref(),
			Fields::Unnamed(f) => f.skip_content.as_ref(),
			Fields::Unit(a) if a.skip_content => Some(&SkipContent::Always),
			Fields::Unit(_) => None,
		}
	}

	pub fn match_fields(&self) -> TokenStream2 {
		match self {
			Fields::Named(named_fields) => {
				let fields =
					named_fields.fields.iter().map(|field| &field.ident).collect::<Vec<_>>();
				quote! {{ #(#fields),* }}
			}
			Fields::Unnamed(fields) => {
				let vars = fields
					.fields
					.iter()
					.enumerate()
					.map(|(i, _)| syn::Ident::new(&format!("field_{}", i), Span::call_site()))
					.collect::<Vec<_>>();

				quote! {( #(#vars),* )}
			}
			Fields::Unit(_) => quote!(),
		}
	}

	#[expect(clippy::wrong_self_convention)]
	pub fn into_value(&self, strategy: &Strategy, crate_path: &CratePath) -> TokenStream2 {
		let value_ty = crate_path.value();
		let object_ty = crate_path.object();
		let array_ty = crate_path.array();
		let value_from_t = crate_path.value_from_t();
		match self {
			Fields::Named(fields) => {
				let map_assignments = fields.map_assignments();

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut map = #object_ty::new();
							map.insert(#variant.to_string(), {
								let mut map = #object_ty::new();
								#(#map_assignments)*
								#value_ty::Object(map)
							});
							#value_ty::Object(map)
						}}
					}
					Strategy::TagKey {
						tag,
						variant,
					} => {
						if fields.contains_key(tag) {
							panic!("Tag key {} is already claimed by a field", tag);
						}

						quote! {{
							let mut map = #object_ty::new();
							map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
							#(#map_assignments)*
							#value_ty::Object(map)
						}}
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						skip_content,
					} => match skip_content {
						Some(SkipContent::Always) => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								#value_ty::Object(map)
							}}
						}
						Some(SkipContent::If(func)) => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								let content_value = {
									let mut map = #object_ty::new();
									#(#map_assignments)*
									#value_ty::Object(map)
								};
								if !#func(&content_value) {
									map.insert(#content.to_string(), content_value);
								}
								#value_ty::Object(map)
							}}
						}
						None => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								map.insert(#content.to_string(), {
									let mut map = #object_ty::new();
									#(#map_assignments)*
									#value_ty::Object(map)
								});
								#value_ty::Object(map)
							}}
						}
					},
					Strategy::Value {
						..
					} => {
						quote! {{
							let mut map = #object_ty::new();
							#(#map_assignments)*
							#value_ty::Object(map)
						}}
					}
				}
			}
			Fields::Unnamed(x) => {
				let value = if !x.tuple && x.fields.len() == 1 {
					quote!(#value_from_t(field_0))
				} else {
					let arr_assignments = x.arr_assignments();
					quote! {{
						let mut arr = #array_ty::new();
						#(#arr_assignments)*
						#value_ty::Array(arr)
					}}
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut map = #object_ty::new();
							map.insert(#variant.to_string(), #value);
							#value_ty::Object(map)
						}}
					}
					Strategy::TagKey {
						..
					} => {
						panic!("Tag key strategy cannot be used with unnamed fields");
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						skip_content,
					} => match skip_content {
						Some(SkipContent::Always) => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								#value_ty::Object(map)
							}}
						}
						Some(SkipContent::If(func)) => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								let content_value = #value;
								if !#func(&content_value) {
									map.insert(#content.to_string(), content_value);
								}
								#value_ty::Object(map)
							}}
						}
						None => {
							quote! {{
								let mut map = #object_ty::new();
								map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
								map.insert(#content.to_string(), #value);
								#value_ty::Object(map)
							}}
						}
					},
					Strategy::Value {
						..
					} => value,
				}
			}
			Fields::Unit(attrs) => match strategy {
				Strategy::VariantKey {
					variant,
				} => {
					if attrs.value.is_some() {
						panic!("Unit variants can only have a value with untagged enums");
					}

					quote! {{
						let mut map = #object_ty::new();
						map.insert(#variant.to_string(), #value_ty::Object(#object_ty::new()));
						#value_ty::Object(map)
					}}
				}
				Strategy::TagKey {
					tag,
					variant,
				} => {
					if attrs.value.is_some() {
						panic!("Unit variants can only have a value with untagged enums");
					}

					quote! {{
						let mut map = #object_ty::new();
						map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
						#value_ty::Object(map)
					}}
				}
				Strategy::TagContentKeys {
					tag,
					variant,
					content,
					skip_content,
				} => {
					if attrs.value.is_some() {
						panic!("Unit variants can only have a value with untagged enums");
					}

					if skip_content.is_some() {
						quote! {{
							let mut map = #object_ty::new();
							map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
							#value_ty::Object(map)
						}}
					} else {
						quote! {{
							let mut map = #object_ty::new();
							map.insert(#tag.to_string(), #value_ty::String(#variant.to_string()));
							map.insert(#content.to_string(), #value_ty::Object(#object_ty::new()));
							#value_ty::Object(map)
						}}
					}
				}
				Strategy::Value {
					variant,
				} => {
					if let Some(UnitValue {
						value,
						..
					}) = attrs.value.as_ref()
					{
						quote!(#value)
					} else if let Some(variant) = variant {
						quote!(#value_ty::String(#variant.to_string()))
					} else {
						quote!(#value_ty::Object(
							#object_ty::new()
						))
					}
				}
			},
		}
	}

	#[expect(clippy::wrong_self_convention)]
	pub fn from_value(
		&self,
		name: &String,
		strategy: &Strategy,
		ok: TokenStream2,
		crate_path: &CratePath,
	) -> With {
		let value_ty = crate_path.value();
		let kind_ty = crate_path.kind();
		let conversion_error_ty = crate_path.conversion_error();
		let type_error_ty = crate_path.type_error();
		let error_expected_content =
			crate_path.error_internal(quote!("Expected content key".to_string()));
		match self {
			Fields::Named(fields) => {
				let map_retrievals = fields.map_retrievals(name, crate_path);

				let final_ok = if fields.default {
					quote!(Ok(result))
				} else {
					ok
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => With::Map(quote! {{
						if let Some(value) = map.remove(#variant) {
							if let #value_ty::Object(mut map) = value {
								#(#map_retrievals)*
								#final_ok
							} else {
								let err = #conversion_error_ty::from_value(
									#kind_ty::Object,
									&value
								).with_context(format!("variant '{}'", #variant));
								return Err(err.into())
							}
						}
					}}),
					Strategy::TagKey {
						tag,
						variant,
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v == Value::String(#variant.to_string())) {
							#(#map_retrievals)*
							#final_ok
						}
					}}),
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						skip_content,
					} => {
						if skip_content.is_some() {
							let default_inits = fields.default_initializers();
							With::Map(quote! {{
								if map.get(#tag).is_some_and(|v| v == Value::String(#variant.to_string())) {
									if let Some(#value_ty::Object(mut map)) = map.remove(#content) {
										#(#map_retrievals)*
										#final_ok
									} else {
										#(#default_inits)*
										#final_ok
									}
								}
							}})
						} else {
							With::Map(quote! {{
								if map.get(#tag).is_some_and(|v| v == Value::String(#variant.to_string())) {
									if let Some(#value_ty::Object(mut map)) = map.remove(#content) {
										#(#map_retrievals)*
										#final_ok
									} else {
										let err = #type_error_ty::Invalid(
											format!("Expected object under content key '{}' for variant '{}'", #content, #variant)
										);
										return Err(err.into())
									}
								}
							}})
						}
					}
					// For an enum, we check first if the variant matches, then decode
					Strategy::Value {
						variant: Some(_),
					} => {
						let field_checks = fields.field_checks();

						With::Map(quote! {{
							let mut valid = true;
							#(#field_checks)*

							if valid {
								#(#map_retrievals)*
								#final_ok
							}
						}})
					}
					// For a struct, there's no other variants to try, so we just decode
					Strategy::Value {
						variant: None,
					} => With::Map(quote! {{
						#(#map_retrievals)*
						#final_ok
					}}),
				}
			}
			Fields::Unnamed(fields) => {
				// Single field
				if !fields.tuple && fields.fields.len() == 1 {
					let ty = &fields.fields[0];
					let retrieve = quote! {{
						let field_0 = <#ty as SurrealValue>::from_value(value)?;
						#ok
					}};

					match strategy {
						Strategy::VariantKey {
							variant,
						} => With::Map(quote! {{
							if let Some(value) = map.remove(#variant) {
								#retrieve
							}
						}}),
						Strategy::TagKey {
							..
						} => {
							panic!("Tag key strategy cannot be used with unnamed fields");
						}
						Strategy::TagContentKeys {
							tag,
							variant,
							content,
							skip_content,
						} => {
							if skip_content.is_some() {
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.remove(#content) {
											#retrieve
										} else {
											let field_0 = <#ty as Default>::default();
											#ok
										}
									}
								}})
							} else {
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.remove(#content) {
											#retrieve
										} else {
											let err = #type_error_ty::Invalid(
												format!("Expected content key '{}' for variant '{}'", #content, #variant)
											);
											return Err(err.into())
										}
									}
								}})
							}
						}
						// For an enum, we check first if the variant matches, then decode
						Strategy::Value {
							variant: Some(_),
						} => With::Value(quote! {{
							if <#ty as SurrealValue>::is_value(&value) {
								#retrieve
							}
						}}),
						// For a struct, there's no other variants to try, so we just decode
						Strategy::Value {
							variant: None,
						} => With::Value(retrieve),
					}
				} else {
					let arr_retrievals = fields.arr_retrievals();
					let retrieve_arr = quote! {{
						#(#arr_retrievals)*
						#ok
					}};

					let retrieve_value = quote! {{
						if let #value_ty::Array(mut arr) = value {
							#retrieve_arr
						} else {
							let err = #conversion_error_ty::from_value(
								#kind_ty::Array(Box::new(#kind_ty::Any), None),
								&value
							);
							return Err(err.into())
						}
					}};

					match strategy {
						Strategy::VariantKey {
							variant,
						} => With::Map(quote! {{
							if let Some(value) = map.remove(#variant) {
								#retrieve_value
							}
						}}),
						Strategy::TagKey {
							..
						} => {
							panic!("Tag key strategy cannot be used with unnamed fields");
						}
						Strategy::TagContentKeys {
							tag,
							variant,
							content,
							skip_content,
						} => {
							if skip_content.is_some() {
								let default_inits = fields.default_initializers();
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.remove(#content) {
											#retrieve_value
										} else {
											#(#default_inits)*
											#ok
										}
									}
								}})
							} else {
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.remove(#content) {
											#retrieve_value
										} else {
											return Err(#error_expected_content)
										}
									}
								}})
							}
						}
						// For an enum, we check first if the variant matches, then decode
						Strategy::Value {
							variant: Some(_),
						} => {
							let field_checks = fields.field_checks();
							With::Arr(quote! {{
								let mut valid = true;
								#(#field_checks)*

								if valid {
									#retrieve_arr
								}
							}})
						}
						// For a struct, there's no other variants to try, so we just decode
						Strategy::Value {
							variant: None,
						} => With::Arr(quote! {{
							#retrieve_arr
						}}),
					}
				}
			}
			Fields::Unit(attrs) => {
				match strategy {
					Strategy::VariantKey {
						variant,
					} => With::Map(quote! {{
						if map.get(#variant).is_some() {
							#ok
						}
					}}),
					Strategy::TagKey {
						tag,
						variant,
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
							#ok
						}
					}}),
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						skip_content,
					} => {
						if skip_content.is_some() {
							With::Map(quote! {{
								if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
									if !map.contains_key(#content)
										|| map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty()))
									{
										#ok
									}
								}
							}})
						} else {
							With::Map(quote! {{
								if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
									if map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty())) {
										#ok
									} else {
										let err = #type_error_ty::Invalid(
											format!("Expected empty object under content key '{}' for variant '{}'", #content, #variant)
										);
										return Err(err.into())
									}
								}
							}})
						}
					}
					// For an enum, we check first if the variant matches, then decode
					Strategy::Value {
						variant: Some(variant),
					} => {
						if let Some(UnitValue {
							is_value,
							..
						}) = attrs.value.as_ref()
						{
							With::Value(quote! {{
								if #is_value {
									#ok
								}
							}})
						} else {
							With::String(quote! {{
								if string == #variant {
									#ok
								}
							}})
						}
					}
					// For a struct, there's no other variants to try, so we just decode
					Strategy::Value {
						variant: None,
					} => {
						if let Some(UnitValue {
							is_value,
							inner,
							..
						}) = attrs.value.as_ref()
						{
							With::Value(quote! {{
								if #is_value {
									#ok
								} else {
									let err = #type_error_ty::Invalid(
										format!("Expected {} while decoding {}", #inner, #name)
									);
									return Err(err.into())
								}
							}})
						} else {
							With::Map(quote! {{
								if map.is_empty() {
									#ok
								} else {
									let err = #type_error_ty::Invalid(
										format!("Expected empty object value while decoding {}", #name)
									);
									return Err(err.into())
								}
							}})
						}
					}
				}
			}
		}
	}

	/// Generates value checking code. Only returns once it has committed that no other variant can
	/// match.
	pub fn is_value(&self, strategy: &Strategy, crate_path: &CratePath) -> With {
		let value_ty = crate_path.value();
		match self {
			Fields::Named(fields) => {
				let field_checks = fields.field_checks();

				match strategy {
					Strategy::VariantKey {
						variant,
					} => With::Map(quote! {{
						if let Some(#value_ty::Object(map)) = map.get(#variant) {
							let mut valid = true;
							#(#field_checks)*
							return valid;
						}
					}}),
					Strategy::TagKey {
						tag,
						variant,
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
							let mut valid = true;
							#(#field_checks)*
							return valid;
						}
					}}),
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						..
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
							if let Some(#value_ty::Object(map)) = map.get(#content) {
								let mut valid = true;
								#(#field_checks)*
								return valid;
							}

							return false;
						}
					}}),
					Strategy::Value {
						..
					} => With::Map(quote! {{
						let mut valid = true;
						#(#field_checks)*
						if valid {
							return true;
						}
					}}),
				}
			}
			Fields::Unnamed(fields) => {
				// Single field
				if !fields.tuple && fields.fields.len() == 1 {
					let ty = &fields.fields[0];
					let check = quote!( <#ty as SurrealValue>::is_value(value) );

					match strategy {
						Strategy::VariantKey {
							variant,
						} => With::Map(quote! {{
							if let Some(value) = map.get(#variant) {
								return #check;
							}
						}}),
						Strategy::TagKey {
							..
						} => {
							panic!("Tag key strategy cannot be used with unnamed fields");
						}
						Strategy::TagContentKeys {
							tag,
							variant,
							content,
							skip_content,
						} => {
							if skip_content.is_some() {
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.get(#content) {
											return #check;
										}
										return true;
									}
								}})
							} else {
								With::Map(quote! {{
									if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
										if let Some(value) = map.get(#content) {
											return #check;
										}
									}
								}})
							}
						}
						Strategy::Value {
							..
						} => With::Value(quote! {{
							if #check {
								return true;
							}
						}}),
					}
				} else {
					let field_checks = fields.field_checks();
					let check_arr = quote!( #(#field_checks)* );
					let check_value = quote! {{
						if let #value_ty::Array(arr) = value {
							#check_arr
						} else {
							valid = false;
						}
					}};

					match strategy {
						Strategy::VariantKey {
							variant,
						} => With::Map(quote! {{
							if let Some(value) = map.get(#variant) {
								let mut valid = true;
								#check_value
								return valid;
							}
						}}),
						Strategy::TagKey {
							..
						} => {
							panic!("Tag key strategy cannot be used with unnamed fields");
						}
						Strategy::TagContentKeys {
							tag,
							variant,
							content,
							..
						} => With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
								if let Some(value) = map.get(#content) {
									let mut valid = true;
									#check_value
									return valid;
								}
							}
						}}),
						Strategy::Value {
							..
						} => With::Arr(quote! {{
							let mut valid = true;
							#check_arr
							if valid {
								return true;
							}
						}}),
					}
				}
			}
			Fields::Unit(attrs) => match strategy {
				Strategy::VariantKey {
					variant,
				} => With::Map(quote! {{
					if map.get(#variant).is_some_and(|v| v.is_object_and(|o| o.is_empty())) {
						return true;
					}
				}}),
				Strategy::TagKey {
					tag,
					variant,
				} => With::Map(quote! {{
					if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
						return true;
					}
				}}),
				Strategy::TagContentKeys {
					tag,
					variant,
					content,
					skip_content,
				} => {
					if skip_content.is_some() {
						With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) &&
								(!map.contains_key(#content)
									|| map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty())))
							{
								return true;
							}
						}})
					} else {
						With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) &&
								map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty()))
							{
								return true;
							}
						}})
					}
				}
				Strategy::Value {
					variant,
				} => {
					if let Some(UnitValue {
						is_value,
						..
					}) = attrs.value.as_ref()
					{
						With::Value(quote! {{
							if #is_value {
								return true;
							}
						}})
					} else if let Some(variant) = variant {
						With::String(quote! {{
							if string == #variant {
								return true;
							}
						}})
					} else {
						With::Map(quote! {{
							if map.is_empty() {
								return true;
							}
						}})
					}
				}
			},
		}
	}

	pub fn kind_of(&self, strategy: &Strategy, crate_path: &CratePath) -> TokenStream2 {
		let kind_ty = crate_path.kind();
		let kind_literal_ty = crate_path.kind_literal();
		match self {
			Fields::Named(fields) => {
				let map_types = fields.map_types();

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							map.insert(#variant.to_string(), {
								let mut map = std::collections::BTreeMap::new();
								#(#map_types)*
								#kind_ty::Literal(#kind_literal_ty::Object(map))
							});
							#kind_ty::Literal(#kind_literal_ty::Object(map))
						}}
					}
					Strategy::TagKey {
						tag,
						variant,
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							map.insert(#tag.to_string(), #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())));
							#(#map_types)*
							#kind_ty::Literal(#kind_literal_ty::Object(map))
						}}
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						..
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							map.insert(#tag.to_string(), #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())));
							map.insert(#content.to_string(), {
								let mut map = std::collections::BTreeMap::new();
								#(#map_types)*
								#kind_ty::Literal(#kind_literal_ty::Object(map))
							});
							#kind_ty::Literal(#kind_literal_ty::Object(map))
						}}
					}
					Strategy::Value {
						..
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							#(#map_types)*
							#kind_ty::Literal(#kind_literal_ty::Object(map))
						}}
					}
				}
			}
			Fields::Unnamed(fields) => {
				let kind_of = if !fields.tuple && fields.fields.len() == 1 {
					let ty = &fields.fields[0];
					quote!( <#ty as SurrealValue>::kind_of() )
				} else {
					let arr_types = fields.arr_types();

					quote! {{
						let mut arr = Vec::new();
						#(#arr_types)*
						#kind_ty::Literal(#kind_literal_ty::Array(arr))
					}}
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut obj = std::collections::BTreeMap::new();
							obj.insert(#variant.to_string(), #kind_of);
							#kind_ty::Literal(#kind_literal_ty::Object(obj))
						}}
					}
					Strategy::TagKey {
						..
					} => {
						panic!("Tag key strategy cannot be used with unnamed fields");
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
						..
					} => {
						quote! {{
							let mut obj = std::collections::BTreeMap::new();
							obj.insert(#tag.to_string(), #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())));
							obj.insert(#content.to_string(), #kind_of);
							#kind_ty::Literal(#kind_literal_ty::Object(obj))
						}}
					}
					Strategy::Value {
						..
					} => kind_of,
				}
			}
			Fields::Unit(attrs) => match strategy {
				Strategy::VariantKey {
					variant,
				} => {
					quote! {{
						let mut obj = std::collections::BTreeMap::new();
						obj.insert(#variant.to_string(), #kind_ty::Literal(#kind_literal_ty::Object(std::collections::BTreeMap::new())));
						#kind_ty::Literal(#kind_literal_ty::Object(obj))
					}}
				}
				Strategy::TagKey {
					tag,
					variant,
				} => {
					quote! {{
						let mut obj = std::collections::BTreeMap::new();
						obj.insert(#tag.to_string(), #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())));
						#kind_ty::Literal(#kind_literal_ty::Object(obj))
					}}
				}
				Strategy::TagContentKeys {
					tag,
					variant,
					content,
					..
				} => {
					quote! {{
						let mut obj = std::collections::BTreeMap::new();
						obj.insert(#tag.to_string(), #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())));
						obj.insert(#content.to_string(), #kind_ty::Literal(#kind_literal_ty::Object(std::collections::BTreeMap::new())));
						#kind_ty::Literal(#kind_literal_ty::Object(obj))
					}}
				}
				Strategy::Value {
					variant,
				} => {
					if let Some(UnitValue {
						kind_of,
						..
					}) = attrs.value.as_ref()
					{
						quote! ( #kind_of )
					} else if let Some(variant) = variant {
						quote! { #kind_ty::Literal(#kind_literal_ty::String(#variant.to_string())) }
					} else {
						quote! { #kind_ty::Literal(#kind_literal_ty::Object(std::collections::BTreeMap::new())) }
					}
				}
			},
		}
	}
}
