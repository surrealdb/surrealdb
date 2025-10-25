mod named;
pub use named::*;

mod unnamed;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::Attribute;
pub use unnamed::*;

use crate::{
	FieldAttributes, NamedFieldsAttributes, Strategy, UnitAttributes, UnitValue,
	UnnamedFieldsAttributes, With,
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
						let field_name = field.ident.as_ref().unwrap();
						let field_attrs = FieldAttributes::parse(field);
						NamedField {
							ident: field_name.clone(),
							ty: field.ty.clone(),
							rename: field_attrs.rename,
						}
					})
					.collect();

				Fields::Named(NamedFields {
					fields,
					default: container_attrs.default,
				})
			}
			syn::Fields::Unnamed(unnamed_fields) => {
				let unnamed_field_attrs = UnnamedFieldsAttributes::parse(attrs);
				let fields = unnamed_fields.unnamed.iter().map(|field| field.ty.clone()).collect();

				Fields::Unnamed(UnnamedFields::new(fields, unnamed_field_attrs.tuple))
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

	#[allow(clippy::wrong_self_convention)]
	pub fn into_value(&self, strategy: &Strategy) -> TokenStream2 {
		match self {
			Fields::Named(fields) => {
				let map_assignments = fields.map_assignments();

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut map = surrealdb_types::Object::new();
							map.insert(#variant.to_string(), {
								let mut map = surrealdb_types::Object::new();
								#(#map_assignments)*
								surrealdb_types::Value::Object(map)
							});
							surrealdb_types::Value::Object(map)
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
							let mut map = surrealdb_types::Object::new();
							map.insert(#tag.to_string(), surrealdb_types::Value::String(#variant.to_string()));
							#(#map_assignments)*
							surrealdb_types::Value::Object(map)
						}}
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
					} => {
						quote! {{
							let mut map = surrealdb_types::Object::new();
							map.insert(#tag.to_string(), surrealdb_types::Value::String(#variant.to_string()));
							map.insert(#content.to_string(), {
								let mut map = surrealdb_types::Object::new();
								#(#map_assignments)*
								surrealdb_types::Value::Object(map)
							});
							surrealdb_types::Value::Object(map)
						}}
					}
					Strategy::Value {
						..
					} => {
						quote! {{
							let mut map = surrealdb_types::Object::new();
							#(#map_assignments)*
							surrealdb_types::Value::Object(map)
						}}
					}
				}
			}
			Fields::Unnamed(x) => {
				let value = if !x.tuple && x.fields.len() == 1 {
					quote!(surrealdb_types::Value::from_t(field_0))
				} else {
					let arr_assignments = x.arr_assignments();
					quote! {{
						let mut arr = surrealdb_types::Array::new();
						#(#arr_assignments)*
						surrealdb_types::Value::Array(arr)
					}}
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut map = surrealdb_types::Object::new();
							map.insert(#variant.to_string(), #value);
							surrealdb_types::Value::Object(map)
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
					} => {
						quote! {{
							let mut map = surrealdb_types::Object::new();
							map.insert(#tag.to_string(), surrealdb_types::Value::String(#variant.to_string()));
							map.insert(#content.to_string(), #value);
							surrealdb_types::Value::Object(map)
						}}
					}
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
						let mut map = surrealdb_types::Object::new();
						map.insert(#variant.to_string(), surrealdb_types::Value::Object(surrealdb_types::Object::new()));
						surrealdb_types::Value::Object(map)
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
						let mut map = surrealdb_types::Object::new();
						map.insert(#tag.to_string(), surrealdb_types::Value::String(#variant.to_string()));
						surrealdb_types::Value::Object(map)
					}}
				}
				Strategy::TagContentKeys {
					tag,
					variant,
					content,
				} => {
					if attrs.value.is_some() {
						panic!("Unit variants can only have a value with untagged enums");
					}

					quote! {{
						let mut map = surrealdb_types::Object::new();
						map.insert(#tag.to_string(), surrealdb_types::Value::String(#variant.to_string()));
						map.insert(#content.to_string(), surrealdb_types::Value::Object(surrealdb_types::Object::new()));
						surrealdb_types::Value::Object(map)
					}}
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
						quote!(surrealdb_types::Value::String(#variant.to_string()))
					} else {
						quote!(surrealdb_types::Value::Object(surrealdb_types::Object::new()))
					}
				}
			},
		}
	}

	#[allow(clippy::wrong_self_convention)]
	pub fn from_value(&self, name: &String, strategy: &Strategy, ok: TokenStream2) -> With {
		match self {
			Fields::Named(fields) => {
				let map_retrievals = fields.map_retrievals(name);

				let final_ok = if fields.default {
					quote!(Ok(result))
				} else {
					ok.clone()
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => With::Map(quote! {{
						if let Some(value) = map.remove(#variant) {
							if let surrealdb_types::Value::Object(mut map) = value {
								#(#map_retrievals)*
								#final_ok
							} else {
								let err = surrealdb_types::ConversionError::from_value(
									surrealdb_types::Kind::Object,
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
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v == Value::String(#variant.to_string())) {
							if let Some(surrealdb_types::Value::Object(mut map)) = map.remove(#content) {
								#(#map_retrievals)*
								#final_ok
							} else {
								let err = surrealdb_types::TypeError::Invalid(
									format!("Expected object under content key '{}' for variant '{}'", #content, #variant)
								);
								return Err(err.into())
							}
						}
					}}),
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
						} => With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
								if let Some(value) = map.remove(#content) {
									#retrieve
								} else {
									let err = surrealdb_types::TypeError::Invalid(
										format!("Expected content key '{}' for variant '{}'", #content, #variant)
									);
									return Err(err.into())
								}
							}
						}}),
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
						if let surrealdb_types::Value::Array(mut arr) = value {
							#retrieve_arr
						} else {
							let err = surrealdb_types::ConversionError::from_value(
								surrealdb_types::Kind::Array(Box::new(surrealdb_types::Kind::Any), None),
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
						} => With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
								if let Some(value) = map.remove(#content) {
									#retrieve_value
								} else {
									return Err(surrealdb_types::anyhow::anyhow!("Expected content key"))
								}
							}
						}}),
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
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
							if map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty())) {
								#ok
							} else {
								let err = surrealdb_types::TypeError::Invalid(
									format!("Expected empty object under content key '{}' for variant '{}'", #content, #variant)
								);
								return Err(err.into())
							}
						}
					}}),
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
									let err = surrealdb_types::TypeError::Invalid(
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
									let err = surrealdb_types::TypeError::Invalid(
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
	pub fn is_value(&self, strategy: &Strategy) -> With {
		match self {
			Fields::Named(fields) => {
				let field_checks = fields.field_checks();

				match strategy {
					Strategy::VariantKey {
						variant,
					} => With::Map(quote! {{
						if let Some(surrealdb_types::Value::Object(map)) = map.get(#variant) {
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
					} => With::Map(quote! {{
						if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
							if let Some(surrealdb_types::Value::Object(map)) = map.get(#content) {
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
						} => With::Map(quote! {{
							if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) {
								if let Some(value) = map.get(#content) {
									return #check;
								}
							}
						}}),
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
						if let surrealdb_types::Value::Array(arr) = value {
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
				} => With::Map(quote! {{
					if map.get(#tag).is_some_and(|v| v.is_string_and(|s| s == #variant)) &&
						map.get(#content).is_some_and(|v| v.is_object_and(|o| o.is_empty()))
					{
						return true;
					}
				}}),
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

	pub fn kind_of(&self, strategy: &Strategy) -> TokenStream2 {
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
								surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
							});
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
						}}
					}
					Strategy::TagKey {
						tag,
						variant,
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							map.insert(#tag.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())));
							#(#map_types)*
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
						}}
					}
					Strategy::TagContentKeys {
						tag,
						variant,
						content,
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							map.insert(#tag.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())));
							map.insert(#content.to_string(), {
								let mut map = std::collections::BTreeMap::new();
								#(#map_types)*
								surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
							});
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
						}}
					}
					Strategy::Value {
						..
					} => {
						quote! {{
							let mut map = std::collections::BTreeMap::new();
							#(#map_types)*
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(map))
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
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Array(arr))
					}}
				};

				match strategy {
					Strategy::VariantKey {
						variant,
					} => {
						quote! {{
							let mut obj = std::collections::BTreeMap::new();
							obj.insert(#variant.to_string(), #kind_of);
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(obj))
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
					} => {
						quote! {{
							let mut obj = std::collections::BTreeMap::new();
							obj.insert(#tag.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())));
							obj.insert(#content.to_string(), #kind_of);
							surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(obj))
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
						obj.insert(#variant.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::new())));
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(obj))
					}}
				}
				Strategy::TagKey {
					tag,
					variant,
				} => {
					quote! {{
						let mut obj = std::collections::BTreeMap::new();
						obj.insert(#tag.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())));
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(obj))
					}}
				}
				Strategy::TagContentKeys {
					tag,
					variant,
					content,
				} => {
					quote! {{
						let mut obj = std::collections::BTreeMap::new();
						obj.insert(#tag.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())));
						obj.insert(#content.to_string(), surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::new())));
						surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(obj))
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
						quote! { surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#variant.to_string())) }
					} else {
						quote! { surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Object(std::collections::BTreeMap::new())) }
					}
				}
			},
		}
	}
}
