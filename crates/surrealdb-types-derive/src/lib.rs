use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, Type, parse_macro_input};

/// Derive macro for implementing the `SurrealValue` trait on structs.
///
/// This macro automatically implements the `SurrealValue` trait for structs, providing
/// seamless conversion between Rust structs and SurrealDB `Value` types. The implementation
/// supports named fields, tuple structs, and unit structs.
///
/// # Examples
///
/// ## Named Fields Struct
///
/// ```rust
/// use surrealdb_types::{SurrealValue, Value};
///
/// #[derive(SurrealValue)]
/// struct Person {
///     name: String,
///     age: i64,
///     active: bool,
/// }
///
/// // Convert struct to SurrealDB Value
/// let person = Person {
///     name: "Alice".to_string(),
///     age: 30,
///     active: true,
/// };
///
/// // Converted to Value::Object as { "name": "Alice", "age": 30, "active": true }
/// let value: Value = person.into_value();
///
/// // Convert SurrealDB Value back to struct
/// let restored_person = Person::from_value(value).unwrap();
/// assert_eq!(restored_person.name, "Alice");
/// assert_eq!(restored_person.age, 30);
/// assert_eq!(restored_person.active, true);
///
/// // Check if a Value represents this type
/// assert!(Person::is_value(&value));
/// ```
///
/// ## Tuple Struct
///
/// ```rust
/// use surrealdb_types::{SurrealValue, Value};
///
/// #[derive(SurrealValue)]
/// struct Point(i64, i64);
///
/// let point = Point(10, 20);
/// let value: Value = point.into_value();
///
/// let restored_point = Point::from_value(value).unwrap();
/// assert_eq!(restored_point.0, 10);
/// assert_eq!(restored_point.1, 20);
/// ```
///
/// ## Unit Struct
///
/// ```rust
/// use surrealdb_types::{SurrealValue, Value};
///
/// #[derive(SurrealValue)]
/// struct Empty;
///
/// let empty = Empty;
/// let value: Value = empty.into_value();
///
/// let restored_empty = Empty::from_value(value).unwrap();
/// ```
///
/// ## Nested Structs
///
/// ```rust
/// use surrealdb_types::{SurrealValue, Value};
///
/// #[derive(SurrealValue)]
/// struct Address {
///     street: String,
///     city: String,
/// }
///
/// #[derive(SurrealValue)]
/// struct Employee {
///     name: String,
///     address: Address,
///     skills: Vec<String>,
/// }
///
/// let employee = Employee {
///     name: "Bob".to_string(),
///     address: Address {
///         street: "123 Main St".to_string(),
///         city: "Anytown".to_string(),
///     },
///     skills: vec!["Rust".to_string(), "SurrealDB".to_string()],
/// };
///
/// let value: Value = employee.into_value();
/// let restored_employee = Employee::from_value(value).unwrap();
/// ```
///
/// ## Generic Structs
///
/// ```rust
/// use surrealdb_types::{SurrealValue, Value};
///
/// #[derive(SurrealValue)]
/// struct Container<T> {
///     data: T,
///     metadata: String,
/// }
///
/// // Works with any type that implements SurrealValue
/// let container = Container {
///     data: vec![1, 2, 3],
///     metadata: "numbers".to_string(),
/// };
///
/// let value: Value = container.into_value();
/// let restored_container = Container::<Vec<i64>>::from_value(value).unwrap();
/// ```
///
/// # Generated Methods
///
/// The derive macro generates implementations for all four methods of the `SurrealValue` trait:
///
/// - `kind_of()` - Returns the `Kind` that represents this type's structure
/// - `is_value(value)` - Checks if a `Value` can be converted to this type
/// - `into_value(self)` - Converts the struct into a `Value`
/// - `from_value(value)` - Attempts to convert a `Value` back to the struct
///
/// # Limitations
///
/// - Only works with structs (not enums)
/// - All field types must implement `SurrealValue`
/// - Named fields are converted to/from SurrealDB objects
/// - Tuple structs are converted to/from SurrealDB arrays
/// - Unit structs are converted to/from empty SurrealDB objects
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
						let #field_name = <#field_type as surrealdb_types::SurrealValue>::from_value(
							obj.get(#field_name_str)
								.ok_or_else(|| anyhow::anyhow!("Failed to convert to {}: Missing field '{}'", Self::kind_of(), #field_name_str))?
								.clone()
						).map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;
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
						if let surrealdb_types::Value::Object(obj) = value {
							#(#is_value_checks)&&*
						} else {
							false
						}
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Object(surrealdb_types::Object::from(std::collections::BTreeMap::from([
							#(#into_value_fields),*
						])))
					}

					fn from_value(value: surrealdb_types::Value) -> anyhow::Result<Self> {
						let surrealdb_types::Value::Object(obj) = value else {
							return Err(anyhow::anyhow!("Failed to convert to {}: Expected Object, got {:?}", Self::kind_of(), value.value_kind()));
						};

						#(#from_value_fields)*

						Ok(Self {
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
						let #field_ident = <#field_type as surrealdb_types::SurrealValue>::from_value(
							values.get(#i)
								.ok_or_else(|| anyhow::anyhow!("Failed to convert to {}: Missing field at index {}", Self::kind_of(), #i))?
								.clone()
						).map_err(|e| anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;
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
						if let surrealdb_types::Value::Array(values) = value {
							values.len() == #field_count && #(#is_value_checks)&&*
						} else {
							false
						}
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Array(surrealdb_types::Array::from(vec![
							#(#into_value_fields),*
						]))
					}

					fn from_value(value: surrealdb_types::Value) -> anyhow::Result<Self> {
						let surrealdb_types::Value::Array(values) = value else {
							return Err(anyhow::anyhow!("Failed to convert to {}: Expected Array, got {:?}", Self::kind_of(), value.value_kind()));
						};

						if values.len() != #field_count {
							return Err(anyhow::anyhow!("Failed to convert to {}: Expected Array of length {}, got {}", Self::kind_of(), #field_count, values.len()));
						}

						#(#from_value_fields)*

						Ok(Self(
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
						matches!(value, surrealdb_types::Value::Object(obj) if obj.is_empty())
					}

					fn into_value(self) -> surrealdb_types::Value {
						surrealdb_types::Value::Object(surrealdb_types::Object::new())
					}

					fn from_value(value: surrealdb_types::Value) -> anyhow::Result<Self> {
						match value {
							surrealdb_types::Value::Object(obj) if obj.is_empty() => Ok(Self),
							_ => Err(anyhow::anyhow!("Failed to convert to {}: Expected empty Object, got {:?}", Self::kind_of(), value.value_kind())),
						}
					}
				}
			};

			output.into()
		}
	}
}
