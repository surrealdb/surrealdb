use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Field, Fields, Ident, LitStr, Type, parse_macro_input};

/// Field-level attributes parsed from #[surreal(...)]
#[derive(Debug, Default)]
struct FieldAttributes {
	/// Renamed field name if specified
	rename: Option<String>,
	/// Whether the field should be flattened
	flatten: bool,
}

/// Parse field-level attributes from a field
fn parse_field_attributes(field: &Field) -> FieldAttributes {
	let mut field_attrs = FieldAttributes::default();

	for attr in &field.attrs {
		if attr.path().is_ident("surreal") {
			attr.parse_nested_meta(|meta| {
				if meta.path.is_ident("rename") {
					if let Ok(value) = meta.value() {
						if let Ok(lit_str) = value.parse::<LitStr>() {
							field_attrs.rename = Some(lit_str.value());
						}
					}
				} else if meta.path.is_ident("flatten") {
					field_attrs.flatten = true;
				}
				Ok(())
			})
			.ok();
		}
	}

	field_attrs
}

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
/// ```compile_fail
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
/// ```compile_fail
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
/// ```compile_fail
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
/// ```compile_fail
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
/// ```compile_fail
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
///
/// # Attributes
///
/// The macro supports field-level attributes via `#[surreal(...)]`:
///
/// - `#[surreal(rename = "field_name")]` - Renames a field when serializing/deserializing
/// - `#[surreal(flatten)]` - Flattens a nested object's fields into the parent
///
/// # Serde Integration
///
/// While this derive macro cannot automatically add serde derives (a limitation of Rust's
/// derive macro system), it's designed to work seamlessly with serde when you manually add
/// both derives with matching attributes:
///
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use surrealdb_types::SurrealValue;
///
/// #[derive(SurrealValue, Serialize, Deserialize)]
/// struct Person {
///     #[surreal(rename = "full_name")]
///     #[serde(rename = "full_name")]
///     name: String,
///     #[surreal(flatten)]
///     #[serde(flatten)]
///     address: Address,
/// }
/// ```
///
/// This pattern ensures consistent serialization between SurrealDB values and JSON.
#[proc_macro_derive(SurrealValue, attributes(surreal))]
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

	let surreal_impl = match fields {
		Fields::Named(named_fields) => generate_named_fields_impl(
			&name,
			&impl_generics,
			&ty_generics,
			&where_clause,
			named_fields,
		),
		Fields::Unnamed(unnamed_fields) => generate_unnamed_fields_impl(
			&name,
			&impl_generics,
			&ty_generics,
			&where_clause,
			unnamed_fields,
		),
		Fields::Unit => {
			generate_unit_struct_impl(&name, &impl_generics, &ty_generics, &where_clause)
		}
	};

	surreal_impl.into()
}

/// Generate implementation for structs with named fields
fn generate_named_fields_impl(
	name: &Ident,
	impl_generics: &syn::ImplGenerics,
	ty_generics: &syn::TypeGenerics,
	where_clause: &Option<&syn::WhereClause>,
	named_fields: &syn::FieldsNamed,
) -> proc_macro2::TokenStream {
	let mut regular_fields = Vec::new();
	let mut flattened_fields = Vec::new();

	// Process each field and parse its attributes
	for field in &named_fields.named {
		let field_name = field.ident.as_ref().unwrap();
		let field_attrs = parse_field_attributes(field);

		if field_attrs.flatten {
			flattened_fields.push((field_name, &field.ty, field_attrs));
		} else {
			regular_fields.push((field_name, &field.ty, field_attrs));
		}
	}

	// Generate the field processing code
	let (
		is_value_checks,
		into_value_fields,
		from_value_fields,
		field_kinds,
		field_names_for_constructor,
	) = generate_field_processing(&regular_fields, &flattened_fields);

	quote! {
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
				let mut map = std::collections::BTreeMap::new();
				#(#into_value_fields)*
				surrealdb_types::Value::Object(surrealdb_types::Object::from(map))
			}

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				let surrealdb_types::Value::Object(obj) = value else {
					return Err(surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Expected Object, got {:?}", Self::kind_of(), value.value_kind()));
				};

				#(#from_value_fields)*

				Ok(Self {
					#(#field_names_for_constructor),*
				})
			}
		}
	}
}

/// Generate field processing code for named fields
fn generate_field_processing(
	regular_fields: &[(&Ident, &Type, FieldAttributes)],
	flattened_fields: &[(&Ident, &Type, FieldAttributes)],
) -> (
	Vec<proc_macro2::TokenStream>,
	Vec<proc_macro2::TokenStream>,
	Vec<proc_macro2::TokenStream>,
	Vec<proc_macro2::TokenStream>,
	Vec<proc_macro2::TokenStream>,
) {
	let mut is_value_checks = Vec::new();
	let mut into_value_fields = Vec::new();
	let mut from_value_fields = Vec::new();
	let mut field_kinds = Vec::new();
	let mut field_names_for_constructor = Vec::new();

	// Handle regular fields
	for (field_name, field_type, field_attrs) in regular_fields {
		let field_name_str = field_name.to_string();
		let field_key = field_attrs.rename.as_ref().unwrap_or(&field_name_str);

		is_value_checks.push(quote! {
			obj.get(#field_key).map_or(false, |v| <#field_type as surrealdb_types::SurrealValue>::is_value(v))
		});

		into_value_fields.push(quote! {
			map.insert(#field_key.to_string(), self.#field_name.into_value());
		});

		from_value_fields.push(quote! {
			let #field_name = <#field_type as surrealdb_types::SurrealValue>::from_value(
				obj.get(#field_key)
					.ok_or_else(|| surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Missing field '{}'", Self::kind_of(), #field_key))?
					.clone()
			).map_err(|e| surrealdb_types::anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;
		});

		field_kinds.push(quote! {
			(#field_key.to_string(), <#field_type as surrealdb_types::SurrealValue>::kind_of())
		});

		field_names_for_constructor.push(quote! { #field_name });
	}

	// Handle flattened fields
	for (field_name, field_type, _field_attrs) in flattened_fields {
		is_value_checks.push(quote! {
			<#field_type as surrealdb_types::SurrealValue>::is_value(&surrealdb_types::Value::Object(obj.clone()))
		});

		into_value_fields.push(quote! {
			if let surrealdb_types::Value::Object(flattened_obj) = self.#field_name.into_value() {
				for (k, v) in flattened_obj {
					map.insert(k, v);
				}
			}
		});

		from_value_fields.push(quote! {
			let #field_name = <#field_type as surrealdb_types::SurrealValue>::from_value(
				surrealdb_types::Value::Object(obj.clone())
			).map_err(|e| surrealdb_types::anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;
		});

		field_names_for_constructor.push(quote! { #field_name });
	}

	(
		is_value_checks,
		into_value_fields,
		from_value_fields,
		field_kinds,
		field_names_for_constructor,
	)
}

/// Generate implementation for structs with unnamed fields (tuple structs)
fn generate_unnamed_fields_impl(
	name: &Ident,
	impl_generics: &syn::ImplGenerics,
	ty_generics: &syn::TypeGenerics,
	where_clause: &Option<&syn::WhereClause>,
	unnamed_fields: &syn::FieldsUnnamed,
) -> proc_macro2::TokenStream {
	let field_types: Vec<&Type> = unnamed_fields.unnamed.iter().map(|f| &f.ty).collect();
	let field_count = field_types.len();

	// If there is only one field, treat it as a transparent wrapper around the inner type.
	if field_count == 1 {
		let field_type = &field_types[0];
		return quote! {
			impl #impl_generics surrealdb_types::SurrealValue for #name #ty_generics #where_clause {
				fn kind_of() -> surrealdb_types::Kind {
					<#field_type as surrealdb_types::SurrealValue>::kind_of()
				}

				fn is_value(value: &surrealdb_types::Value) -> bool {
					<#field_type as surrealdb_types::SurrealValue>::is_value(value)
				}

				fn into_value(self) -> surrealdb_types::Value {
					self.0.into_value()
				}

				fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
					<#field_type as surrealdb_types::SurrealValue>::from_value(value).map(Self)
				}
			}
		};
	}

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
			let field_type = &field_types[i];
			let field_ident =
				syn::Ident::new(&format!("field_{}", i), proc_macro2::Span::call_site());
			quote! {
				let #field_ident = <#field_type as surrealdb_types::SurrealValue>::from_value(
					values.get(#i)
						.ok_or_else(|| surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Missing field at index {}", Self::kind_of(), #i))?
						.clone()
				).map_err(|e| surrealdb_types::anyhow::anyhow!("Failed to convert to {}: {}", Self::kind_of(), e))?;
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

	quote! {
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

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				let surrealdb_types::Value::Array(values) = value else {
					return Err(surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Expected Array, got {:?}", Self::kind_of(), value.value_kind()));
				};

				if values.len() != #field_count {
					return Err(surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Expected Array of length {}, got {}", Self::kind_of(), #field_count, values.len()));
				}

				#(#from_value_fields)*

				Ok(Self(
					#(#field_assignments),*
				))
			}
		}
	}
}

/// Generate implementation for unit structs
fn generate_unit_struct_impl(
	name: &Ident,
	impl_generics: &syn::ImplGenerics,
	ty_generics: &syn::TypeGenerics,
	where_clause: &Option<&syn::WhereClause>,
) -> proc_macro2::TokenStream {
	quote! {
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

			fn from_value(value: surrealdb_types::Value) -> surrealdb_types::anyhow::Result<Self> {
				match value {
					surrealdb_types::Value::Object(obj) if obj.is_empty() => Ok(Self),
					_ => Err(surrealdb_types::anyhow::anyhow!("Failed to convert to {}: Expected empty Object, got {:?}", Self::kind_of(), value.value_kind())),
				}
			}
		}
	}
}
