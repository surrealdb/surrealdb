mod attr;
use attr::*;

mod structs;
use structs::*;

mod r#impl;
use r#impl::*;
use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod kind;

#[proc_macro_derive(SurrealValue, attributes(surreal))]
pub fn surreal_value(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	let name = &input.ident;
	let generics = &input.generics;

	match &input.data {
		syn::Data::Struct(data) => {
			let fields = Fields::parse(&data.fields, &input.attrs);
			impl_struct(name, generics, fields)
		}
		syn::Data::Enum(data) => {
			let r#enum = Enum::parse(data, &input.attrs);
			impl_enum(name, generics, r#enum)
		}
		syn::Data::Union(_) => panic!("SurrealValue cannot be derived for unions"),
	}
}

/// A procedural macro for creating `Kind` values with a convenient DSL syntax.
///
/// This macro provides a rich syntax for constructing type definitions, including:
/// - Basic types: `string`, `bool`, `int`, `datetime`, etc.
/// - Parameterized types: `array<string>`, `set<int, 5>`, `record<user | post>`
/// - Union types: `string | bool | int`
/// - Literal values: `true`, `false`, `42`, `"hello"`
/// - Object literals: `{ status: string, "user-id": int }`
/// - Array literals: `[string, int, bool]`
/// - Escape hatches: `(expr)` for arbitrary Rust expressions
/// - Type prefixes: `Kind::String`, `Literal::Bool(true)`
///
/// # Examples
///
/// ```ignore
/// use surrealdb_types::{kind, Kind};
///
/// // Basic types
/// let string_kind = kind!(string);
/// let array_of_strings = kind!(array<string>);
///
/// // Union types
/// let string_or_int = kind!(string | int);
///
/// // Object literals with mixed key styles
/// let response_type = kind!({
///     status: string,
///     "user-id": int,
///     data: any
/// });
///
/// // Dynamic types with escape hatch
/// fn generic_array<T: SurrealValue>() -> Kind {
///     kind!(array<(T::kind_of())>)
/// }
/// ```
#[proc_macro]
pub fn kind(input: TokenStream) -> TokenStream {
	kind::kind(input)
}
