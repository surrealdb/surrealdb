use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::parse::ParseBuffer;
use syn::{Attribute, Lit};

#[derive(Debug, Default)]
pub struct UnitAttributes {
	pub value: Option<UnitValue>,
}

impl UnitAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut variant_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("value") {
						let Ok(value) = meta.value() else {
							panic!("Failed to parse value attribute");
						};

						variant_attrs.value = Some(UnitValue::parse(value));
					}

					Ok(())
				})
				.ok();
			}
		}

		variant_attrs
	}
}

#[derive(Debug)]
pub struct UnitValue {
	pub inner: TokenStream2,
	pub value: TokenStream2,
	pub is_value: TokenStream2,
	pub kind_of: TokenStream2,
}

impl UnitValue {
	pub fn parse(buf: &ParseBuffer<'_>) -> Self {
		// Check for custom tokens first (null, none)
		if buf.peek(syn::Ident) {
			let ident = buf.parse::<syn::Ident>().unwrap();
			let ident_str = ident.to_string().to_lowercase();
			let inner = ident.to_token_stream();

			let (value, is_value, kind_of) = match ident_str.as_str() {
				"none" => (
					quote!(surrealdb_types::Value::None),
					quote!(value.is_none()),
					quote!(surrealdb_types::Kind::None),
				),
				"null" => (
					quote!(surrealdb_types::Value::Null),
					quote!(value.is_null()),
					quote!(surrealdb_types::Kind::Null),
				),
				_ => panic!(
					"Invalid identifier: {}. Only null and none identifiers are supported.",
					ident
				),
			};

			return UnitValue {
				inner,
				value,
				is_value,
				kind_of,
			};
		}

		// Fall back to parsing as literal
		let lit = buf.parse::<Lit>().unwrap();
		let inner = lit.to_token_stream();

		let (value, is_value, kind_of) = match lit {
			// Literal true
			Lit::Bool(x) if x.value => (
				quote!(surrealdb_types::Value::Bool(true)),
				quote!(value.is_true()),
				quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Bool(true))),
			),
			// Literal false
			Lit::Bool(x) if !x.value => (
				quote!(surrealdb_types::Value::Bool(false)),
				quote!(value.is_false()),
				quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Bool(false))),
			),
			// Literal string
			Lit::Str(x) => {
				let inner = x.value();
				(
					quote!(surrealdb_types::Value::String(#inner.to_string())),
					quote!(value.is_string_and(|s| s == #inner)),
					quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#inner.to_string()))),
				)
			}
			// Literal integer
			Lit::Int(x) => {
				let inner = x.base10_digits().parse::<i64>().unwrap();
				(
					quote!(surrealdb_types::Value::Number(surrealdb_types::Number::Int(#inner))),
					quote!(value.is_int_and(|i| i == &#inner)),
					quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Integer(#inner))),
				)
			}
			// Literal float
			Lit::Float(x) => {
				let inner = x.base10_digits().parse::<f64>().unwrap();
				(
					quote!(surrealdb_types::Value::Number(surrealdb_types::Number::Float(#inner))),
					quote!(value.is_float_and(|f| f == &#inner)),
					quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Float(#inner))),
				)
			}
			_ => panic!(
				"Invalid value: {}. Only literal boolean, string, integer, float, null and none are supported.",
				inner
			),
		};

		UnitValue {
			inner,
			value,
			is_value,
			kind_of,
		}
	}
}
