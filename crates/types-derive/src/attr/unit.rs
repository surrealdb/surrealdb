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
		let lit = buf.parse::<Lit>().unwrap();
		let inner = lit.to_token_stream();
		let value = quote!(surrealdb_types::Value::from_t(#inner));

		let (is_value, kind_of) = match lit {
			// Literal true
			Lit::Bool(x) if x.value => (
				quote!(value.is_true()),
				quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Bool(true))),
			),
			// Literal false
			Lit::Bool(x) if !x.value => (
				quote!(value.is_false()),
				quote!(surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Bool(false))),
			),
			// Literal string
			Lit::Str(x) => {
				let inner = x.value();
				(
					quote! {
						if let surrealdb_types::Value::String(x) = &value {
							x == #inner
						}
					},
					quote!( surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#inner)) ),
				)
			}
			// Literal integer
			Lit::Int(x) => {
				let inner = x.base10_digits().parse::<i64>().unwrap();
				(
					quote! {
						if let surrealdb_types::Value::Number(surrealdb_types::Number::Int(x)) = &value {
							x == #inner
						}
					},
					quote!( surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Integer(#inner)) ),
				)
			}
			// Literal float
			Lit::Float(x) => {
				let inner = x.base10_digits().parse::<f64>().unwrap();
				(
					quote! {
						if let surrealdb_types::Value::Number(surrealdb_types::Number::Float(x)) = &value {
							x == #inner
						}
					},
					quote!( surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Float(#inner)) ),
				)
			}
			Lit::Verbatim(x) => {
				let literal_str = x.to_string().to_lowercase();
				match literal_str.as_str() {
					"none" => (quote!(value.is_none()), quote!(surrealdb_types::Kind::None)),
					"null" => (quote!(value.is_null()), quote!(surrealdb_types::Kind::Null)),
					_ => panic!(
						"Invalid value: {}. Only literal boolean, string, integer, float, null and none are supported.",
						inner
					),
				}
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
