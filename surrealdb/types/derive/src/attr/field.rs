use syn::{Field, Lit, LitStr, Path};

#[derive(Debug, Default)]
pub struct FieldAttributes {
	pub rename: Option<String>,
	/// Default for a missing field during from_value.
	pub default: Option<FieldDefault>,
	pub wrap: bool,
	/// When true, this field's serialized object is merged into the parent instead of
	/// being inserted under a single key. Like serde's `#[serde(flatten)]`.
	pub flatten: bool,
}

/// Per-field default for deserialization when the key is missing.
#[derive(Debug, Clone)]
pub enum FieldDefault {
	/// Use `<Type>::default()`.
	UseDefault,
	/// Use the given path as a function call (e.g. `default_code()`).
	Path(Path),
	/// Use a Rust literal, converting string literals into the field type.
	Literal(Lit),
}

impl FieldAttributes {
	pub fn parse(input: &Field) -> Self {
		let mut field_attrs = Self::default();

		for attr in &input.attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("rename") {
						let Ok(value) = meta.value() else {
							panic!("Failed to parse rename attribute");
						};

						let Ok(lit_str) = value.parse::<LitStr>() else {
							panic!("Failed to parse rename attribute");
						};

						field_attrs.rename = Some(lit_str.value());
					} else if meta.path.is_ident("flatten") {
						field_attrs.flatten = true;
					} else if meta.path.is_ident("wrap") {
						field_attrs.wrap = true;
					} else if meta.path.is_ident("default") {
						if meta.input.peek(syn::token::Eq) {
							let value = meta
								.value()
								.expect("surreal(default = ...) requires a string value");
							let lit_str: LitStr = value
								.parse()
								.expect("surreal(default = ...) value must be a string");
							field_attrs.default =
								Some(if let Ok(literal) = lit_str.parse::<Lit>() {
									FieldDefault::Literal(literal)
								} else {
									FieldDefault::Path(lit_str.parse::<Path>().expect(
										"surreal(default = \"...\") must be a Rust literal or function path",
									))
								});
						} else {
							// #[surreal(default)]
							field_attrs.default = Some(FieldDefault::UseDefault);
						}
					}

					Ok(())
				})
				.ok();
			}
		}

		if field_attrs.flatten && field_attrs.rename.is_some() {
			panic!(
				"Cannot combine #[surreal(flatten)] with #[surreal(rename)] on the same field; `flatten` merges the field's contents into the parent object, so there is no key for `rename` to change"
			);
		}

		field_attrs
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_field_default() {
		let field = syn::parse_quote! {
			#[surreal(default)]
			value: bool
		};
		assert!(matches!(FieldAttributes::parse(&field).default, Some(FieldDefault::UseDefault)));
	}

	#[test]
	fn parse_function_default() {
		let field = syn::parse_quote! {
			#[surreal(default = "defaults::enabled")]
			value: bool
		};
		assert!(matches!(FieldAttributes::parse(&field).default, Some(FieldDefault::Path(_))));
	}

	#[test]
	fn parse_literal_default() {
		let field = syn::parse_quote! {
			#[surreal(default = "true")]
			value: bool
		};
		assert!(matches!(
			FieldAttributes::parse(&field).default,
			Some(FieldDefault::Literal(Lit::Bool(literal))) if literal.value
		));
	}

	#[test]
	#[should_panic(expected = "must be a Rust literal or function path")]
	fn reject_default_expression() {
		let field = syn::parse_quote! {
			#[surreal(default = "1 + 2")]
			value: i64
		};
		FieldAttributes::parse(&field);
	}

	#[test]
	#[should_panic(expected = "must be a Rust literal or function path")]
	fn reject_malformed_default() {
		let field = syn::parse_quote! {
			#[surreal(default = "\"unterminated")]
			value: String
		};
		FieldAttributes::parse(&field);
	}

	#[test]
	#[should_panic(expected = "value must be a string")]
	fn reject_unquoted_default() {
		let field = syn::parse_quote! {
			#[surreal(default = true)]
			value: bool
		};
		FieldAttributes::parse(&field);
	}
}
