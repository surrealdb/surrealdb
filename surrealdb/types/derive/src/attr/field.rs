use syn::{Field, LitStr, Path};

#[derive(Debug, Default)]
pub struct FieldAttributes {
	pub rename: Option<String>,
	/// When set, missing field during from_value uses this default (either Default::default() or a
	/// path).
	pub default: Option<FieldDefault>,
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
					} else if meta.path.is_ident("default") {
						if meta.input.peek(syn::token::Eq) {
							// #[surreal(default = "path")]
							let value =
								meta.value().expect("surreal(default = ...) requires a path value");
							let lit_str: LitStr = value
								.parse()
								.expect("surreal(default = ...) value must be a string");
							let path = syn::parse_str::<Path>(&lit_str.value())
								.expect("surreal(default = \"...\") must be a valid path");
							field_attrs.default = Some(FieldDefault::Path(path));
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

		field_attrs
	}
}
