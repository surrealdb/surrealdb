use syn::{Attribute, LitStr};

use crate::SkipContent;

#[derive(Debug, Default)]
pub struct NamedFieldsAttributes {
	pub default: bool,
	/// Per-variant content skipping. `Always` for `#[surreal(skip_content)]`,
	/// `If(path)` for `#[surreal(skip_content_if = "predicate")]`.
	pub skip_content: Option<SkipContent>,
}

impl NamedFieldsAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut named_field_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("default") {
						named_field_attrs.default = true;
					} else if meta.path.is_ident("skip_content") {
						if named_field_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same variant"
							);
						}
						named_field_attrs.skip_content = Some(SkipContent::Always);
					} else if meta.path.is_ident("skip_content_if") {
						if named_field_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same variant"
							);
						}
						if let Ok(value) = meta.value()
							&& let Ok(lit_str) = value.parse::<LitStr>()
						{
							named_field_attrs.skip_content = Some(SkipContent::If(
								syn::parse_str(&lit_str.value())
									.expect("skip_content_if must be a valid path"),
							));
						}
					}

					Ok(())
				})
				.ok();
			}
		}

		named_field_attrs
	}
}
