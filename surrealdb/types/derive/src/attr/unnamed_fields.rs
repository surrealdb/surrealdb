use syn::{Attribute, LitStr};

use crate::SkipContent;

#[derive(Debug, Default)]
pub struct UnnamedFieldsAttributes {
	pub tuple: bool,
	/// Per-variant content skipping. `Always` for `#[surreal(skip_content)]`,
	/// `If(path)` for `#[surreal(skip_content_if = "predicate")]`.
	pub skip_content: Option<SkipContent>,
}

impl UnnamedFieldsAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut unnamed_field_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("tuple") {
						unnamed_field_attrs.tuple = true;
					} else if meta.path.is_ident("skip_content") {
						if unnamed_field_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same variant"
							);
						}
						unnamed_field_attrs.skip_content = Some(SkipContent::Always);
					} else if meta.path.is_ident("skip_content_if") {
						if unnamed_field_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same variant"
							);
						}
						if let Ok(value) = meta.value()
							&& let Ok(lit_str) = value.parse::<LitStr>()
						{
							unnamed_field_attrs.skip_content = Some(SkipContent::If(
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

		unnamed_field_attrs
	}
}
