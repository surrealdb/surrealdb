use syn::{Attribute, Ident, LitStr};

use crate::SkipContent;

#[derive(Debug, Default)]
pub struct EnumAttributes {
	/// Whether the enum is untagged
	pub untagged: bool,
	/// Tag field name for internally/adjacently tagged enums
	pub tag: Option<String>,
	/// Content field name for adjacently tagged enums
	pub content: Option<String>,
	/// Enum-level content skipping. `Always` for `#[surreal(skip_content)]`,
	/// `If(path)` for `#[surreal(skip_content_if = "predicate")]`.
	pub skip_content: Option<SkipContent>,
	/// Whether to transform variant names to uppercase
	pub casing: Option<Casing>,
}

impl EnumAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut enum_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("untagged") {
						enum_attrs.untagged = true;
					} else if meta.path.is_ident("tag") {
						if let Ok(value) = meta.value()
							&& let Ok(lit_str) = value.parse::<LitStr>()
						{
							enum_attrs.tag = Some(lit_str.value());
						}
					} else if meta.path.is_ident("content") {
						if let Ok(value) = meta.value()
							&& let Ok(lit_str) = value.parse::<LitStr>()
						{
							enum_attrs.content = Some(lit_str.value());
						}
					} else if meta.path.is_ident("skip_content") {
						if enum_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same enum"
							);
						}
						enum_attrs.skip_content = Some(SkipContent::Always);
					} else if meta.path.is_ident("skip_content_if") {
						if enum_attrs.skip_content.is_some() {
							panic!(
								"Cannot use both skip_content and skip_content_if on the same enum"
							);
						}
						if let Ok(value) = meta.value()
							&& let Ok(lit_str) = value.parse::<LitStr>()
						{
							enum_attrs.skip_content = Some(SkipContent::If(
								syn::parse_str(&lit_str.value())
									.expect("skip_content_if must be a valid path"),
							));
						}
					} else if meta.path.is_ident("uppercase") {
						enum_attrs.casing = Some(Casing::Uppercase);
					} else if meta.path.is_ident("lowercase") {
						enum_attrs.casing = Some(Casing::Lowercase);
					}
					Ok(())
				})
				.ok();
			}
		}

		enum_attrs
	}

	pub fn variant_string(&self, variant: &Ident) -> String {
		match self.casing {
			Some(Casing::Uppercase) => variant.to_string().to_uppercase(),
			Some(Casing::Lowercase) => variant.to_string().to_lowercase(),
			None => variant.to_string(),
		}
	}
}

#[derive(Debug)]
pub enum Casing {
	Uppercase,
	Lowercase,
}
