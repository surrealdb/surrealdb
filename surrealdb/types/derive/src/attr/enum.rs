use heck::{
	ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase,
	ToUpperCamelCase,
};
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
	/// Serde-style case transformation for variant names.
	pub rename_all: Option<Casing>,
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
					} else if meta.path.is_ident("rename_all") {
						if enum_attrs.casing.is_some() {
							panic!(
								"Cannot combine rename_all with the legacy uppercase/lowercase attribute on the same enum; remove the legacy attribute"
							);
						}
						let Ok(value) = meta.value() else {
							panic!(
								"rename_all requires a value, e.g. #[surreal(rename_all = \"snake_case\")]"
							);
						};
						let Ok(lit_str) = value.parse::<LitStr>() else {
							panic!("Failed to parse rename_all attribute");
						};
						let Some(casing) = Casing::from_rename_all(&lit_str.value()) else {
							panic!("Invalid rename_all value: {}", lit_str.value());
						};
						enum_attrs.rename_all = Some(casing);
					} else if meta.path.is_ident("uppercase") {
						if enum_attrs.rename_all.is_some() {
							panic!(
								"Cannot combine rename_all with the legacy uppercase/lowercase attribute on the same enum; remove the legacy attribute"
							);
						}
						enum_attrs.casing = Some(Casing::Uppercase);
					} else if meta.path.is_ident("lowercase") {
						if enum_attrs.rename_all.is_some() {
							panic!(
								"Cannot combine rename_all with the legacy uppercase/lowercase attribute on the same enum; remove the legacy attribute"
							);
						}
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
		let s = crate::unraw(variant);
		match self.rename_all.or(self.casing) {
			Some(casing) => casing.apply(&s),
			None => s,
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub enum Casing {
	Uppercase,
	Lowercase,
	PascalCase,
	CamelCase,
	SnakeCase,
	ScreamingSnake,
	KebabCase,
	ScreamingKebab,
}

impl Casing {
	pub fn from_rename_all(value: &str) -> Option<Self> {
		match value {
			"lowercase" => Some(Self::Lowercase),
			"UPPERCASE" => Some(Self::Uppercase),
			"PascalCase" => Some(Self::PascalCase),
			"camelCase" => Some(Self::CamelCase),
			"snake_case" => Some(Self::SnakeCase),
			"SCREAMING_SNAKE_CASE" => Some(Self::ScreamingSnake),
			"kebab-case" => Some(Self::KebabCase),
			"SCREAMING-KEBAB-CASE" => Some(Self::ScreamingKebab),
			_ => None,
		}
	}

	pub fn apply(&self, value: &str) -> String {
		match self {
			Self::Uppercase => value.to_uppercase(),
			Self::Lowercase => value.to_lowercase(),
			Self::PascalCase => value.to_upper_camel_case(),
			Self::CamelCase => value.to_lower_camel_case(),
			Self::SnakeCase => value.to_snake_case(),
			Self::ScreamingSnake => value.to_shouty_snake_case(),
			Self::KebabCase => value.to_kebab_case(),
			Self::ScreamingKebab => value.to_shouty_kebab_case(),
		}
	}
}
