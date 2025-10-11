use syn::{Attribute, Ident, LitStr};

#[derive(Debug, Default)]
pub struct EnumAttributes {
	/// Whether the enum is untagged
	pub untagged: bool,
	/// Tag field name for internally/adjacently tagged enums
	pub tag: Option<String>,
	/// Content field name for adjacently tagged enums
	pub content: Option<String>,
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
						if let Ok(value) = meta.value() {
							if let Ok(lit_str) = value.parse::<LitStr>() {
								enum_attrs.tag = Some(lit_str.value());
							}
						}
					} else if meta.path.is_ident("content") {
						if let Ok(value) = meta.value() {
							if let Ok(lit_str) = value.parse::<LitStr>() {
								enum_attrs.content = Some(lit_str.value());
							}
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
