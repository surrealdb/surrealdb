use syn::Ident;

use crate::EnumAttributes;

pub enum Strategy {
	VariantKey {
		variant: String,
	},
	TagKey {
		tag: String,
		variant: String,
	},
	TagContentKeys {
		tag: String,
		variant: String,
		content: String,
		skip_content_if: Option<syn::Path>,
	},
	Value {
		variant: Option<String>,
	},
}

impl Strategy {
	pub fn for_struct() -> Self {
		Self::Value {
			variant: None,
		}
	}

	pub fn for_enum(variant: &Ident, attrs: &EnumAttributes) -> Self {
		let variant = attrs.variant_string(variant);

		if attrs.untagged {
			if attrs.tag.is_some() || attrs.content.is_some() {
				panic!("Untagged enum cannot have a tag or content");
			}

			return Self::Value {
				variant: Some(variant),
			};
		}

		if let Some(tag) = attrs.tag.as_ref() {
			if let Some(content) = attrs.content.as_ref() {
				return Self::TagContentKeys {
					tag: tag.to_string(),
					variant,
					content: content.to_string(),
					skip_content_if: attrs
						.skip_content_if
						.as_ref()
						.map(|s| syn::parse_str(s).expect("skip_content_if must be a valid path")),
				};
			}

			return Self::TagKey {
				tag: tag.to_string(),
				variant,
			};
		}

		if attrs.content.is_some() {
			panic!("Content key cannot be specified without a tag key");
		}

		Self::VariantKey {
			variant,
		}
	}
}
