use syn::Ident;

use crate::{EnumAttributes, SkipContent};

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
		skip_content: Option<SkipContent>,
	},
	Value {
		variant: Option<String>,
	},
}

impl Strategy {
	/// If the variant has per-variant `skip_content` / `skip_content_if`, override
	/// the strategy's skip_content. Only valid for `TagContentKeys`.
	pub fn with_variant_skip_content(self, variant_skip: Option<SkipContent>) -> Self {
		let Some(skip) = variant_skip else {
			return self;
		};
		match self {
			Self::TagContentKeys {
				tag,
				variant,
				content,
				..
			} => Self::TagContentKeys {
				tag,
				variant,
				content,
				skip_content: Some(skip),
			},
			// TagKey already has no content field, so per-variant skip_content is a no-op
			s @ Self::TagKey {
				..
			} => s,
			_ => panic!(
				"#[surreal(skip_content)] / #[surreal(skip_content_if)] can only be used on variants of enums with a tag"
			),
		}
	}

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
				let skip_content = if attrs.skip_content {
					Some(SkipContent::Always)
				} else {
					attrs.skip_content_if.as_ref().map(|s| {
						SkipContent::If(
							syn::parse_str(s).expect("skip_content_if must be a valid path"),
						)
					})
				};
				return Self::TagContentKeys {
					tag: tag.to_string(),
					variant,
					content: content.to_string(),
					skip_content,
				};
			}

			// tag without content: TagKey. Enum-level skip_content is implicit
			// (TagKey never produces a content field).
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
