use syn::Attribute;

#[derive(Debug, Default)]
pub struct NamedFieldsAttributes {
	pub default: bool,
}

impl NamedFieldsAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut named_field_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("default") {
						named_field_attrs.default = true;
					}

					Ok(())
				})
				.ok();
			}
		}

		named_field_attrs
	}
}
