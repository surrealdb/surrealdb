use syn::Attribute;

#[derive(Debug, Default)]
pub struct UnnamedFieldsAttributes {
	pub tuple: bool,
}

impl UnnamedFieldsAttributes {
	pub fn parse(attrs: &[Attribute]) -> Self {
		let mut unnamed_field_attrs = Self::default();

		for attr in attrs {
			if attr.path().is_ident("surreal") {
				attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("tuple") {
						unnamed_field_attrs.tuple = true;
					}

					Ok(())
				})
				.ok();
			}
		}

		unnamed_field_attrs
	}
}
