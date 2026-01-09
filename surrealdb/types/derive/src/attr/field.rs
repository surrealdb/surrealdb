use syn::{Field, LitStr};

#[derive(Debug, Default)]
pub struct FieldAttributes {
	pub rename: Option<String>,
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
					}

					Ok(())
				})
				.ok();
			}
		}

		field_attrs
	}
}
