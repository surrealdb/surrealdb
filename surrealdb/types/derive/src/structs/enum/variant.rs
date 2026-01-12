use syn::Ident;

use crate::Fields;

pub struct EnumVariant {
	pub ident: Ident,
	pub fields: Fields,
}

impl EnumVariant {
	pub fn parse(variant: &syn::Variant) -> Self {
		EnumVariant {
			ident: variant.ident.clone(),
			fields: Fields::parse(&variant.fields, &variant.attrs),
		}
	}
}
