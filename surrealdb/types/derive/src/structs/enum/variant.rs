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

	/// Returns true if this is a unit variant marked with `#[surreal(other)]`,
	/// indicating it should act as a catch-all fallback during deserialization.
	pub fn is_other(&self) -> bool {
		matches!(&self.fields, Fields::Unit(attrs) if attrs.other)
	}
}
