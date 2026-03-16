mod variant;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::Attribute;
pub use variant::*;

use crate::{CratePath, EnumAttributes, Strategy, WithMap};

pub struct Enum {
	pub attrs: EnumAttributes,
	pub variants: Vec<EnumVariant>,
}

impl Enum {
	pub fn parse(data: &syn::DataEnum, attrs: &[Attribute]) -> Self {
		Enum {
			attrs: EnumAttributes::parse(attrs),
			variants: data.variants.iter().map(EnumVariant::parse).collect(),
		}
	}

	fn strategy_for_variant(variant: &EnumVariant, attrs: &EnumAttributes) -> Strategy {
		Strategy::for_enum(&variant.ident, attrs)
			.with_variant_skip_content(variant.fields.skip_content().cloned())
	}

	#[allow(clippy::wrong_self_convention)]
	pub fn into_value(&self, attrs: &EnumAttributes, crate_path: &CratePath) -> TokenStream2 {
		let variants = self
			.variants
			.iter()
			.map(|variant| {
				let ident = &variant.ident;
				let fields = variant.fields.match_fields();
				let strategy = Self::strategy_for_variant(variant, attrs);
				let into_value = variant.fields.into_value(&strategy, crate_path);

				quote! {
					Self::#ident #fields => {
						#into_value
					}
				}
			})
			.collect::<Vec<_>>();

		quote! {
			match self {
				#(#variants)*
			}
		}
	}

	#[allow(clippy::wrong_self_convention)]
	pub fn from_value(
		&self,
		name: &String,
		attrs: &EnumAttributes,
		crate_path: &CratePath,
	) -> TokenStream2 {
		let value_ty = crate_path.value();
		let error_no_variants_matched = crate_path
			.error_internal(quote! { format!("Failed to decode {}, no variants matched", #name) });

		let mut with_map = WithMap::new();

		for variant in &self.variants {
			let ident = &variant.ident;
			let fields = variant.fields.match_fields();
			let ok = quote!(return Ok(Self::#ident #fields));
			let strategy = Self::strategy_for_variant(variant, attrs);
			with_map.push(variant.fields.from_value(&ident.to_string(), &strategy, ok, crate_path));
		}

		let match_map = match with_map.wants_map() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::Object(mut map) => {
					#(#x)*
				}
			},
		};

		let match_arr = match with_map.wants_arr() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::Array(mut arr) => {
					#(#x)*
				}
			},
		};

		let match_string = match with_map.wants_string() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::String(string) => {
					#(#x)*
				}
			},
		};

		let match_value = match with_map.wants_value() {
			None => quote!(_ => {}),
			Some(x) => quote! {
				_ => {
					#(#x)*
				}
			},
		};

		// If a variant is marked with #[surreal(other)], use it as the fallback
		// instead of returning an error when no variants match. This enables
		// forward compatibility: unknown values on the wire fall back to the
		// designated variant rather than failing deserialization.
		let fallback = if let Some(other) = self.variants.iter().find(|v| v.is_other()) {
			let ident = &other.ident;
			quote! { Ok(Self::#ident) }
		} else {
			quote! { Err(#error_no_variants_matched) }
		};

		quote! {
			match value {
				#match_map
				#match_arr
				#match_string
				#match_value
			};

			#fallback
		}
	}

	pub fn is_value(&self, attrs: &EnumAttributes, crate_path: &CratePath) -> TokenStream2 {
		let value_ty = crate_path.value();

		let mut with_map = WithMap::new();

		for variant in &self.variants {
			let strategy = Self::strategy_for_variant(variant, attrs);
			with_map.push(variant.fields.is_value(&strategy, crate_path));
		}

		let match_map = match with_map.wants_map() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::Object(map) => {
					#(#x)*
				}
			},
		};

		let match_arr = match with_map.wants_arr() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::Array(arr) => {
					#(#x)*
				}
			},
		};

		let match_string = match with_map.wants_string() {
			None => quote!(),
			Some(x) => quote! {
				#value_ty::String(string) => {
					#(#x)*
				}
			},
		};

		let match_value = match with_map.wants_value() {
			None => quote!(_ => {}),
			Some(x) => quote! {
				_ => {
					#(#x)*
				}
			},
		};

		quote! {
			match value {
				#match_map
				#match_arr
				#match_string
				#match_value
			}

			false
		}
	}

	pub fn kind_of(&self, attrs: &EnumAttributes, crate_path: &CratePath) -> TokenStream2 {
		let kind_ty = crate_path.kind();

		let variants = self
			.variants
			.iter()
			.map(|variant| {
				let strategy = Self::strategy_for_variant(variant, attrs);
				variant.fields.kind_of(&strategy, crate_path)
			})
			.collect::<Vec<_>>();

		if variants.len() == 1 {
			variants[0].clone()
		} else {
			quote! {
				#kind_ty::Either(vec![#(#variants),*])
			}
		}
	}
}
