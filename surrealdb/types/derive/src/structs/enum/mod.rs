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

	#[allow(clippy::wrong_self_convention)]
	pub fn into_value(&self, attrs: &EnumAttributes, crate_path: &CratePath) -> TokenStream2 {
		let variants = self
			.variants
			.iter()
			.map(|variant| {
				let ident = &variant.ident;
				let fields = variant.fields.match_fields();
				let into_value = variant
					.fields
					.into_value(&Strategy::for_enum(&variant.ident, attrs), crate_path);

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
			let strategy = Strategy::for_enum(&variant.ident, attrs);
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

		quote! {
			match value {
				#match_map
				#match_arr
				#match_string
				#match_value
			};

			Err(#error_no_variants_matched)
		}
	}

	pub fn is_value(&self, attrs: &EnumAttributes, crate_path: &CratePath) -> TokenStream2 {
		let value_ty = crate_path.value();

		let mut with_map = WithMap::new();

		for variant in &self.variants {
			let strategy = Strategy::for_enum(&variant.ident, attrs);
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
				variant.fields.kind_of(&Strategy::for_enum(&variant.ident, attrs), crate_path)
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
