use proc_macro::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Store, attributes(format))]
pub fn store(input: TokenStream) -> TokenStream {
	// Parse the token stream
	let input = parse_macro_input!(input as DeriveInput);
	// Fetch the struct name
	let name = &input.ident;
	// Fetch the macro options
	let conf = input.attrs.iter().find(|a| a.path.is_ident("format")).map(|a| a.tokens.to_string());
	// Fetch the output format
	let func = match conf {
		Some(v) if v.as_str() == "(NamedCompact)" => format_ident!("to_vec_named_compact"),
		Some(v) if v.as_str() == "(Compact)" => format_ident!("to_vec_compact"),
		Some(v) if v.as_str() == "(Named)" => format_ident!("to_vec_named"),
		_ => format_ident!("to_vec"),
	};
	//
	let output = quote! {

		impl #name {
			pub fn to_vec(&self) -> Vec<u8> {
				self.into()
			}
		}

		impl From<Vec<u8>> for #name {
			fn from(v: Vec<u8>) -> Self {
				Self::from(&v)
			}
		}

		impl From<&Vec<u8>> for #name {
			fn from(v: &Vec<u8>) -> Self {
				bung::from_slice::<Self>(v).unwrap()
			}
		}

		impl From<#name> for Vec<u8> {
			fn from(v: #name) -> Vec<u8> {
				Self::from(&v)
			}
		}

		impl From<&#name> for Vec<u8> {
			fn from(v: &#name) -> Vec<u8> {
				crate::sql::serde::serialize_internal(|| {
					bung::#func(v).unwrap_or_default()
				})
			}
		}

	};

	output.into()
}

#[proc_macro_derive(Key)]
pub fn key(input: TokenStream) -> TokenStream {
	// Parse the token stream
	let input = parse_macro_input!(input as DeriveInput);
	// Fetch the struct name
	let name = &input.ident;
	// Compute the generics
	let generics = input.generics;
	let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
	assert!(generics.lifetimes().count() <= 1);
	let (lifetime, from_owned) =
		if let Some(lifetime_def) = generics.lifetimes().next() {
			let lifetime = &lifetime_def.lifetime;
			(quote! {#lifetime}, quote!{})
		} else {
			(quote! {}, quote!{
				impl #impl_generics From<Vec<u8>> for #name #ty_generics #where_clause {
					fn from(v: Vec<u8>) -> Self {
						Self::decode(&v).unwrap()
					}
				}
			})
		};

	// Generate the output
	let output = quote! {

		impl #impl_generics From<#name #ty_generics> for Vec<u8> #where_clause {
			fn from(v: #name #ty_generics) -> Vec<u8> {
				v.encode().unwrap_or_default()
			}
		}

		impl #impl_generics From<&#name #ty_generics> for Vec<u8> #where_clause {
			fn from(v: &#name #ty_generics) -> Vec<u8> {
				v.encode().unwrap_or_default()
			}
		}

		#from_owned

		impl #impl_generics From<&#lifetime Vec<u8>> for #name #ty_generics #where_clause {
			fn from(v: &#lifetime Vec<u8>) -> Self {
				Self::decode(v).unwrap()
			}
		}

		impl #impl_generics #name #ty_generics #where_clause {

			pub fn encode(&self) -> Result<Vec<u8>, crate::err::Error> {
				crate::sql::serde::beg_internal_serialization();
				let v = storekey::serialize(self);
				crate::sql::serde::end_internal_serialization();
				Ok(v?)
			}

			pub fn decode(v: &#lifetime[u8]) -> Result<Self, crate::err::Error> {
				let v = storekey::deserialize(v);
				Ok(v?)
			}

		}

	};

	output.into()
}
