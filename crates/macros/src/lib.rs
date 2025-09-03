use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, Item, parse_macro_input};
mod wasm_async_trait;

/// A proc macro that automatically applies the correct async_trait attribute
/// for WASM and non-WASM targets on both traits and implementations.
#[proc_macro_attribute]
pub fn wasm_async_trait(_args: TokenStream, input: TokenStream) -> TokenStream {
	let item = parse_macro_input!(input as Item);

	match item {
		Item::Trait(trait_item) => wasm_async_trait::handle_trait(trait_item),
		Item::Impl(impl_item) => wasm_async_trait::handle_impl(impl_item),
		_ => Error::new_spanned(
			&item,
			"wasm_async_trait can only be applied to traits or impl blocks",
		)
		.to_compile_error()
		.into(),
	}
}

#[proc_macro_derive(Store)]
pub fn store(input: TokenStream) -> TokenStream {
	// Parse the token stream
	let input = parse_macro_input!(input as DeriveInput);
	// Fetch the struct name
	let name = &input.ident;
	// Add derived implementations
	let output = quote! {

		impl TryFrom<#name> for Vec<u8> {
			type Error = crate::err::Error;
			fn try_from(v: #name) -> Result<Self, Self::Error> {
				Self::try_from(&v)
			}
		}

		impl TryFrom<Vec<u8>> for #name {
			type Error = crate::err::Error;
			fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
				Self::try_from(&v)
			}
		}

		impl TryFrom<&#name> for Vec<u8> {
			type Error = crate::err::Error;
			fn try_from(v: &#name) -> Result<Self, Self::Error> {
				let mut out:Vec<u8> = vec![];
				revision::Revisioned::serialize_revisioned(v, &mut out)?;
				Ok(out)
			}
		}

		impl TryFrom<&Vec<u8>> for #name {
			type Error = crate::err::Error;
			fn try_from(v: &Vec<u8>) -> Result<Self, Self::Error> {
				Ok(revision::Revisioned::deserialize_revisioned(&mut v.as_slice())?)
			}
		}

	};
	//
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
	let (lifetime, from_owned) = if let Some(lifetime_def) = generics.lifetimes().next() {
		let lifetime = &lifetime_def.lifetime;
		(quote! {#lifetime}, quote! {})
	} else {
		(
			quote! {},
			quote! {
				impl #impl_generics From<Vec<u8>> for #name #ty_generics #where_clause {
					fn from(v: Vec<u8>) -> Self {
						Self::decode(&v).unwrap()
					}
				}
			},
		)
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
				let v = storekey::serialize(self);
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
