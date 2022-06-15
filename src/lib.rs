use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Store)]
pub fn store(input: TokenStream) -> TokenStream {
	// Parse the token stream
	let input = parse_macro_input!(input as DeriveInput);
	// Fetch the struct name
	let name = &input.ident;
	// Generate the output
	let output = quote! {

		impl From<#name> for Vec<u8> {
			fn from(v: #name) -> Vec<u8> {
				crate::sql::serde::beg_internal_serialization();
				let v = msgpack::to_vec(&v).unwrap_or_default();
				crate::sql::serde::end_internal_serialization();
				v
			}
		}

		impl From<&#name> for Vec<u8> {
			fn from(v: &#name) -> Vec<u8> {
				crate::sql::serde::beg_internal_serialization();
				let v = msgpack::to_vec(&v).unwrap_or_default();
				crate::sql::serde::end_internal_serialization();
				v
			}
		}

		impl From<Vec<u8>> for #name {
			fn from(v: Vec<u8>) -> Self {
				msgpack::from_slice::<Self>(&v).unwrap()
			}
		}

		impl From<&Vec<u8>> for #name {
			fn from(v: &Vec<u8>) -> Self {
				msgpack::from_slice::<Self>(&v).unwrap()
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
	// Generate the output
	let output = quote! {

		impl From<#name> for Vec<u8> {
			fn from(v: #name) -> Vec<u8> {
				v.encode().unwrap_or_default()
			}
		}

		impl From<&#name> for Vec<u8> {
			fn from(v: &#name) -> Vec<u8> {
				v.encode().unwrap_or_default()
			}
		}

		impl From<Vec<u8>> for #name {
			fn from(v: Vec<u8>) -> Self {
				Self::decode(&v).unwrap()
			}
		}

		impl From<&Vec<u8>> for #name {
			fn from(v: &Vec<u8>) -> Self {
				Self::decode(v).unwrap()
			}
		}

		impl #name {

			pub fn encode(&self) -> Result<Vec<u8>, crate::err::Error> {
				crate::sql::serde::beg_internal_serialization();
				let v = storekey::serialize(self);
				crate::sql::serde::end_internal_serialization();
				Ok(v?)
			}

			pub fn decode(v: &[u8]) -> Result<#name, crate::err::Error> {
				let v = storekey::deserialize(v);
				Ok(v?)
			}

		}

	};

	output.into()
}
