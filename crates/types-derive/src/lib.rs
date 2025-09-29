mod attr;
use attr::*;

mod structs;
use structs::*;

mod r#impl;
use r#impl::*;
use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(SurrealValue, attributes(surreal))]
pub fn surreal_value(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	let name = &input.ident;
	let generics = &input.generics;

	match &input.data {
		syn::Data::Struct(data) => {
			let fields = Fields::parse(&data.fields, &input.attrs);
			impl_struct(name, generics, fields)
		}
		syn::Data::Enum(data) => {
			let r#enum = Enum::parse(data, &input.attrs);
			impl_enum(name, generics, r#enum)
		}
		syn::Data::Union(_) => panic!("SurrealValue cannot be derived for unions"),
	}
}
