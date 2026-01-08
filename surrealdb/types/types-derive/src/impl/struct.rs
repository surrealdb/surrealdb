use proc_macro::TokenStream;
use quote::quote;
use syn::{Generics, Ident};

use crate::{CratePath, Fields, Strategy, With};

pub fn impl_struct(
	name: &Ident,
	generics: &Generics,
	fields: Fields,
	crate_path: &CratePath,
) -> TokenStream {
	let strategy = Strategy::for_struct();
	let match_fields = fields.match_fields();
	let from_ok = quote!(Ok(Self #match_fields));

	let value_ty = crate_path.value();
	let kind_ty = crate_path.kind();
	let conversion_error_ty = crate_path.conversion_error();

	let into_value = fields.into_value(&strategy, crate_path);
	let from_value = match fields.from_value(&name.to_string(), &strategy, from_ok, crate_path) {
		With::Map(x) => quote! {
			if let #value_ty::Object(mut map) = value {
				#x
			} else {
				let err = #conversion_error_ty::from_value(
					#kind_ty::Object,
					&value
				);
				Err(err.into())
			}
		},
		With::Arr(x) => quote! {
			if let #value_ty::Array(mut arr) = value {
				#x
			} else {
				let err = #conversion_error_ty::from_value(
					#kind_ty::Array(Box::new(#kind_ty::Any), None),
					&value
				);
				Err(err.into())
			}
		},
		With::String(x) => quote! {
			if let #value_ty::String(string) = value {
				#x
			} else {
				let err = #conversion_error_ty::from_value(
					#kind_ty::String,
					&value
				);
				Err(err.into())
			}
		},
		With::Value(x) => x,
	};

	let is_value = match fields.is_value(&strategy, crate_path) {
		With::Map(x) => quote! {
			if let #value_ty::Object(map) = value {
				#x
			}

			false
		},
		With::Arr(x) => quote! {
			if let #value_ty::Array(arr) = value {
				#x
			}

			false
		},
		With::String(x) => quote! {
			if let #value_ty::String(string) = value {
				#x
			}

			false
		},
		With::Value(x) => x,
	};
	let kind_of = fields.kind_of(&strategy, crate_path);

	let let_fields = if fields.has_fields() {
		quote!( let Self #match_fields = self; )
	} else {
		quote!()
	};

	let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

	let value_ty = crate_path.value();
	let kind_ty = crate_path.kind();
	let anyhow_result = crate_path.anyhow_result();

	quote! {
		impl #impl_generics SurrealValue for #name #type_generics #where_clause {
			fn into_value(self) -> #value_ty {
				#let_fields
				#into_value
			}

			fn from_value(value: #value_ty) -> #anyhow_result<Self> {
				#from_value
			}

			fn is_value(value: &#value_ty) -> bool {
				#is_value;

				false
			}

			fn kind_of() -> #kind_ty {
				#kind_of
			}
		}
	}
	.into()
}
