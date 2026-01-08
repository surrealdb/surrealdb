use proc_macro::TokenStream;
use quote::quote;
use syn::{Generics, Ident};

use crate::{CratePath, Enum};

pub fn impl_enum(
	name: &Ident,
	generics: &Generics,
	r#enum: Enum,
	crate_path: &CratePath,
) -> TokenStream {
	let into_value = r#enum.into_value(&r#enum.attrs, crate_path);
	let from_value = r#enum.from_value(&name.to_string(), &r#enum.attrs, crate_path);
	let is_value = r#enum.is_value(&r#enum.attrs, crate_path);
	let kind_of = r#enum.kind_of(&r#enum.attrs, crate_path);

	let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

	let value_ty = crate_path.value();
	let kind_ty = crate_path.kind();
	let anyhow_result = crate_path.anyhow_result();

	quote! {
		impl #impl_generics SurrealValue for #name #type_generics #where_clause {
			fn into_value(self) -> #value_ty {
				#into_value
			}

			fn from_value(value: #value_ty) -> #anyhow_result<Self> {
				#from_value
			}

			fn is_value(value: &#value_ty) -> bool {
				#is_value
			}

			fn kind_of() -> #kind_ty {
				#kind_of
			}
		}
	}
	.into()
}
