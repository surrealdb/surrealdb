use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
	Expr, ExprLit, FnArg, GenericArgument, ItemFn, Lit, Meta, MetaNameValue, PatType,
	PathArguments, ReturnType, Type, TypePath, parse_macro_input,
};

#[proc_macro_attribute]
pub fn surrealism(attr: TokenStream, item: TokenStream) -> TokenStream {
	let args = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
	let input_fn = parse_macro_input!(item as ItemFn);

	let mut is_default = false;
	let mut export_name_override: Option<String> = None;
	let mut is_init = false;

	for meta in args.iter() {
		match meta {
			Meta::NameValue(MetaNameValue {
				path,
				value,
				..
			}) if path.is_ident("name") => {
				if let Expr::Lit(ExprLit {
					lit: Lit::Str(s),
					..
				}) = value
				{
					let val = s.value();
					if !val.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
						panic!(
							"#[surrealism(name = \"...\")] must use only ASCII letters, digits, and underscores"
						);
					}
					export_name_override = Some(val);
				}
			}
			Meta::Path(path) if path.is_ident("default") => {
				is_default = true;
			}
			Meta::Path(path) if path.is_ident("init") => {
				is_init = true;
			}
			_ => panic!(
				"Unsupported attribute: expected #[surrealism], #[surrealism(default)], #[surrealism(init)], or #[surrealism(name = \"...\")]"
			),
		}
	}

	let fn_name = &input_fn.sig.ident;
	let fn_vis = &input_fn.vis;
	let fn_sig = &input_fn.sig;
	let fn_block = &input_fn.block;

	let mut arg_patterns = Vec::new();
	let mut arg_types = Vec::new();

	for arg in &fn_sig.inputs {
		match arg {
			FnArg::Typed(PatType {
				pat,
				ty,
				..
			}) => {
				arg_patterns.push(pat.clone());
				arg_types.push(ty);
			}
			FnArg::Receiver(_) => panic!("`self` is not supported in #[surrealism] functions"),
		}
	}

	let (tuple_type, tuple_pattern) = if arg_types.is_empty() {
		(quote! { () }, quote! { () })
	} else {
		(quote! { ( #(#arg_types),*, ) }, quote! { ( #(#arg_patterns),*, ) })
	};

	let (result_type, is_result) = match &fn_sig.output {
		ReturnType::Default => (quote! { () }, false),
		ReturnType::Type(_, ty) => {
			if let Type::Path(TypePath {
				path,
				..
			}) = &**ty
			{
				if let Some(last_segment) = path.segments.last() {
					if last_segment.ident == "Result" {
						if let PathArguments::AngleBracketed(args) = &last_segment.arguments {
							if let Some(GenericArgument::Type(inner_type)) = args.args.first() {
								(quote! { #inner_type }, true)
							} else {
								(quote! { #ty }, false)
							}
						} else {
							(quote! { #ty }, false)
						}
					} else {
						(quote! { #ty }, false)
					}
				} else {
					(quote! { #ty }, false)
				}
			} else {
				(quote! { #ty }, false)
			}
		}
	};

	let export_suffix = if is_default {
		String::new()
	} else {
		export_name_override.unwrap_or_else(|| fn_name.to_string())
	};

	let p1_exports = generate_p1_exports(
		fn_name,
		&arg_patterns,
		&tuple_type,
		&tuple_pattern,
		&result_type,
		is_result,
		is_init,
		&export_suffix,
	);

	let p2_exports = generate_p2_exports(
		fn_name,
		&arg_patterns,
		&tuple_type,
		&tuple_pattern,
		&result_type,
		is_result,
		is_init,
		&export_suffix,
	);

	let expanded = quote! {
		#fn_vis #fn_sig #fn_block

		#[cfg(not(feature = "p2"))]
		const _: () = { #p1_exports };

		#[cfg(feature = "p2")]
		#p2_exports
	};

	TokenStream::from(expanded)
}

#[allow(clippy::too_many_arguments)]
fn generate_p1_exports(
	fn_name: &syn::Ident,
	arg_patterns: &[Box<syn::Pat>],
	tuple_type: &proc_macro2::TokenStream,
	tuple_pattern: &proc_macro2::TokenStream,
	result_type: &proc_macro2::TokenStream,
	is_result: bool,
	is_init: bool,
	export_suffix: &str,
) -> proc_macro2::TokenStream {
	let export_ident = format_ident!("__sr_fnc__{}", export_suffix);
	let args_ident = format_ident!("__sr_args__{}", export_suffix);
	let returns_ident = format_ident!("__sr_returns__{}", export_suffix);

	let try_or_fail = |expr: proc_macro2::TokenStream, context: &str| {
		let context = syn::LitStr::new(context, proc_macro2::Span::call_site());
		quote! {
			match #expr {
				Ok(val) => val,
				Err(e) => {
					eprintln!(concat!(#context, " error: {}"), e);
					return -1;
				}
			}
		}
	};

	if is_init {
		let init_call = if is_result {
			let expr = quote! { #fn_name() };
			quote! {
				match #expr {
					Ok(()) => 0,
					Err(e) => {
						eprintln!("Init error: {}", e);
						-1
					}
				}
			}
		} else {
			quote! {
				#fn_name();
				0
			}
		};

		quote! {
			#[unsafe(no_mangle)]
			pub extern "C" fn __sr_init() -> i32 {
				#init_call
			}
		}
	} else {
		let function_call = if is_result {
			quote! { #fn_name(#(#arg_patterns),*).map_err(|e| e.to_string()) }
		} else {
			quote! { Ok(#fn_name(#(#arg_patterns),*)) }
		};

		let transfer_call = {
			let expr = quote! { f.invoke_raw(&mut controller, ptr.into()) };
			let try_or_fail_result = try_or_fail(expr, "Function invocation");
			quote! {
				match (*#try_or_fail_result).try_into() {
					Ok(ptr) => ptr,
					Err(_) => {
						eprintln!("Transfer error: pointer overflow");
						-1
					}
				}
			}
		};

		let args_call = {
			let expr = quote! { f.args_raw(&mut controller) };
			let try_or_fail_result = try_or_fail(expr, "Args");
			quote! {
				match (*#try_or_fail_result).try_into() {
					Ok(ptr) => ptr,
					Err(_) => {
						eprintln!("Transfer error: pointer overflow");
						-1
					}
				}
			}
		};

		let returns_call = {
			let expr = quote! { f.returns_raw(&mut controller) };
			let try_or_fail_result = try_or_fail(expr, "Returns");
			quote! {
				match (*#try_or_fail_result).try_into() {
					Ok(ptr) => ptr,
					Err(_) => {
						eprintln!("Transfer error: pointer overflow");
						-1
					}
				}
			}
		};

		quote! {
			#[unsafe(no_mangle)]
			pub extern "C" fn #export_ident(ptr: u32) -> i32 {
				use surrealism::types::transfer::Transfer;
				let mut controller = surrealism::Controller {};
				let f = surrealism::SurrealismFunction::<#tuple_type, #result_type, _>::from(
					|#tuple_pattern: #tuple_type| #function_call
				);
				#transfer_call
			}

			#[unsafe(no_mangle)]
			pub extern "C" fn #args_ident() -> i32 {
				use surrealism::types::transfer::Transfer;
				let mut controller = surrealism::Controller {};
				let f = surrealism::SurrealismFunction::<#tuple_type, #result_type, _>::from(
					|#tuple_pattern: #tuple_type| #function_call
				);
				#args_call
			}

			#[unsafe(no_mangle)]
			pub extern "C" fn #returns_ident() -> i32 {
				use surrealism::types::transfer::Transfer;
				let mut controller = surrealism::Controller {};
				let f = surrealism::SurrealismFunction::<#tuple_type, #result_type, _>::from(
					|#tuple_pattern: #tuple_type| #function_call
				);
				#returns_call
			}
		}
	}
}

#[allow(clippy::too_many_arguments)]
fn generate_p2_exports(
	fn_name: &syn::Ident,
	arg_patterns: &[Box<syn::Pat>],
	tuple_type: &proc_macro2::TokenStream,
	tuple_pattern: &proc_macro2::TokenStream,
	result_type: &proc_macro2::TokenStream,
	is_result: bool,
	is_init: bool,
	export_suffix: &str,
) -> proc_macro2::TokenStream {
	let safe_suffix = if export_suffix.is_empty() {
		"default"
	} else {
		export_suffix
	};
	let p2_handler_ident = format_ident!("__sr_p2_invoke_{}", safe_suffix);
	let p2_args_ident = format_ident!("__sr_p2_args_{}", safe_suffix);
	let p2_returns_ident = format_ident!("__sr_p2_returns_{}", safe_suffix);

	if is_init {
		let init_call = if is_result {
			quote! { #fn_name().map_err(|e| e.to_string()) }
		} else {
			quote! { #fn_name(); Ok(()) }
		};

		quote! {
			pub fn __sr_p2_init() -> Result<(), String> {
				#init_call
			}
		}
	} else {
		let function_call = if is_result {
			quote! { #fn_name(#(#arg_patterns),*).map_err(|e| e.to_string()) }
		} else {
			quote! { Ok(#fn_name(#(#arg_patterns),*)) }
		};

		quote! {
			pub fn #p2_handler_ident(args_bytes: &[u8]) -> Result<Vec<u8>, String> {
				use surrealism::types::serialize::{Serializable, Serialized};
				use surrealism::types::args::Args;
				use surrealdb_types::SurrealValue;

				let values = Vec::<surrealdb_types::Value>::deserialize(
					Serialized(args_bytes.to_vec().into())
				).map_err(|e| e.to_string())?;
				let #tuple_pattern: #tuple_type = <#tuple_type as Args>::from_values(values)
					.map_err(|e| e.to_string())?;

				let result: Result<#result_type, String> = #function_call;
				let val = result?;
				let public_val = val.into_value();
				public_val.serialize().map(|s| s.0.to_vec()).map_err(|e| e.to_string())
			}

			pub fn #p2_args_ident() -> Result<Vec<u8>, String> {
				use surrealism::types::serialize::Serializable;
				use surrealism::types::args::Args;
				let kinds = <#tuple_type as Args>::kinds();
				kinds.serialize().map(|s| s.0.to_vec()).map_err(|e| e.to_string())
			}

			pub fn #p2_returns_ident() -> Result<Vec<u8>, String> {
				use surrealism::types::serialize::Serializable;
				use surrealdb_types::SurrealValue;
				let kind = <#result_type as SurrealValue>::kind_of();
				kind.serialize().map(|s| s.0.to_vec()).map_err(|e| e.to_string())
			}
		}
	}
}
