use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{
	parse_macro_input, Expr, ExprLit, FnArg, GenericArgument, ItemFn, Lit, Meta, MetaNameValue,
	PatType, PathArguments, ReturnType, Type, TypePath,
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
            Meta::NameValue(MetaNameValue { path, value, .. }) if path.is_ident("name") => {
                if let Expr::Lit(ExprLit {
                    lit: Lit::Str(s), ..
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

	// Collect argument patterns and types
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

	// Compose tuple type and pattern (single args are passed directly)
	let (tuple_type, tuple_pattern) = if arg_types.is_empty() {
		(quote! { () }, quote! { () })
	} else if arg_types.len() == 1 {
		(quote! { (#(#arg_types),*,) }, quote! { (#(#arg_patterns),*,) })
	} else {
		(quote! { ( #(#arg_types),*, ) }, quote! { ( #(#arg_patterns),*, ) })
	};

	// Return type analysis
	let (result_type, is_result) = match &fn_sig.output {
		ReturnType::Default => (quote! { () }, false),
		ReturnType::Type(_, ty) => {
			// Check if the return type is Result<T, E>
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

	// Export function names
	let export_suffix = if is_default {
		String::new()
	} else {
		export_name_override.unwrap_or_else(|| fn_name.to_string())
	};

	let export_ident = format_ident!("__sr_fnc__{}", export_suffix);
	let args_ident = format_ident!("__sr_args__{}", export_suffix);
	let returns_ident = format_ident!("__sr_returns__{}", export_suffix);

	// DRY error handling pattern
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

	let expanded = if is_init {
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
			#fn_vis #fn_sig #fn_block

			#[unsafe(no_mangle)]
			pub extern "C" fn __sr_init() -> i32 {
				#init_call
			}
		}
	} else {
		let function_call = if is_result {
			quote! {
				#fn_name(#(#arg_patterns),*).map_err(|e| e.to_string())
			}
		} else {
			quote! {
				Ok(#fn_name(#(#arg_patterns),*))
			}
		};

		let transfer_call = if is_result {
			let expr = quote! { f.invoke_raw(&mut controller, ptr.into()) };
			let try_or_fail_result = try_or_fail(expr, "Function invocation");
			quote! {
				(*#try_or_fail_result)
				.try_into()
				.unwrap_or_else(|_| {
					eprintln!("Transfer error: pointer overflow");
					-1
				})
			}
		} else {
			quote! {
				match f.invoke_raw(&mut controller, ptr.into()) {
					Ok(result) => match (*result).try_into() {
						Ok(ptr) => ptr,
						Err(_) => {
							eprintln!("Transfer error: pointer overflow");
							-1
						}
					},
					Err(e) => {
						eprintln!("Function invocation error: {}", e);
						-1
					}
				}
			}
		};

		let args_call = if is_result {
			let expr = quote! { f.args_raw(&mut controller) };
			let try_or_fail_result = try_or_fail(expr, "Args");
			quote! {
				(*#try_or_fail_result)
				.try_into()
				.unwrap_or_else(|_| {
					eprintln!("Transfer error: pointer overflow");
					-1
				})
			}
		} else {
			quote! {
				match f.args_raw(&mut controller) {
					Ok(result) => match (*result).try_into() {
						Ok(ptr) => ptr,
						Err(_) => {
							eprintln!("Transfer error: pointer overflow");
							-1
						}
					},
					Err(e) => {
						eprintln!("Args error: {}", e);
						-1
					}
				}
			}
		};

		let returns_call = if is_result {
			let expr = quote! { f.returns_raw(&mut controller) };
			let try_or_fail_result = try_or_fail(expr, "Returns");
			quote! {
				(*#try_or_fail_result)
				.try_into()
				.unwrap_or_else(|_| {
					eprintln!("Transfer error: pointer overflow");
					-1
				})
			}
		} else {
			quote! {
				match f.returns_raw(&mut controller) {
					Ok(result) => match (*result).try_into() {
						Ok(ptr) => ptr,
						Err(_) => {
							eprintln!("Transfer error: pointer overflow");
							-1
						}
					},
					Err(e) => {
						eprintln!("Returns error: {}", e);
						-1
					}
				}
			}
		};

		quote! {
			#fn_vis #fn_sig #fn_block

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
	};

	TokenStream::from(expanded)
}
