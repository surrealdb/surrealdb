use quote::{format_ident, quote, quote_spanned};

/// Encode a name for use in a Rust identifier. Escapes `_` and `::`
/// to avoid collisions:
///   - `_`  → `_u`  (u for underscore)
///   - `::` → `_s`  (s for scope separator)
fn encode_name(name: &str) -> String {
	name.replace('_', "_u").replace("::", "_s")
}

/// Generate the sentinel const for compile-time duplicate detection.
pub(crate) fn generate_sentinel(
	export_name: Option<&str>,
	gates: &[&syn::Attribute],
) -> proc_macro2::TokenStream {
	let sentinel_ident = format_ident!("{}", sentinel_const_name(export_name));
	quote! {
		#(#gates)*
		#[doc(hidden)]
		#[allow(dead_code, non_upper_case_globals)]
		const #sentinel_ident: () = ();
	}
}

/// Generate sentinel consts for each argument wire name within a function.
/// Uses `_a` as the argument delimiter, which is collision-free because
/// only `_u` and `_s` are produced by `encode_name`.
pub(crate) fn generate_arg_sentinels(
	export_name: Option<&str>,
	arg_wire_names: &[String],
	gates: &[&syn::Attribute],
) -> proc_macro2::TokenStream {
	let sentinels: Vec<_> = arg_wire_names
		.iter()
		.map(|arg_name| {
			let const_name = arg_sentinel_const_name(export_name, arg_name);
			let ident = format_ident!("{}", const_name);
			quote! {
				#(#gates)*
				#[doc(hidden)]
				#[allow(dead_code, non_upper_case_globals)]
				const #ident: () = ();
			}
		})
		.collect();
	quote! { #(#sentinels)* }
}

/// Generate the registration body (invoke/args/returns fns + inventory submit).
/// For init functions, generates the init wrapper + submit.
#[allow(clippy::too_many_arguments)]
pub(crate) fn generate_registration_body(
	fn_name: &syn::Ident,
	arg_patterns: &[syn::Pat],
	arg_wire_names: &[String],
	tuple_type: &proc_macro2::TokenStream,
	tuple_pattern: &proc_macro2::TokenStream,
	result_type: &proc_macro2::TokenStream,
	is_result: bool,
	is_async: bool,
	is_init: bool,
	export_name: Option<&str>,
	tx: Option<&crate::extract::TxParam>,
	comment: Option<&str>,
	// The author's `cfg`/`cfg_attr` attributes. Repeated on every item emitted
	// here, because they gate the function and everything here names it.
	gates: &[&syn::Attribute],
) -> proc_macro2::TokenStream {
	if is_init {
		// `init` is sync-lifted, and a sync-lifted export cannot block, so an
		// `async fn` here has no way to make progress. `handler.rs` rejects it
		// before codegen; these arms are the sync ones only.
		let init_call = if is_result {
			quote! { #fn_name().map_err(|e| e.to_string()) }
		} else {
			quote! { #fn_name(); Ok(()) }
		};

		let init_ident = format_ident!("__sr_init_{}", fn_name);

		quote! {
			#(#gates)*
			fn #init_ident() -> Result<(), String> {
				#init_call
			}

			#(#gates)*
			surrealism::inventory::submit!(surrealism::SurrealismInit(#init_ident));
		}
	} else {
		let invoke_ident = format_ident!("__sr_invoke_{}", fn_name);
		let args_ident = format_ident!("__sr_args_{}", fn_name);
		let returns_ident = format_ident!("__sr_returns_{}", fn_name);

		// `tx_bind` and `call_args` prepend the transaction handle when the
		// function asked for one, so the user's parameter order is preserved.
		let declares_tx_expr = if tx.is_some() {
			quote! { true }
		} else {
			quote! { false }
		};

		let (tx_bind, call_args, writeable_expr) = match tx {
			Some(tx) => {
				let pattern = &tx.pattern;
				let ty = &tx.ty;
				(
					quote! {
						let #pattern: #ty = match __sr_tx {
							Some(handle) => unsafe { <#ty>::__from_borrow(handle) },
							None => return Err(
								"function requires a transaction but the host lent none"
									.to_string(),
							),
						};
					},
					quote! { #pattern, #(#arg_patterns),* },
					quote! { <#ty as ::surrealism::TxMode>::WRITEABLE },
				)
			}
			None => (quote! {}, quote! { #(#arg_patterns),* }, quote! { false }),
		};

		let function_call = if is_async && is_result {
			quote! { #fn_name(#call_args).await.map_err(|e| e.to_string()) }
		} else if is_async {
			quote! { Ok(#fn_name(#call_args).await) }
		} else if is_result {
			quote! { #fn_name(#call_args).map_err(|e| e.to_string()) }
		} else {
			quote! { Ok(#fn_name(#call_args)) }
		};

		let name_expr = match export_name {
			None => quote! { None },
			Some(s) => quote! { Some(#s) },
		};

		let comment_expr = match comment {
			None => quote! { None },
			Some(s) => quote! { Some(#s) },
		};

		// Spanned at the function name, so `line!()` reports the definition
		// rather than the attribute.
		let location_expr = quote_spanned! {fn_name.span()=>
			::core::concat!(::core::file!(), ":", ::core::line!())
		};

		let wire_name_strs = arg_wire_names.iter().map(|n| n.as_str()).collect::<Vec<_>>();

		let arg_sentinels = generate_arg_sentinels(export_name, arg_wire_names, gates);

		quote! {
			#arg_sentinels

			#(#gates)*
			fn #invoke_ident<'__sr>(
				__sr_tx: Option<&'__sr ::surrealism::bindings::surrealism::extension::host::Transaction>,
				raw_args: &'__sr [u8],
			) -> ::surrealism::registry::InvokeFuture<'__sr> {
				Box::pin(async move {
					use surrealism::types::args::Args;
					use surrealdb_types::SurrealValue;

					let values = surrealdb_types::decode_value_list(raw_args)
						.map_err(|e| e.to_string())?;
					let #tuple_pattern: #tuple_type =
						<#tuple_type as Args>::from_values(values)
							.map_err(|e| e.to_string())?;

					#tx_bind

					let result: Result<#result_type, String> = #function_call;
					let val = result?;
					let public_val = val.into_value();
					surrealdb_types::encode(&public_val).map_err(|e| e.to_string())
				})
			}

			#(#gates)*
			fn #args_ident() -> Result<Vec<u8>, String> {
				use surrealism::types::args::Args;
				let kinds = <#tuple_type as Args>::kinds();
				let names: &[&str] = &[#(#wire_name_strs),*];
				let arguments: Vec<(&str, surrealdb_types::Kind)> =
					names.iter().copied().zip(kinds).collect();
				surrealdb_types::encode_argument_list(&arguments).map_err(|e| e.to_string())
			}

			#(#gates)*
			fn #returns_ident() -> Result<Vec<u8>, String> {
				use surrealdb_types::SurrealValue;
				let kind = <#result_type as SurrealValue>::kind_of();
				surrealdb_types::encode_kind(&kind).map_err(|e| e.to_string())
			}

			#(#gates)*
			surrealism::inventory::submit!(surrealism::SurrealismEntry {
				name: #name_expr,
				comment: #comment_expr,
				location: #location_expr,
				invoke: #invoke_ident,
				args: #args_ident,
				returns: #returns_ident,
				writeable: #writeable_expr,
				declares_transaction: #declares_tx_expr,
			});
		}
	}
}

/// Build a unique sentinel const name for compile-time duplicate detection.
///
/// - Default export (`None`) -> `__sr_export_default`
/// - Named export (`Some(name)`) -> `__sr_export__` + encoded name
///
/// The name part is encoded so `_` and `::` in export names produce
/// valid, collision-free Rust identifiers:
///   - `_`  -> `_u`  (u for underscore)
///   - `::` -> `_s`  (s for scope separator)
///
/// `_` is escaped first so that a literal `_s` in a name becomes `_us`,
/// which cannot collide with `::` (which becomes `_s`).
fn sentinel_const_name(export_name: Option<&str>) -> String {
	match export_name {
		None => "__sr_export_default".to_string(),
		Some(name) => {
			let encoded = encode_name(name);
			format!("__sr_export__{encoded}")
		}
	}
}

/// Build a unique sentinel const name for compile-time duplicate argument
/// detection within a function.
///
/// Uses `_a` as the argument delimiter between the encoded function name
/// and the encoded argument name. This is collision-free because `_a`
/// never appears in `encode_name` output (only `_u` and `_s` do).
fn arg_sentinel_const_name(export_name: Option<&str>, arg_name: &str) -> String {
	let fn_part = match export_name {
		None => "default".to_string(),
		Some(name) => encode_name(name),
	};
	let arg_part = encode_name(arg_name);
	format!("__sr_export__{fn_part}_a{arg_part}")
}
