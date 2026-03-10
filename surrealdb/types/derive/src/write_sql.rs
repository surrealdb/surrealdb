use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Expr, LitStr, Token};

struct WriteSqlInput {
	f: Expr,
	fmt: Expr,
	format_string: LitStr,
	args: Vec<Expr>,
}

impl Parse for WriteSqlInput {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		// Parse: f, fmt, "format string", arg1, arg2, ...
		let f: Expr = input.parse()?;
		input.parse::<Token![,]>()?;

		let fmt: Expr = input.parse()?;
		input.parse::<Token![,]>()?;

		let format_string: LitStr = input.parse()?;

		let mut args = Vec::new();
		while !input.is_empty() {
			input.parse::<Token![,]>()?;
			if input.is_empty() {
				break;
			}
			args.push(input.parse()?);
		}

		Ok(WriteSqlInput {
			f,
			fmt,
			format_string,
			args,
		})
	}
}

enum Placeholder {
	Positional,
	Named(String),
}

struct FormatPart {
	literal: String,
	placeholder: Option<Placeholder>,
}

fn parse_format_string(format_str: &str) -> Result<Vec<FormatPart>, String> {
	let mut parts = Vec::new();
	let mut chars = format_str.chars().peekable();
	let mut current_literal = String::new();

	while let Some(ch) = chars.next() {
		if ch == '{' {
			if chars.peek() == Some(&'{') {
				// Escaped brace: {{
				chars.next();
				current_literal.push('{');
			} else {
				// Start of a placeholder
				let mut placeholder_content = String::new();
				let mut found_close = false;

				for inner_ch in chars.by_ref() {
					if inner_ch == '}' {
						found_close = true;
						break;
					} else if inner_ch == '{' {
						return Err("Nested braces are not supported in format strings".to_string());
					}
					placeholder_content.push(inner_ch);
				}

				if !found_close {
					return Err("Unclosed placeholder in format string".to_string());
				}

				// Determine if it's positional or named
				let placeholder = if placeholder_content.is_empty() {
					Placeholder::Positional
				} else {
					// Validate identifier
					if placeholder_content.chars().all(|c| c.is_alphanumeric() || c == '_')
						&& !placeholder_content.chars().next().unwrap().is_numeric()
					{
						Placeholder::Named(placeholder_content)
					} else {
						return Err(format!(
							"Invalid placeholder identifier: '{}'",
							placeholder_content
						));
					}
				};

				parts.push(FormatPart {
					literal: current_literal.clone(),
					placeholder: Some(placeholder),
				});
				current_literal.clear();
			}
		} else if ch == '}' {
			if chars.peek() == Some(&'}') {
				// Escaped brace: }}
				chars.next();
				current_literal.push('}');
			} else {
				return Err("Unmatched closing brace in format string".to_string());
			}
		} else {
			current_literal.push(ch);
		}
	}

	// Add remaining literal if any
	if !current_literal.is_empty() || parts.is_empty() {
		parts.push(FormatPart {
			literal: current_literal,
			placeholder: None,
		});
	}

	Ok(parts)
}

pub fn write_sql_impl(input: TokenStream) -> TokenStream {
	let input = syn::parse_macro_input!(input as WriteSqlInput);

	let format_str = input.format_string.value();
	let parts = match parse_format_string(&format_str) {
		Ok(parts) => parts,
		Err(e) => {
			return syn::Error::new_spanned(&input.format_string, e).to_compile_error().into();
		}
	};

	// Count positional placeholders
	let positional_count =
		parts.iter().filter(|p| matches!(p.placeholder, Some(Placeholder::Positional))).count();

	// Validate positional placeholder count
	if positional_count != input.args.len() {
		return syn::Error::new_spanned(
			&input.format_string,
			format!(
				"Expected {} positional arguments but got {}",
				positional_count,
				input.args.len()
			),
		)
		.to_compile_error()
		.into();
	}

	let f = &input.f;
	let fmt = &input.fmt;

	let mut statements = Vec::new();
	let mut positional_idx = 0;

	for part in parts {
		// Add literal if non-empty
		if !part.literal.is_empty() {
			let literal = &part.literal;
			statements.push(quote! {
				#f.push_str(#literal);
			});
		}

		// Add placeholder if present
		if let Some(placeholder) = part.placeholder {
			match placeholder {
				Placeholder::Positional => {
					let arg = &input.args[positional_idx];
					positional_idx += 1;
					statements.push(quote! {
						surrealdb_types::ToSql::fmt_sql(&#arg, #f, #fmt);
					});
				}
				Placeholder::Named(name) => {
					let ident = syn::Ident::new(&name, proc_macro2::Span::call_site());
					statements.push(quote! {
						surrealdb_types::ToSql::fmt_sql(&#ident, #f, #fmt);
					});
				}
			}
		}
	}

	let output = quote! {
		{
			#(#statements)*
		}
	};

	output.into()
}
