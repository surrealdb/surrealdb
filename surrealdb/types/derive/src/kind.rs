use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::Parse;
use syn::{Ident, Token, parse_macro_input};

/// Represents the parsed kind syntax
#[derive(Debug, Clone)]
pub enum KindExpr {
	/// Simple identifier like `string`, `bool`
	Ident(Ident),
	/// Parameterized kind like `record<user>`, `array<string, 10>`
	Parameterized {
		name: Ident,
		params: Vec<KindParam>,
	},
	/// Union type like `string | bool`
	Union(Vec<KindExpr>),
	/// Literal value like `true`, `false`, `42`, `"hello"`
	Literal(syn::Lit),
	/// Kind:: prefix like `Kind::String`
	KindPrefix(syn::Path),
	/// Literal:: prefix like `Literal::Bool(true)`
	LiteralPrefix(syn::Expr),
	/// Parenthesized expression escape hatch like `(my_expr)`
	Parenthesized(syn::Expr),
	/// Object literal like `{ status: "OK", time: string }`
	Object(Vec<(String, KindExpr)>),
	/// Array literal like `[string, int, bool]`
	Array(Vec<KindExpr>),
}

/// Parameters inside angle brackets
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum KindParam {
	/// An identifier parameter
	Ident(Ident),
	/// A literal parameter (numbers, strings)
	Literal(syn::Lit),
	/// A nested kind expression
	Kind(KindExpr),
}

impl Parse for KindExpr {
	/// Parse a `KindExpr` from the input token stream.
	///
	/// This is the main parsing entry point that handles union types (`|` operator)
	/// and delegates to `parse_primary` for individual kind expressions.
	fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
		let mut expr = Self::parse_primary(input)?;

		// Check for union operator |
		while input.peek(Token![|]) {
			input.parse::<Token![|]>()?;
			let right = Self::parse_primary(input)?;

			expr = match expr {
				KindExpr::Union(mut variants) => {
					variants.push(right);
					KindExpr::Union(variants)
				}
				_ => KindExpr::Union(vec![expr, right]),
			};
		}

		Ok(expr)
	}
}

impl KindExpr {
	/// Parse a primary (non-union) kind expression.
	///
	/// Handles all non-union syntax including:
	/// - Array literals: `[string, int]`
	/// - Object literals: `{ key: value }`
	/// - Parenthesized expressions: `(expr)`
	/// - Literal values: `true`, `42`, `"hello"`
	/// - Identifiers with optional parameters: `string`, `array<string>`, `record<user | post>`
	/// - Path prefixes: `Kind::String`, `Literal::Bool(true)`
	fn parse_primary(input: syn::parse::ParseStream) -> syn::Result<Self> {
		// Check for array literals with square brackets
		if input.peek(syn::token::Bracket) {
			return Self::parse_array(input);
		}

		// Check for object literals with curly braces
		if input.peek(syn::token::Brace) {
			return Self::parse_object(input);
		}

		// Check for parenthesized expressions first
		if input.peek(syn::token::Paren) {
			let content;
			syn::parenthesized!(content in input);
			let expr: syn::Expr = content.parse()?;
			return Ok(KindExpr::Parenthesized(expr));
		}

		// Check for literals
		if input.peek(syn::LitBool)
			|| input.peek(syn::LitInt)
			|| input.peek(syn::LitFloat)
			|| input.peek(syn::LitStr)
		{
			let lit: syn::Lit = input.parse()?;
			return Ok(KindExpr::Literal(lit));
		}

		// Parse identifier or path
		let name: Ident = input.parse()?;
		let name_str = name.to_string();

		// Special handling for boolean literals as identifiers
		if name_str == "true" {
			return Ok(KindExpr::Literal(syn::Lit::Bool(syn::LitBool::new(true, name.span()))));
		} else if name_str == "false" {
			return Ok(KindExpr::Literal(syn::Lit::Bool(syn::LitBool::new(false, name.span()))));
		}

		// Check for :: (path separator)
		if input.peek(Token![::]) {
			input.parse::<Token![::]>()?;

			if name_str == "Kind" {
				// Parse Kind::Something - just store the remaining path after ::
				let path: syn::Path = input.parse()?;
				return Ok(KindExpr::KindPrefix(path));
			} else if name_str == "Literal" {
				// Parse Literal::Something
				let expr: syn::Expr = input.parse()?;
				return Ok(KindExpr::LiteralPrefix(expr));
			} else {
				return Err(input.error("Only Kind:: and Literal:: prefixes are supported"));
			}
		}

		// Check for angle brackets
		if input.peek(Token![<]) {
			input.parse::<Token![<]>()?;
			let params = Self::parse_params(input)?;
			input.parse::<Token![>]>()?;

			Ok(KindExpr::Parameterized {
				name,
				params,
			})
		} else {
			Ok(KindExpr::Ident(name))
		}
	}

	/// Parse an array literal like `[string, int, bool]`.
	///
	/// Array literals represent a fixed sequence of kind types and are converted to
	/// `Kind::Literal(KindLiteral::Array(...))`.
	fn parse_array(input: syn::parse::ParseStream) -> syn::Result<Self> {
		let content;
		syn::bracketed!(content in input);

		let mut elements = Vec::new();

		while !content.is_empty() {
			// Parse element expression
			let element_expr = Self::parse(&content)?;
			elements.push(element_expr);

			// Optional trailing comma
			if content.peek(Token![,]) {
				content.parse::<Token![,]>()?;
			}
		}

		Ok(KindExpr::Array(elements))
	}

	/// Parse an object literal like `{ status: string, time: datetime }`.
	///
	/// Object literals support both identifier keys (`status`) and string literal keys
	/// (`"user-id"`) for keys with special characters. Values can be any kind expression.
	/// The result is converted to `Kind::Literal(KindLiteral::Object(...))`.
	fn parse_object(input: syn::parse::ParseStream) -> syn::Result<Self> {
		let content;
		syn::braced!(content in input);

		let mut fields = Vec::new();

		while !content.is_empty() {
			// Parse key - can be identifier or string literal
			let key_str = if content.peek(syn::LitStr) {
				// String literal key like "user-id"
				let lit: syn::LitStr = content.parse()?;
				lit.value()
			} else if content.peek(Ident) {
				// Identifier key like status
				let ident: Ident = content.parse()?;
				ident.to_string()
			} else {
				return Err(content.error("Expected field name (identifier or string literal)"));
			};

			// Parse colon
			content.parse::<Token![:]>()?;

			// Parse value expression
			let value_expr = Self::parse(&content)?;

			fields.push((key_str, value_expr));

			// Optional trailing comma
			if content.peek(Token![,]) {
				content.parse::<Token![,]>()?;
			}
		}

		Ok(KindExpr::Object(fields))
	}

	/// Parse parameters inside angle brackets like `<string, 10>` or `<user | post>`.
	///
	/// Parameters can be:
	/// - Identifiers: `string`, `user`
	/// - Literals: `10`, `"value"`
	/// - Parenthesized expressions: `(T::kind_of())`
	/// - Nested kind expressions: `string | int`
	///
	/// Parameters are separated by commas or pipes (for union types).
	fn parse_params(input: syn::parse::ParseStream) -> syn::Result<Vec<KindParam>> {
		let mut params = Vec::new();

		while !input.peek(Token![>]) && !input.is_empty() {
			if input.peek(syn::token::Paren) {
				// Parse parenthesized expression as a parameter
				let kind_expr = Self::parse_primary(input)?;
				params.push(KindParam::Kind(kind_expr));
			} else if input.peek(syn::Lit) {
				// Parse literal (numbers, strings)
				let lit: syn::Lit = input.parse()?;
				params.push(KindParam::Literal(lit));
			} else if input.peek(Ident) {
				// Look ahead to see if this is a nested kind expression
				let fork = input.fork();
				let _: Ident = fork.parse()?;

				if fork.peek(Token![<]) || fork.peek(Token![|]) {
					// This is a nested kind expression
					let kind_expr = input.parse()?;
					params.push(KindParam::Kind(kind_expr));
				} else {
					// Simple identifier
					let ident: Ident = input.parse()?;
					params.push(KindParam::Ident(ident));
				}
			} else {
				return Err(
					input.error("Expected identifier, literal, or parenthesized expression")
				);
			}

			// Handle comma or pipe separator
			if input.peek(Token![,]) {
				input.parse::<Token![,]>()?;
			} else if input.peek(Token![|]) && !input.peek2(Token![>]) {
				// This | is not the end, continue parsing parameters
				input.parse::<Token![|]>()?;
			} else if !input.peek(Token![>]) {
				break;
			}
		}

		Ok(params)
	}
}

/// Main entry point for the `kind!` procedural macro.
///
/// This macro provides a convenient DSL for creating `Kind` values with support for:
/// - Basic types: `string`, `bool`, `int`, `array`, `record`, etc.
/// - Parameterized types: `array<string>`, `set<int, 5>`, `record<user | post>`
/// - Union types: `string | bool | int`
/// - Literals: `true`, `false`, `42`, `"hello"`
/// - Object literals: `{ status: string, "user-id": int }`
/// - Array literals: `[string, int, bool]`
/// - Escape hatches: `(expr)` for arbitrary expressions
/// - Prefixes: `Kind::String`, `Literal::Bool(true)`
///
/// # Examples
///
/// ```ignore
/// use surrealdb_types::kind;
///
/// // Basic types
/// let k = kind!(string);
/// let k = kind!(array<string>);
///
/// // Union types
/// let k = kind!(string | int | bool);
///
/// // Object literals
/// let k = kind!({ status: string, time: datetime });
///
/// // Dynamic kinds with escape hatch
/// let k = kind!(array<(T::kind_of())>);
/// ```
pub fn kind(input: TokenStream) -> TokenStream {
	let expr = parse_macro_input!(input as KindExpr);
	let tokens = generate_kind_code(&expr);
	TokenStream::from(tokens)
}

/// Generate Rust code for a parsed `KindExpr`.
///
/// Converts the parsed kind expression into tokens that construct the appropriate
/// `surrealdb_types::Kind` variant at compile time.
fn generate_kind_code(expr: &KindExpr) -> TokenStream2 {
	match expr {
		KindExpr::Ident(ident) => generate_simple_kind(ident),
		KindExpr::Parameterized {
			name,
			params,
		} => generate_parameterized_kind(name, params),
		KindExpr::Union(variants) => {
			let variant_codes: Vec<_> = variants.iter().map(generate_kind_code).collect();
			quote! {
				surrealdb_types::Kind::Either(vec![#(#variant_codes),*])
			}
		}
		KindExpr::Literal(lit) => generate_literal_kind(lit),
		KindExpr::KindPrefix(path) => {
			// Convert Kind::Something to surrealdb_types::Kind::Something
			quote! { surrealdb_types::Kind::#path }
		}
		KindExpr::LiteralPrefix(expr) => {
			quote! {
				surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::#expr)
			}
		}
		KindExpr::Parenthesized(expr) => {
			quote! { #expr }
		}
		KindExpr::Object(fields) => {
			let field_codes: Vec<_> = fields
				.iter()
				.map(|(key, value)| {
					let value_code = generate_kind_code(value);
					quote! { (#key.to_string(), #value_code) }
				})
				.collect();

			quote! {
				surrealdb_types::Kind::Literal(
					surrealdb_types::KindLiteral::Object(
						std::collections::BTreeMap::from([#(#field_codes),*])
					)
				)
			}
		}
		KindExpr::Array(elements) => {
			let element_codes: Vec<_> = elements.iter().map(generate_kind_code).collect();

			quote! {
				surrealdb_types::Kind::Literal(
					surrealdb_types::KindLiteral::Array(vec![#(#element_codes),*])
				)
			}
		}
	}
}

/// Generate code for literal kind values like `true`, `42`, `"hello"`.
///
/// Converts parsed literals into `Kind::Literal(KindLiteral::...)` expressions.
fn generate_literal_kind(lit: &syn::Lit) -> TokenStream2 {
	match lit {
		syn::Lit::Bool(lit_bool) => {
			let value = lit_bool.value;
			quote! {
				surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Bool(#value))
			}
		}
		syn::Lit::Int(lit_int) => {
			if let Ok(value) = lit_int.base10_parse::<i64>() {
				quote! {
					surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Integer(#value))
				}
			} else {
				quote! { compile_error!("Integer literal out of range") }
			}
		}
		syn::Lit::Float(lit_float) => {
			if let Ok(value) = lit_float.base10_parse::<f64>() {
				quote! {
					surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::Float(#value))
				}
			} else {
				quote! { compile_error!("Float literal out of range") }
			}
		}
		syn::Lit::Str(lit_str) => {
			let value = lit_str.value();
			quote! {
				surrealdb_types::Kind::Literal(surrealdb_types::KindLiteral::String(#value.to_string()))
			}
		}
		_ => quote! { compile_error!("Unsupported literal type") },
	}
}

/// Generate code for simple (non-parameterized) kind identifiers.
///
/// Maps simple identifiers like `string`, `bool`, `int` to their corresponding
/// `Kind` enum variants. For example, `string` becomes `Kind::String`.
fn generate_simple_kind(ident: &Ident) -> TokenStream2 {
	let name = ident.to_string();
	match name.as_str() {
		"any" => quote! { surrealdb_types::Kind::Any },
		"none" => quote! { surrealdb_types::Kind::None },
		"null" => quote! { surrealdb_types::Kind::Null },
		"bool" => quote! { surrealdb_types::Kind::Bool },
		"bytes" => quote! { surrealdb_types::Kind::Bytes },
		"datetime" => quote! { surrealdb_types::Kind::Datetime },
		"decimal" => quote! { surrealdb_types::Kind::Decimal },
		"duration" => quote! { surrealdb_types::Kind::Duration },
		"float" => quote! { surrealdb_types::Kind::Float },
		"int" => quote! { surrealdb_types::Kind::Int },
		"number" => quote! { surrealdb_types::Kind::Number },
		"object" => quote! { surrealdb_types::Kind::Object },
		"string" => quote! { surrealdb_types::Kind::String },
		"uuid" => quote! { surrealdb_types::Kind::Uuid },
		"regex" => quote! { surrealdb_types::Kind::Regex },
		"range" => quote! { surrealdb_types::Kind::Range },
		"table" => quote! { surrealdb_types::Kind::Table(vec![]) },
		"record" => quote! { surrealdb_types::Kind::Record(vec![]) },
		"geometry" => quote! { surrealdb_types::Kind::Geometry(vec![]) },
		"set" => quote! { surrealdb_types::Kind::Set(Box::new(surrealdb_types::Kind::Any), None) },
		"array" => {
			quote! { surrealdb_types::Kind::Array(Box::new(surrealdb_types::Kind::Any), None) }
		}
		"file" => quote! { surrealdb_types::Kind::File(vec![]) },
		"function" => quote! { surrealdb_types::Kind::Function(None, None) },
		_ => quote! { compile_error!(concat!("Unknown kind: ", #name)) },
	}
}

/// Generate code for parameterized kinds like `record<user>`, `array<string, 10>`.
///
/// Handles kinds that take parameters in angle brackets:
/// - `record<user | post>` - record tables
/// - `array<string, 10>` - array with element type and optional size
/// - `set<int, 5>` - set with element type and optional size
/// - `geometry<point | polygon>` - geometry types
/// - `file<bucket1 | bucket2>` - file buckets
fn generate_parameterized_kind(name: &Ident, params: &[KindParam]) -> TokenStream2 {
	let name_str = name.to_string();

	match name_str.as_str() {
		"table" => {
			let mut tables = Vec::new();
			for param in params {
				match param {
					KindParam::Ident(ident) => {
						tables.push(ident.to_string());
					}
					KindParam::Kind(KindExpr::Union(variants)) => {
						// Handle union inside table parameters: table<user | post>
						for variant in variants {
							if let KindExpr::Ident(ident) = variant {
								tables.push(ident.to_string());
							}
						}
					}
					_ => {} // Ignore other parameter types
				}
			}

			quote! {
				surrealdb_types::Kind::Table(vec![#(#tables.into()),*])
			}
		}
		"record" => {
			let mut tables = Vec::new();
			for param in params {
				match param {
					KindParam::Ident(ident) => {
						tables.push(ident.to_string());
					}
					KindParam::Kind(KindExpr::Union(variants)) => {
						// Handle union inside record parameters: record<user | post>
						for variant in variants {
							if let KindExpr::Ident(ident) = variant {
								tables.push(ident.to_string());
							}
						}
					}
					_ => {} // Ignore other parameter types
				}
			}

			quote! {
				surrealdb_types::Kind::Record(vec![#(#tables.into()),*])
			}
		}
		"array" => {
			if params.is_empty() {
				quote! { surrealdb_types::Kind::Array(Box::new(surrealdb_types::Kind::Any), None) }
			} else {
				let inner_kind = generate_param_code(&params[0]);
				let size = if params.len() > 1 {
					if let KindParam::Literal(syn::Lit::Int(int_lit)) = &params[1] {
						let val = int_lit.base10_parse::<u64>().unwrap_or(0);
						quote! { Some(#val) }
					} else {
						quote! { None }
					}
				} else {
					quote! { None }
				};

				quote! {
					surrealdb_types::Kind::Array(Box::new(#inner_kind), #size)
				}
			}
		}
		"set" => {
			if params.is_empty() {
				quote! { surrealdb_types::Kind::Set(Box::new(surrealdb_types::Kind::Any), None) }
			} else {
				let inner_kind = generate_param_code(&params[0]);
				let size = if params.len() > 1 {
					if let KindParam::Literal(syn::Lit::Int(int_lit)) = &params[1] {
						let val = int_lit.base10_parse::<u64>().unwrap_or(0);
						quote! { Some(#val) }
					} else {
						quote! { None }
					}
				} else {
					quote! { None }
				};

				quote! {
					surrealdb_types::Kind::Set(Box::new(#inner_kind), #size)
				}
			}
		}
		"geometry" => {
			let mut geom_kinds = Vec::new();
			for param in params {
				match param {
					KindParam::Ident(ident) => {
						let geom_name = ident.to_string();
						match geom_name.as_str() {
							"point" => {
								geom_kinds.push(quote! { surrealdb_types::GeometryKind::Point })
							}
							"line" => {
								geom_kinds.push(quote! { surrealdb_types::GeometryKind::Line })
							}
							"polygon" => {
								geom_kinds.push(quote! { surrealdb_types::GeometryKind::Polygon })
							}
							"multipoint" => geom_kinds
								.push(quote! { surrealdb_types::GeometryKind::MultiPoint }),
							"multiline" => {
								geom_kinds.push(quote! { surrealdb_types::GeometryKind::MultiLine })
							}
							"multipolygon" => geom_kinds
								.push(quote! { surrealdb_types::GeometryKind::MultiPolygon }),
							"collection" => geom_kinds
								.push(quote! { surrealdb_types::GeometryKind::Collection }),
							_ => {}
						}
					}
					KindParam::Kind(KindExpr::Union(variants)) => {
						// Handle union inside geometry parameters: geometry<point | polygon>
						for variant in variants {
							if let KindExpr::Ident(ident) = variant {
								let geom_name = ident.to_string();
								match geom_name.as_str() {
									"point" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::Point }),
									"line" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::Line }),
									"polygon" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::Polygon }),
									"multipoint" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::MultiPoint }),
									"multiline" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::MultiLine }),
									"multipolygon" => geom_kinds.push(
										quote! { surrealdb_types::GeometryKind::MultiPolygon },
									),
									"collection" => geom_kinds
										.push(quote! { surrealdb_types::GeometryKind::Collection }),
									_ => {}
								}
							}
						}
					}
					_ => {} // Ignore other parameter types
				}
			}

			quote! {
				surrealdb_types::Kind::Geometry(vec![#(#geom_kinds),*])
			}
		}
		"file" => {
			let mut buckets = Vec::new();
			for param in params {
				match param {
					KindParam::Ident(ident) => {
						buckets.push(ident.to_string());
					}
					KindParam::Kind(KindExpr::Union(variants)) => {
						// Handle union inside file parameters: file<images | videos>
						for variant in variants {
							if let KindExpr::Ident(ident) = variant {
								buckets.push(ident.to_string());
							}
						}
					}
					_ => {} // Ignore other parameter types
				}
			}

			quote! {
				surrealdb_types::Kind::File(vec![#(#buckets.to_string()),*])
			}
		}
		_ => quote! { compile_error!(concat!("Unknown parameterized kind: ", #name_str)) },
	}
}

/// Generate code for a single parameter value.
///
/// Parameters can be identifiers, literals, or nested kind expressions.
/// This function delegates to the appropriate generation function based on the parameter type.
fn generate_param_code(param: &KindParam) -> TokenStream2 {
	match param {
		KindParam::Ident(ident) => generate_simple_kind(ident),
		KindParam::Literal(lit) => quote! { #lit },
		KindParam::Kind(kind) => generate_kind_code(kind),
	}
}
