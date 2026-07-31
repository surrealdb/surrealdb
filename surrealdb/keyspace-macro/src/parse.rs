//! Syntax layer: turns the `keyspace!` token stream into an untyped AST.
//!
//! The AST mirrors the source one-to-one and keeps a [`Span`] on every literal,
//! field and cut marker. Spans cannot be recovered after this point, and every
//! diagnostic the checker emits points at one, so nothing here may discard one.
//!
//! Grammar (informal):
//!
//! ```text
//! keyspace   := prelude* item*
//! prelude    := "types" "{" type_decl* "}" | "enums" "{" enum_decl* "}"
//! type_decl  := ("fixed" "(" int ")" | "var") name_spec ("," name_spec)* ";"
//! name_spec  := ident ("=" type)?            // with `=`, ident is an alias for type
//! enum_decl  := ident ":" repr "{" ident "=" int ("," ident "=" int)* "}"
//! item       := attr* ident "=" source ("=>" type)? opts? (";" | "{" item* "}")
//! source     := "[" segment ("," segment)* "]"
//!             | ident "+" "[" segment ("," segment)* "]"   // extends another entry
//! segment    := str | byte_str                             // literal tag bytes
//!             | "raw" int                                  // one raw byte
//!             | ident ":" field_ty                         // a field
//!             | "@" ident?                                 // truncation point; `@open` cuts a
//!                                                          // list without its terminator
//! field_ty   := type | "[" type ".." "]"                   // plain field | terminated list
//! opts       := "(" opt ("," opt)* ")"
//! opt        := "also_range" | "format_generic" | "ignore_trailing"
//!             | "derive" "(" ("+"|"-") ident ("," ..)* ")"
//!             | "ctx" "=" expr
//! attr       := "#[cfg(..)]" | "#[format(ty)]" | doc comment
//!             | "#[waive(overlaps = ident, at = str, tracked = str)]"
//! ```
//!
//! An item nested inside a level inherits that level's segments; a top-level
//! item is rooted nowhere and encodes exactly what it declares.

use proc_macro2::{Span, TokenStream};
use syn::parse::{Parse, ParseStream};
use syn::{Attribute, Expr, Ident, LitInt, Token, Type, braced, bracketed, parenthesized, token};

/// A parsed `keyspace!` invocation.
pub struct Keyspace {
	pub types: Vec<TypeDecl>,
	pub enums: Vec<EnumDecl>,
	pub items: Vec<Item>,
}

/// One `fixed(N) A, B = Ty;` or `var A, B = Ty;` line of the `types` prelude.
pub struct TypeDecl {
	pub width: DeclWidth,
	/// Declared name, plus the type it expands to when it is an alias.
	pub names: Vec<(Ident, Option<Type>)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DeclWidth {
	/// Exactly `n` bytes, no terminator.
	Fixed(usize),
	/// Self-delimiting and variable length: the encoding says where the field
	/// ends, by escaping and terminating its bytes or by a leading discriminant.
	Var,
}

/// A key-embedded enum whose discriminants are pinned by the schema rather than
/// derived, because the stored bytes predate `storekey`'s `idx + 2` numbering.
pub struct EnumDecl {
	pub attrs: PassthroughAttrs,
	pub ident: Ident,
	pub repr: Ident,
	pub variants: Vec<(Ident, LitInt)>,
}

/// A level (has a `{ .. }` body) or an entry (terminated by `;`).
pub enum Item {
	Level(Level),
	Entry(Entry),
}

pub struct Level {
	pub attrs: ItemAttrs,
	pub ident: Ident,
	pub segments: Vec<Segment>,
	pub value: Option<Type>,
	pub opts: Opts,
	pub items: Vec<Item>,
}

pub struct Entry {
	pub attrs: ItemAttrs,
	pub ident: Ident,
	/// `entry = base + [..]`: this entry's bytes extend `base`'s byte-for-byte.
	pub extends: Option<Ident>,
	pub segments: Vec<Segment>,
	pub value: Option<Type>,
	pub opts: Opts,
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Segment {
	/// Literal tag bytes, written verbatim.
	Lit {
		bytes: Vec<u8>,
		span: Span,
	},
	/// A single raw byte. Distinct from `Lit` only so the schema can spell legacy
	/// magic bytes numerically.
	Raw {
		byte: u8,
		span: Span,
	},
	Field {
		name: Ident,
		ty: FieldTy,
	},
	/// A truncation point: generates a prefix type ending here.
	Cut {
		/// Explicit name; otherwise derived from the preceding field.
		name: Option<Ident>,
		/// Cut the preceding list field *without* its closing terminator, so the
		/// result bounds a partial list.
		open: bool,
		span: Span,
	},
}

#[derive(Clone)]
pub enum FieldTy {
	Plain(Type),
	/// `[T..]`: zero or more `T`, each written behind `mark_terminator()`, closed
	/// by a single terminator byte.
	List {
		elem: Type,
		#[allow(dead_code)]
		span: Span,
	},
}

#[derive(Default)]
pub struct Opts {
	/// The type is both a stored key and a scan bound.
	pub also_range: bool,
	/// Emit `impl<F> Encode<F>` so the type embeds in keys of any format.
	pub format_generic: bool,
	pub derive_add: Vec<Ident>,
	pub derive_remove: Vec<Ident>,
	/// Expression building `KVValue::KeyContext` from `&self`, for values whose
	/// decode needs data carried only by the key.
	pub ctx: Option<Expr>,
	/// Bytes after a decoded key are ignored rather than rejected, for a layout
	/// that other keys extend and that must keep decoding as they grow.
	pub ignore_trailing: bool,
}

/// Attributes reproduced verbatim on generated items.
#[derive(Default, Clone)]
pub struct PassthroughAttrs {
	pub docs: Vec<Attribute>,
	pub cfgs: Vec<Attribute>,
}

impl PassthroughAttrs {
	pub fn all(&self) -> impl Iterator<Item = &Attribute> {
		self.docs.iter().chain(self.cfgs.iter())
	}
}

pub struct Waive {
	pub overlaps: Ident,
	/// The literal bytes the licensed collision happens at. A waiver covers only
	/// divergences at these bytes, so a different collision between the same two
	/// declarations still reports.
	pub at: Vec<u8>,
	/// Why the collision is tolerated. Required: a waiver with no stated reason is
	/// the kind that outlives its cause.
	pub tracked: String,
	pub span: Span,
}

#[derive(Default)]
pub struct ItemAttrs {
	pub passthrough: PassthroughAttrs,
	/// `#[format(T)]`: pin this item's `storekey` format parameter.
	pub format: Option<Type>,
	/// `#[waive(overlaps = other)]`: grandfather a known ambiguity.
	pub waives: Vec<Waive>,
}

impl Parse for Keyspace {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let mut types = Vec::new();
		let mut enums = Vec::new();
		let mut items = Vec::new();

		while !input.is_empty() {
			if input.peek(Ident) && input.peek2(token::Brace) {
				let kw: Ident = input.fork().parse()?;
				if kw == "types" || kw == "enums" {
					let _: Ident = input.parse()?;
					let body;
					braced!(body in input);
					while !body.is_empty() {
						if kw == "types" {
							types.push(body.parse()?);
						} else {
							enums.push(body.parse()?);
						}
					}
					continue;
				}
			}
			items.push(input.parse()?);
		}

		Ok(Keyspace {
			types,
			enums,
			items,
		})
	}
}

impl Parse for TypeDecl {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let kw: Ident = input.parse()?;
		let width = if kw == "fixed" {
			let inner;
			parenthesized!(inner in input);
			let n: LitInt = inner.parse()?;
			DeclWidth::Fixed(n.base10_parse()?)
		} else if kw == "var" {
			DeclWidth::Var
		} else {
			return Err(syn::Error::new(
				kw.span(),
				"expected `fixed(N)` or `var` in the `types` prelude",
			));
		};

		let mut names = Vec::new();
		loop {
			let name: Ident = input.parse()?;
			let expansion = if input.peek(Token![=]) {
				let _: Token![=] = input.parse()?;
				Some(input.parse()?)
			} else {
				None
			};
			names.push((name, expansion));
			if input.peek(Token![,]) {
				let _: Token![,] = input.parse()?;
				continue;
			}
			break;
		}
		let _: Token![;] = input.parse()?;

		Ok(TypeDecl {
			width,
			names,
		})
	}
}

impl Parse for EnumDecl {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let attrs = parse_passthrough(&Attribute::parse_outer(input)?)?;
		let ident: Ident = input.parse()?;
		let _: Token![:] = input.parse()?;
		let repr: Ident = input.parse()?;

		let body;
		braced!(body in input);
		let mut variants = Vec::new();
		while !body.is_empty() {
			let name: Ident = body.parse()?;
			let _: Token![=] = body.parse()?;
			let disc: LitInt = body.parse()?;
			variants.push((name, disc));
			if body.peek(Token![,]) {
				let _: Token![,] = body.parse()?;
			}
		}

		Ok(EnumDecl {
			attrs,
			ident,
			repr,
			variants,
		})
	}
}

impl Parse for Item {
	fn parse(input: ParseStream) -> syn::Result<Self> {
		let attrs = parse_item_attrs(input)?;
		let ident: Ident = input.parse()?;
		let _: Token![=] = input.parse()?;

		let extends = if input.peek(Ident) {
			let base: Ident = input.parse()?;
			let _: Token![+] = input.parse()?;
			Some(base)
		} else {
			None
		};

		let seg_body;
		bracketed!(seg_body in input);
		let segments = parse_segments(&seg_body)?;

		let value = if input.peek(Token![=>]) {
			let _: Token![=>] = input.parse()?;
			Some(input.parse()?)
		} else {
			None
		};

		let opts = if input.peek(token::Paren) {
			let inner;
			parenthesized!(inner in input);
			parse_opts(&inner)?
		} else {
			Opts::default()
		};

		if input.peek(token::Brace) {
			if let Some(base) = extends {
				return Err(syn::Error::new(
					base.span(),
					"a level cannot extend another entry; `+` is only valid on entries",
				));
			}
			let body;
			braced!(body in input);
			let mut items = Vec::new();
			while !body.is_empty() {
				items.push(body.parse()?);
			}
			return Ok(Item::Level(Level {
				attrs,
				ident,
				segments,
				value,
				opts,
				items,
			}));
		}

		let _: Token![;] = input.parse()?;
		Ok(Item::Entry(Entry {
			attrs,
			ident,
			extends,
			segments,
			value,
			opts,
		}))
	}
}

fn parse_segments(input: ParseStream) -> syn::Result<Vec<Segment>> {
	let mut out = Vec::new();
	while !input.is_empty() {
		out.push(parse_segment(input)?);
		if input.peek(Token![,]) {
			let _: Token![,] = input.parse()?;
		}
	}
	Ok(out)
}

fn parse_segment(input: ParseStream) -> syn::Result<Segment> {
	if input.peek(Token![@]) {
		let at: Token![@] = input.parse()?;
		let (name, open) = if input.peek(Ident) {
			let id: Ident = input.parse()?;
			if id == "open" {
				(None, true)
			} else {
				(Some(id), false)
			}
		} else {
			(None, false)
		};
		return Ok(Segment::Cut {
			name,
			open,
			span: at.span,
		});
	}

	if input.peek(syn::LitStr) {
		let lit: syn::LitStr = input.parse()?;
		return Ok(Segment::Lit {
			bytes: lit.value().into_bytes(),
			span: lit.span(),
		});
	}

	if input.peek(syn::LitByteStr) {
		let lit: syn::LitByteStr = input.parse()?;
		return Ok(Segment::Lit {
			bytes: lit.value(),
			span: lit.span(),
		});
	}

	// `raw 0xNN`: an ident that is not followed by `:` .
	if input.peek(Ident) && !input.peek2(Token![:]) {
		let kw: Ident = input.parse()?;
		if kw != "raw" {
			return Err(syn::Error::new(
				kw.span(),
				"expected a literal, `raw <byte>`, `name: Type`, or `@`",
			));
		}
		let lit: LitInt = input.parse()?;
		let byte: u8 = lit.base10_parse().map_err(|_| {
			syn::Error::new(lit.span(), "`raw` takes a single byte value in `0..=255`")
		})?;
		return Ok(Segment::Raw {
			byte,
			span: lit.span(),
		});
	}

	let name: Ident = input.parse()?;
	let _: Token![:] = input.parse()?;
	let ty = if input.peek(token::Bracket) {
		let inner;
		let bracket = bracketed!(inner in input);
		let elem: Type = inner.parse()?;
		let _: Token![..] = inner.parse()?;
		FieldTy::List {
			elem,
			span: bracket.span.join(),
		}
	} else {
		FieldTy::Plain(input.parse()?)
	};

	Ok(Segment::Field {
		name,
		ty,
	})
}

fn parse_opts(input: ParseStream) -> syn::Result<Opts> {
	let mut opts = Opts::default();
	while !input.is_empty() {
		let kw: Ident = input.parse()?;
		if kw == "also_range" {
			opts.also_range = true;
		} else if kw == "format_generic" {
			opts.format_generic = true;
		} else if kw == "derive" {
			let inner;
			parenthesized!(inner in input);
			while !inner.is_empty() {
				if inner.peek(Token![+]) {
					let _: Token![+] = inner.parse()?;
					opts.derive_add.push(inner.parse()?);
				} else if inner.peek(Token![-]) {
					let _: Token![-] = inner.parse()?;
					opts.derive_remove.push(inner.parse()?);
				} else {
					return Err(syn::Error::new(inner.span(), "expected `+Derive` or `-Derive`"));
				}
				if inner.peek(Token![,]) {
					let _: Token![,] = inner.parse()?;
				}
			}
		} else if kw == "ctx" {
			let _: Token![=] = input.parse()?;
			opts.ctx = Some(input.parse()?);
		} else if kw == "ignore_trailing" {
			opts.ignore_trailing = true;
		} else {
			return Err(syn::Error::new(
				kw.span(),
				"unknown option; expected `also_range`, `format_generic`, `derive(..)`, \
				 `ctx = ..` or `ignore_trailing`",
			));
		}
		if input.peek(Token![,]) {
			let _: Token![,] = input.parse()?;
		}
	}
	Ok(opts)
}

fn parse_item_attrs(input: ParseStream) -> syn::Result<ItemAttrs> {
	let raw = Attribute::parse_outer(input)?;
	let mut out = ItemAttrs::default();
	let mut passthrough = Vec::new();

	for attr in raw {
		if attr.path().is_ident("format") {
			out.format = Some(attr.parse_args()?);
		} else if attr.path().is_ident("waive") {
			out.waives.push(parse_waive(&attr)?);
		} else {
			passthrough.push(attr);
		}
	}

	out.passthrough = parse_passthrough(&passthrough)?;
	Ok(out)
}

fn parse_waive(attr: &Attribute) -> syn::Result<Waive> {
	let mut overlaps = None;
	let mut at = None;
	let mut tracked = None;
	let span = attr.path().segments[0].ident.span();
	attr.parse_nested_meta(|meta| {
		if meta.path.is_ident("overlaps") {
			overlaps = Some(meta.value()?.parse::<Ident>()?);
			Ok(())
		} else if meta.path.is_ident("at") {
			let value = meta.value()?;
			at = Some(if value.peek(syn::LitByteStr) {
				value.parse::<syn::LitByteStr>()?.value()
			} else {
				value.parse::<syn::LitStr>()?.value().into_bytes()
			});
			Ok(())
		} else if meta.path.is_ident("tracked") {
			tracked = Some(meta.value()?.parse::<syn::LitStr>()?.value());
			Ok(())
		} else {
			Err(meta.error("expected `overlaps = <entry>`, `at = \"..\"` or `tracked = \"..\"`"))
		}
	})?;

	let usage = "`#[waive(overlaps = <entry>, at = \"<bytes>\", tracked = \"<why>\")]`";
	let (Some(overlaps), Some(at), Some(tracked)) = (overlaps, at, tracked) else {
		return Err(syn::Error::new(
			span,
			format!(
				"a waiver must name the other party, the bytes the collision happens at, and why \
				 it is tolerated: {usage}"
			),
		));
	};
	Ok(Waive {
		overlaps,
		at,
		tracked,
		span,
	})
}

fn parse_passthrough(attrs: &[Attribute]) -> syn::Result<PassthroughAttrs> {
	let mut out = PassthroughAttrs::default();
	for attr in attrs {
		if attr.path().is_ident("doc") {
			out.docs.push(attr.clone());
		} else if attr.path().is_ident("cfg") {
			out.cfgs.push(attr.clone());
		} else {
			return Err(syn::Error::new_spanned(
				attr,
				"unsupported attribute; expected `cfg`, `doc`, `format` or `waive`",
			));
		}
	}
	Ok(out)
}

/// Parses a whole invocation. Used by the proc-macro entry point and the
/// in-crate tests alike.
pub fn parse_keyspace(input: TokenStream) -> syn::Result<Keyspace> {
	syn::parse2(input)
}
