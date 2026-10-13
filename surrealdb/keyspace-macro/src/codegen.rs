//! Code generation: turns the checked [`Model`] into the key types.
//!
//! Names in the output are deliberately unhygienic (`Span::call_site`), so they
//! resolve against the invocation site. The invoking module must therefore have
//! these in scope:
//!
//! - `KVKey`, `KVKeyDecode` — the key traits.
//! - `KVSubspace`, `KVRange` — the bound traits, and `TypedRange`/`RawRange`, the ranges they
//!   build.
//! - `KVValue` — the value trait, for the `KVKey::Value` binding.
//! - `Key`, `KeyRange` — the byte and byte-range types.
//! - `KeyError` — the key error enum, with `Unencodable` and `Corrupted` variants.
//!
//! Everything else is referenced through an absolute path (`::storekey`,
//! `::anyhow`, `::std`), so the expansion does not depend on any other import.
//!
//! Literal bytes are written one `u8` at a time rather than as a slice. That is
//! what `storekey` does for a `u8` field, and matching it byte for byte is what
//! keeps the generated encoders compatible with data written before this macro
//! existed.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::model::{Emit, Format, Model, ResolvedEnum, Role, TypeDef, Width, extends_bytes};

pub fn generate(model: &Model) -> TokenStream {
	let enums = model.enums.iter().map(enum_def);
	let types = model.types.iter().map(|def| type_def(model, def));
	let builders = builders(model);
	let context_diagnostic = context_diagnostic();

	quote! {
		#context_diagnostic
		#(#enums)*
		#(#types)*
		#builders
	}
}

/// A trait whose only job is to attach a readable message to the one thing the
/// macro cannot check: whether a value type decodes without help from its key.
fn context_diagnostic() -> TokenStream {
	quote! {
		/// Value types that decode without any data from their key.
		///
		/// Generated `value_context` bodies require this bound, so a value whose
		/// decode needs key-derived context fails to compile with a message
		/// naming the fix rather than a bare associated-type mismatch.
		#[diagnostic::on_unimplemented(
			message = "`{Self}` needs data from its key in order to decode",
			label = "value type used by a key with no `ctx`",
			note = "add `ctx = |k| ..` to this entry in `keyspace!` so the key supplies its \
					`KVValue::KeyContext`"
		)]
		pub trait UnitKeyContext: KVValue<KeyContext = ()> {}

		impl<T> UnitKeyContext for T where T: KVValue<KeyContext = ()> {}
	}
}

fn enum_def(def: &ResolvedEnum) -> TokenStream {
	let ResolvedEnum {
		attrs,
		ident,
		repr,
		variants,
	} = def;
	let passthrough = attrs.all();
	let names = variants.iter().map(|(n, _)| n);
	let discriminants = variants.iter().map(|(_, d)| d);
	let encode_arms = variants.iter().map(|(name, disc)| quote!(Self::#name => #disc));
	let decode_arms = variants.iter().map(|(name, disc)| quote!(#disc => Self::#name));

	quote! {
		#(#passthrough)*
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		#[repr(#repr)]
		pub enum #ident {
			#(#names = #discriminants,)*
		}

		impl<__F> ::storekey::Encode<__F> for #ident {
			fn encode<W>(&self, w: &mut ::storekey::Writer<W>) -> Result<(), ::storekey::EncodeError>
			where
				W: ::std::io::Write,
			{
				let raw: #repr = match self { #(#encode_arms,)* };
				::storekey::Encode::<__F>::encode(&raw, w)
			}
		}

		impl<'de, __F> ::storekey::BorrowDecode<'de, __F> for #ident {
			fn borrow_decode(
				r: &mut ::storekey::BorrowReader<'de>,
			) -> Result<Self, ::storekey::DecodeError> {
				let raw: #repr = ::storekey::BorrowDecode::<__F>::borrow_decode(r)?;
				Ok(match raw {
					#(#decode_arms,)*
					_ => return Err(::storekey::DecodeError::InvalidFormat),
				})
			}
		}
	}
}

fn type_def(model: &Model, def: &TypeDef) -> TokenStream {
	let name = &def.name;
	let attrs = def.attrs.all();
	let derives = &def.derives;
	let lifetime = def.lifetime_toks();
	let route_doc = format!(
		" Route: `{}`\n\n Generated from the `{}` entry of the keyspace schema.",
		def.route, def.source
	);

	let fields = def.fields.iter().map(|f| {
		let fname = &f.name;
		let fty = &f.ty;
		quote!(pub #fname: #fty,)
	});

	let struct_def = quote! {
		#[doc = #route_doc]
		#(#attrs)*
		#[derive(#(#derives),*)]
		pub struct #name #lifetime {
			#(#fields)*
		}
	};

	let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
	let encode = encode_impl(def);
	let decode = decode_impl(def);
	let ctor = constructor(def);
	let into_owned = into_owned(def);
	let key_impls = key_impls(def);
	let range_impls = range_impls(model, def);
	let extension_bounds = extension_bounds(model, def);

	quote! {
		#struct_def
		#(#cfgs)* #encode
		#(#cfgs)* #decode
		#(#cfgs)* #ctor
		#(#cfgs)* #into_owned
		#key_impls
		#range_impls
		#extension_bounds
	}
}

/// Bounds over a key and the keys that extend it.
///
/// A stored key whose bytes another key extends needs an upper bound past the
/// whole run. `next` would give the successor of this key alone and so exclude
/// every extension; appending `0xff` is not an alternative, because `0xff` is an
/// ordinary byte in this encoding and `bytes ++ 0xff ++ 0x00` sorts after it.
fn extension_bounds(model: &Model, def: &TypeDef) -> TokenStream {
	// Bounds already get these from `range_impls`.
	if !def.extended_by_keys || !def.is_decodable() || def.also_range {
		return TokenStream::new();
	}
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let impl_lt = def.lifetime.then(|| quote!(<'a>));
	let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
	// The run covers this key and its extensions, so it is single-typed only when
	// they all store the same thing. The bound traits are what construct a range, so
	// the key implements them too — its region is the family it heads.
	let bound_traits = bound_traits(model, def);
	let RangeWrapper {
		ty: range_ty,
		wrap,
		..
	} = range_wrapper(model, def);

	quote! {
		#bound_traits

		#(#cfgs)*
		impl #impl_lt #name #lifetime {
			/// The first key ordering after this key and every key that extends it.
			///
			/// This is the exclusive upper bound for a scan meant to cover one key
			/// together with its extensions.
			pub fn skip_extensions(&self) -> ::anyhow::Result<Key<'static>> {
				Ok(KVKey::encode_key(self)?.next_neighbour_expect())
			}

			/// The range covering this key and every key that extends it.
			pub fn range_subtree(&self) -> ::anyhow::Result<#range_ty> {
				let start = KVKey::encode_key(self)?;
				let end = start.clone().next_neighbour_expect();
				let range = KeyRange { start, end };
				Ok(#wrap)
			}
		}
	}
}

/// How this type's ranges are wrapped: with the value type its subtree stores, or
/// as an untyped region.
struct RangeWrapper {
	/// The return type of the range-producing methods.
	ty: TokenStream,
	/// Wraps a `range` local into that type.
	wrap: TokenStream,
	/// The children-only range, built by the trait.
	own: TokenStream,
}

fn range_wrapper(model: &Model, def: &TypeDef) -> RangeWrapper {
	match subtree_value(model, def) {
		Some(_) => RangeWrapper {
			ty: quote!(TypedRange<<Self as KVRange>::Value>),
			wrap: quote!(KVRange::typed(self, range)),
			own: quote!(KVRange::typed_range(self)),
		},
		None => RangeWrapper {
			ty: quote!(RawRange),
			wrap: quote!(KVSubspace::raw(self, range)),
			own: quote!(KVSubspace::raw_range(self)),
		},
	}
}

/// `KVSubspace`, and `KVRange` when the subtree holds one value type.
///
/// These are the only constructors of a range, which is what keeps a hand-built
/// byte range from reaching the datastore.
fn bound_traits(model: &Model, def: &TypeDef) -> TokenStream {
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
	let fmt = concrete_format(def);

	let value_impl = subtree_value(model, def).map(|value| {
		quote! {
			#(#cfgs)*
			impl #lifetime KVRange for #name #lifetime {
				type Value = #value;
			}
		}
	});

	quote! {
		#(#cfgs)*
		impl #lifetime KVSubspace for #name #lifetime {
			fn encode_bound(&self) -> ::anyhow::Result<Key<'static>> {
				let bytes = ::storekey::encode_vec_format::<#fmt, _>(self)
					.map_err(|_| KeyError::Unencodable)?;
				Ok(Key::from(bytes))
			}
		}

		#value_impl
	}
}

/// Generic parameters and where-clause fragments for a `storekey` impl under this
/// type's format.
fn format_generics(def: &TypeDef) -> (TokenStream, TokenStream) {
	let lt = if def.lifetime {
		quote!('a,)
	} else {
		Default::default()
	};
	match &def.format {
		Format::Generic => (quote!(<#lt __F>), quote!(__F)),
		Format::Default => (quote!(<#lt>), quote!(())),
		Format::Pinned(t) => (quote!(<#lt>), quote!(#t)),
	}
}

/// The format to encode this type's own bytes under, from a context that has no
/// format parameter of its own — a `KVKey` or `KVRange` impl.
///
/// A format-generic type is instantiated at the default format. That is sound
/// precisely because it is generic: `impl<F> Encode<F>` only compiles when every
/// field encodes the same under every format, which is what R13 relies on when a
/// level root bounds keys pinned to another format.
fn concrete_format(def: &TypeDef) -> TokenStream {
	match &def.format {
		Format::Generic => quote!(()),
		Format::Default => quote!(()),
		Format::Pinned(t) => quote!(#t),
	}
}

fn encode_impl(def: &TypeDef) -> TokenStream {
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let (generics, fmt) = format_generics(def);

	let steps = def.emit.iter().map(|step| match step {
		Emit::Lit(bytes) => {
			let writes = bytes.iter().map(|b| quote!(::storekey::Encode::<#fmt>::encode(&#b, w)?;));
			quote!(#(#writes)*)
		}
		Emit::Field(i) => {
			let f = &def.fields[*i].name;
			quote!(::storekey::Encode::<#fmt>::encode(&self.#f, w)?;)
		}
		// A list writes each element behind `mark_terminator()` so an element that
		// starts with a low byte is escaped and cannot be mistaken for the
		// terminator that closes the list.
		Emit::ListTerminated(i) => {
			let f = &def.fields[*i].name;
			quote! {
				for element in self.#f.iter() {
					w.mark_terminator();
					::storekey::Encode::<#fmt>::encode(element, w)?;
				}
				w.write_terminator()?;
			}
		}
		// The open variant deliberately omits the terminator: the result is a
		// bound over every key whose list starts with these elements.
		Emit::ListOpen(i) => {
			let f = &def.fields[*i].name;
			quote! {
				for element in self.#f.iter() {
					w.mark_terminator();
					::storekey::Encode::<#fmt>::encode(element, w)?;
				}
			}
		}
	});

	quote! {
		impl #generics ::storekey::Encode<#fmt> for #name #lifetime {
			fn encode<W>(&self, w: &mut ::storekey::Writer<W>) -> Result<(), ::storekey::EncodeError>
			where
				W: ::std::io::Write,
			{
				#(#steps)*
				Ok(())
			}
		}
	}
}

fn decode_impl(def: &TypeDef) -> TokenStream {
	let name = &def.name;
	let (_, fmt) = format_generics(def);
	let generics = match &def.format {
		Format::Generic => quote!(<'de, __F>),
		_ => quote!(<'de>),
	};
	// A borrowed type decodes from the very bytes it borrows.
	let self_lt = def.lifetime.then(|| quote!(<'de>));

	let steps = def.emit.iter().map(|step| match step {
		Emit::Lit(bytes) => {
			let checks = bytes.iter().map(|b| {
				quote! {
					{
						let actual: u8 = ::storekey::BorrowDecode::<#fmt>::borrow_decode(r)?;
						if actual != #b {
							return Err(::storekey::DecodeError::InvalidFormat);
						}
					}
				}
			});
			quote!(#(#checks)*)
		}
		Emit::Field(i) => {
			let f = &def.fields[*i].name;
			quote!(let #f = ::storekey::BorrowDecode::<#fmt>::borrow_decode(r)?;)
		}
		Emit::ListTerminated(i) => {
			let f = &def.fields[*i].name;
			let elem = def.fields[*i].elem.clone().unwrap_or_else(|| quote!(_));
			quote! {
				let #f = {
					let mut items: ::std::vec::Vec<#elem> = ::std::vec::Vec::new();
					while !r.read_terminal()? {
						items.push(::storekey::BorrowDecode::<#fmt>::borrow_decode(r)?);
					}
					::std::borrow::Cow::Owned(items)
				};
			}
		}
		// An open list has no terminator, so it is a bound rather than a key and
		// is never decoded.
		Emit::ListOpen(i) => {
			let f = &def.fields[*i].name;
			quote!(let #f = ::std::borrow::Cow::Owned(::std::vec::Vec::new());)
		}
	});

	let field_names = def.fields.iter().map(|f| &f.name);

	quote! {
		impl #generics ::storekey::BorrowDecode<'de, #fmt> for #name #self_lt {
			fn borrow_decode(
				r: &mut ::storekey::BorrowReader<'de>,
			) -> Result<Self, ::storekey::DecodeError> {
				#(#steps)*
				Ok(Self { #(#field_names,)* })
			}
		}
	}
}

fn constructor(def: &TypeDef) -> TokenStream {
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let params = def.fields.iter().map(|f| {
		let fname = &f.name;
		let fty = &f.ty;
		quote!(#fname: #fty)
	});
	let names: Vec<_> = def.fields.iter().map(|f| &f.name).collect();
	let doc = format!(" Builds `{}` from every field of its route, in encode order.", def.name);

	quote! {
		impl #lifetime #name #lifetime {
			#[doc = #doc]
			#[allow(clippy::too_many_arguments)]
			pub fn new(#(#params),*) -> Self {
				Self { #(#names,)* }
			}
		}
	}
}

/// Lifts a key decoded from borrowed bytes into one that owns them.
///
/// A key read from a scan borrows the bytes it was decoded from, so it cannot
/// outlive the batch that lent them. Code that reads a region and then acts on
/// what it found — a compactor collecting the deltas it will delete, say — needs
/// the key to survive that far, and without this the only thing that does is the
/// undecoded bytes, which is the typing the schema exists to supply.
///
/// Emitted only for types that borrow; for the rest the key already owns
/// everything and `'static` is the only lifetime it has.
fn into_owned(def: &TypeDef) -> TokenStream {
	if !def.lifetime {
		return TokenStream::new();
	}
	let name = &def.name;
	let fields = def.fields.iter().map(|f| {
		let fname = &f.name;
		// Every borrowed field type resolves through a `Cow`, so the owned form is
		// the same `Cow` holding its own copy.
		if f.borrowed {
			quote!(#fname: ::std::borrow::Cow::Owned(self.#fname.into_owned()))
		} else {
			quote!(#fname: self.#fname)
		}
	});
	let doc =
		format!(" The same `{}`, owning every field it borrowed from the key bytes.", def.name);

	quote! {
		impl #name<'_> {
			#[doc = #doc]
			pub fn into_owned(self) -> #name<'static> {
				#name { #(#fields,)* }
			}
		}
	}
}

fn key_impls(def: &TypeDef) -> TokenStream {
	let Some(value) = &def.value else {
		return TokenStream::new();
	};
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
	let impl_lt = def.lifetime.then(|| quote!(<'a>));
	let fmt = concrete_format(def);

	// `ctx` supplies the value decoder with data carried only by the key. Without
	// one the value must decode standalone, which the `UnitKeyContext` bound
	// reports in plain language if it does not hold.
	let context_body = match &def.ctx {
		Some(expr) => quote!( (#expr)(self) ),
		None => quote! {{
			fn assert_unit_context<T: UnitKeyContext>() {}
			assert_unit_context::<#value>();
		}},
	};

	let corrupt = format!("cannot decode {} key", def.name);
	let trailing_check = if def.ignore_trailing {
		// Declared forward-compatible: a longer key that starts with this layout
		// still decodes, and the extra bytes are left alone.
		TokenStream::new()
	} else {
		quote! {
			if !reader.is_empty() {
				return Err(KeyError::Corrupted(#corrupt).into());
			}
		}
	};
	let decode_body = quote! {
		let mut reader = ::storekey::BorrowReader::new(bytes);
		let decoded = <Self as ::storekey::BorrowDecode<'_, #fmt>>::borrow_decode(&mut reader)
			.map_err(|_| KeyError::Corrupted(#corrupt))?;
		#trailing_check
		Ok(decoded)
	};
	// A type with no borrowed field still decodes from borrowed bytes; it just
	// does not keep them.
	let decode_impl = if def.lifetime {
		quote! {
			#(#cfgs)*
			impl #impl_lt KVKeyDecode<'a> for #name<'a> {
				fn decode_key(bytes: &'a [u8]) -> ::anyhow::Result<Self> {
					#decode_body
				}
			}
		}
	} else {
		quote! {
			#(#cfgs)*
			impl KVKeyDecode<'_> for #name {
				fn decode_key(bytes: &[u8]) -> ::anyhow::Result<Self> {
					#decode_body
				}
			}
		}
	};

	quote! {
		#(#cfgs)*
		impl #lifetime KVKey for #name #lifetime {
			type Value = #value;

			fn encode_buffer(&self, buffer: &mut ::std::vec::Vec<u8>) -> ::anyhow::Result<()> {
				::storekey::encode_format::<#fmt, _, _>(buffer, self)
					.map_err(|_| KeyError::Unencodable)?;
				Ok(())
			}

			fn value_context(&self) -> <#value as KVValue>::KeyContext {
				#context_body
			}
		}

		#decode_impl
	}
}

/// Range and bound construction.
///
/// Every bound is derived with `Key::next` (append `0x00`, the immediate
/// successor) or `Key::next_neighbour` (increment the last non-`0xFF` byte, the
/// successor of every extension). Choosing between them is the whole game: a
/// bound that is a *complete* key needs `next`, while a bound that is only a
/// *prefix* of stored keys needs `next_neighbour`, or the scan silently drops
/// every key that extends it. R8 guarantees the successor exists, so none of
/// these can panic.
fn range_impls(model: &Model, def: &TypeDef) -> TokenStream {
	if !def.also_range {
		return TokenStream::new();
	}
	let name = &def.name;
	let lifetime = def.lifetime_toks();
	let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
	let impl_lt = def.lifetime.then(|| quote!(<'a>));
	let fmt = concrete_format(def);

	// A bound whose subtree holds exactly one value type hands back decoded values;
	// anything else names a region that can only be destroyed, counted or read raw.
	// Every range this type produces carries that answer with it, so a caller cannot
	// read a region as if it were single-typed.
	let RangeWrapper {
		ty: range_ty,
		wrap,
		own: range_body,
	} = range_wrapper(model, def);
	let bound_traits = bound_traits(model, def);

	let subtree = def.extended_by_keys || def.role != Role::Prefix;
	let subtree_method = subtree.then(|| {
		quote! {
			/// The range covering this key's own bytes and everything beneath
			/// them.
			///
			/// Use this when entries live at the bound itself as well as under it;
			/// [`Self::range`] starts one byte later and would skip the former.
			pub fn range_subtree(&self) -> ::anyhow::Result<#range_ty> {
				let start = KVSubspace::encode_bound(self)?;
				let end = start.clone().next_neighbour_expect();
				let range = KeyRange { start, end };
				Ok(#wrap)
			}

			/// The first key ordering after this key and all of its extensions.
			///
			/// This is the correct exclusive upper bound for a key whose bytes are
			/// a prefix of longer keys; appending `0xff` is not, because
			/// `0xff` is an ordinary byte in this encoding and
			/// `bytes ++ 0xff ++ 0x00` would sort after it.
			pub fn skip_extensions(&self) -> ::anyhow::Result<Key<'static>> {
				Ok(KVSubspace::encode_bound(self)?.next_neighbour_expect())
			}
		}
	});

	let borrows_next = next_field(model, def).is_some_and(|(ty, ..)| mentions_lifetime_a(&ty));
	let method_lt = (borrows_next && !def.lifetime).then(|| quote!(<'a>));

	let where_method = next_field(model, def).map(|(ty, extended, _)| {
		// An inclusive end (and an exclusive start) must clear every key that
		// extends the bound when the bound is only a prefix of stored keys.
		let past = if extended {
			quote!(next_neighbour_expect())
		} else {
			quote!(next())
		};
		quote! {
			/// The range of keys whose next field falls within `bounds`.
			///
			/// The successor is chosen from the layout: whether this bound is a
			/// complete key or a prefix of longer keys decides `Key::next` versus
			/// `Key::next_neighbour`, so an inclusive end covers every key that
			/// extends it.
			#[allow(clippy::ptr_arg)]
			pub fn range_where #method_lt (
				&self,
				bounds: impl ::std::ops::RangeBounds<#ty>,
			) -> ::anyhow::Result<#range_ty> {
				use ::std::ops::Bound;
				let start = match bounds.start_bound() {
					Bound::Unbounded => KVSubspace::encode_bound(self)?.next(),
					Bound::Included(v) => self.bound_with(v)?,
					Bound::Excluded(v) => self.bound_with(v)?.#past,
				};
				let end = match bounds.end_bound() {
					Bound::Unbounded => KVSubspace::encode_bound(self)?.next_neighbour_expect(),
					Bound::Included(v) => self.bound_with(v)?.#past,
					Bound::Excluded(v) => self.bound_with(v)?,
				};
				let range = KeyRange { start, end };
				Ok(#wrap)
			}
		}
	});

	let bound_with = next_field(model, def).map(|(ty, _, lead)| {
		// The bytes the entry's encoder writes between the cut and the field, in the
		// same one-`u8`-at-a-time form the encoder uses, so a bound built here lands
		// on the same boundary a stored key does.
		let lead_writes = lead.iter().map(
			|b| quote!(::storekey::Encode::<#fmt>::encode(&#b, &mut w).map_err(|_| KeyError::Unencodable)?;),
		);
		quote! {
			/// This bound's bytes, the tag bytes that follow them in the layout, and
			/// then one more field value.
			#[allow(clippy::ptr_arg)]
			fn bound_with #method_lt (&self, value: &#ty) -> ::anyhow::Result<Key<'static>> {
				let mut buffer = ::std::vec::Vec::new();
				{
					let mut w = ::storekey::Writer::new(&mut buffer);
					::storekey::Encode::<#fmt>::encode(self, &mut w)
						.map_err(|_| KeyError::Unencodable)?;
					#(#lead_writes)*
					::storekey::Encode::<#fmt>::encode(value, &mut w)
						.map_err(|_| KeyError::Unencodable)?;
				}
				Ok(Key::from(buffer))
			}
		}
	});

	quote! {
		#bound_traits

		#(#cfgs)*
		impl #impl_lt #name #lifetime {
			/// The range of every key strictly beneath these bytes.
			///
			/// The start is these bytes followed by `0x00`, so the bound itself is
			/// excluded; the end is the successor of the bound's last non-`0xFF`
			/// byte. Guaranteed to exist by the schema check, so this cannot fail
			/// on the bound arithmetic.
			pub fn range(&self) -> ::anyhow::Result<#range_ty> {
				#range_body
			}

			#subtree_method
			#where_method
			#bound_with
		}
	}
}

/// Whether a resolved type mentions the generated struct lifetime, i.e. borrows.
fn mentions_lifetime_a(tokens: &TokenStream) -> bool {
	use proc_macro2::TokenTree;
	fn scan(tokens: TokenStream) -> bool {
		let mut iter = tokens.into_iter().peekable();
		while let Some(token) = iter.next() {
			match token {
				TokenTree::Group(g) if scan(g.stream()) => {
					return true;
				}
				TokenTree::Punct(p) if p.as_char() == '\'' => {
					if matches!(iter.peek(), Some(TokenTree::Ident(id)) if id == "a") {
						return true;
					}
				}
				_ => {}
			}
		}
		false
	}
	scan(tokens.clone())
}

/// The one value type stored beneath this bound, if there is exactly one and a
/// range can decode it.
///
/// This is what decides whether a bound is a `KVRange` — scannable, handing back
/// decoded values — or only a `KVSubspace`, which can be destroyed and counted but
/// not read. Four things rule a bound out:
///
/// - two or more value types beneath it, so a scan could not say what it returned;
/// - none at all, which is a region reserved for keys not yet declared;
/// - a covered entry that declares `ctx`, whose value needs data only its own key carries, so
///   nothing derived from a range can decode it;
/// - a waiver naming the declaration, which licenses another declaration to write inside its bytes.
///   The value type would be a promise the layout contradicts, so the region stays byte-only until
///   the overlap is resolved.
///
/// `cfg` is ignored, as everywhere else in the checker: a bound that is
/// single-typed only while a feature is off would otherwise change trait with the
/// build.
fn subtree_value(model: &Model, def: &TypeDef) -> Option<TokenStream> {
	if model.waivers.iter().any(|w| w.from == def.source || w.to == def.source) {
		return None;
	}
	let mut found: Option<(String, TokenStream)> = None;
	for other in model.types.iter().filter(|t| t.is_decodable()) {
		let covered =
			extends_bytes(&other.syms, &def.syms) || (def.is_decodable() && other.syms == def.syms);
		if !covered {
			continue;
		}
		if other.ctx.is_some() {
			return None;
		}
		let value = other.value.as_ref()?;
		let rendered = crate::model::render_type(value);
		match &found {
			Some((first, _)) if *first != rendered => return None,
			Some(_) => {}
			None => found = Some((rendered, quote!(#value))),
		}
	}
	found.map(|(_, value)| value)
}

/// What a bounded range over this prefix can bound on: the next field the
/// entry's encoder writes after the point the bound stops at, the literal bytes
/// it writes in between, and whether stored keys continue past that field.
///
/// The literals matter because a bound is `encode(self) ++ lead ++
/// encode(value)`: a tag written between the cut and the field belongs to the
/// bound, and omitting it would place the bound outside the run of keys it is
/// meant to delimit. `None` when there is nothing to bound on, either because
/// the layout continues with a list rather than a single value or because the
/// bound itself stops part-way through one.
fn next_field(model: &Model, def: &TypeDef) -> Option<(TokenStream, bool, Vec<u8>)> {
	if def.role != Role::Prefix {
		return None;
	}
	let entry = model
		.types
		.iter()
		.find(|t| t.role != Role::Prefix && t.source == def.source && t.name != def.name)?;
	// A bound stopping inside a list bounds the list itself: what follows in the
	// layout is another element, not a field.
	if matches!(def.emit.last(), Some(Emit::ListOpen(_))) {
		return None;
	}
	let mut lead = Vec::new();
	let mut rest = entry.emit.iter().skip(def.emit.len());
	let index = loop {
		match rest.next()? {
			Emit::Lit(bytes) => lead.extend_from_slice(bytes),
			Emit::Field(i) => break *i,
			Emit::ListTerminated(_) | Emit::ListOpen(_) => return None,
		}
	};
	let field = entry.fields.get(index)?;
	debug_assert_ne!(field.width, Width::List, "a list field is emitted as a list step");
	// An inclusive bound on the field has to clear everything that can follow it:
	// the rest of this entry's own layout, or a key declared as extending it.
	let extended = rest.next().is_some() || entry.extended_by_keys;
	Some((field.ty.clone(), extended, lead))
}

/// Parent-to-child constructors, so a child key is only ever named through the
/// level that identifies its parents.
fn builders(model: &Model) -> TokenStream {
	let mut per_root: std::collections::BTreeMap<String, Vec<TokenStream>> =
		std::collections::BTreeMap::new();

	for def in &model.types {
		let Some(root) = &def.parent_root else {
			continue;
		};
		if def.name == *root {
			continue;
		}
		let Some(root_def) = model.types.iter().find(|t| t.name == *root) else {
			continue;
		};

		let method = &def.builder;
		let target = &def.name;
		let target_lt = def.lifetime_toks();
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		// A borrowing child hanging off a root that owns all of its own fields
		// needs the lifetime introduced on the method: the impl block has none to
		// lend it.
		let method_lt = (def.lifetime && !root_def.lifetime).then(|| quote!(<'a>));

		// The child's own fields are arguments; the inherited ones come from the
		// root the method is called on.
		let params = def.own_fields().map(|f| {
			let fname = &f.name;
			let fty = &f.ty;
			quote!(#fname: #fty)
		});
		let inherited = def
			.fields
			.iter()
			.filter(|f| f.inherited)
			.map(|f| {
				let fname = &f.name;
				// Root fields are cloned rather than moved so a root can build many
				// children. A field the root does not carry has nowhere to come from;
				// R14 rejects that schema, and this keeps the expansion from quietly
				// substituting a default if the rule is ever weakened, because a
				// defaulted `ns` or `tb` is a well-formed key addressing another
				// tenant's subspace.
				if root_def.fields.iter().any(|rf| rf.name == f.name) {
					quote!(#fname: self.#fname.clone())
				} else {
					let msg = format!(
						"`{}` inherits `{fname}`, which `{}` does not carry",
						def.source, root_def.source
					);
					quote!(#fname: ::std::compile_error!(#msg))
				}
			})
			.collect::<Vec<_>>();
		let own = def.own_fields().map(|f| {
			let fname = &f.name;
			quote!(#fname)
		});
		let doc = format!(" Names `{}` beneath this subtree. Route: `{}`.", def.name, def.route);

		per_root.entry(root.to_string()).or_default().push(quote! {
			#(#cfgs)*
			#[doc = #doc]
			#[allow(clippy::too_many_arguments)]
			pub fn #method #method_lt (&self, #(#params),*) -> #target #target_lt {
				#target { #(#inherited,)* #(#own,)* }
			}
		});
	}

	let blocks = per_root.into_iter().filter_map(|(root, methods)| {
		let root_def = model.types.iter().find(|t| t.name == root)?;
		let name = &root_def.name;
		let lifetime = root_def.lifetime_toks();
		let impl_lt = root_def.lifetime.then(|| quote!(<'a>));
		let cfgs: Vec<_> = root_def.attrs.cfgs.iter().collect();
		Some(quote! {
			#(#cfgs)*
			impl #impl_lt #name #lifetime {
				#(#methods)*
			}
		})
	});

	quote!(#(#blocks)*)
}

/// The fieldless mirror of every stored key, replacing a hand-maintained
/// classification enum.
pub fn key_kind(model: &Model) -> TokenStream {
	let stored: Vec<&TypeDef> = model.types.iter().filter(|t| t.is_decodable()).collect();
	let variants = stored.iter().map(|t| {
		let v = format_ident!("{}", crate::model::camel(&t.source));
		let cfgs: Vec<_> = t.attrs.cfgs.iter().collect();
		let doc = format!(" `{}`", t.route);
		quote!(#(#cfgs)* #[doc = #doc] #v,)
	});
	let arms = stored.iter().map(|t| {
		let v = format_ident!("{}", crate::model::camel(&t.source));
		let route = &t.route;
		let cfgs: Vec<_> = t.attrs.cfgs.iter().collect();
		quote!(#(#cfgs)* Self::#v => #route,)
	});

	quote! {
		/// Every kind of key the keyspace stores.
		#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
		pub enum KeyKind {
			#(#variants)*
		}

		impl KeyKind {
			/// The key's route in the keyspace map notation.
			pub fn route(self) -> &'static str {
				match self {
					#(#arms)*
				}
			}
		}
	}
}
