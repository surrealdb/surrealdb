//! The reverse direction: raw bytes back to a typed key and a typed value.
//!
//! Because one invocation declares the whole keyspace, the generated decoder is
//! exhaustive by construction: there is no key it does not know about, and no
//! registration step that could be forgotten.
//!
//! Dispatch is a byte trie over each key's leading literal run. The trie walks
//! literal bytes and steps over fixed-width fields by their known length, so it
//! discriminates through the namespace and database identifiers without decoding
//! them. It stops at the first variable-width field, since nothing after one sits
//! at a fixed offset; the keys reachable past that point are then tried in order,
//! longest layout first.
//!
//! Trying real decoders rather than a hand-rolled byte matcher is deliberate: the
//! reverse map cannot drift from the encoders, because it *is* the encoders run
//! backwards. It also resolves declared overlaps for free — `storekey` rejects
//! trailing bytes, so a key that is a prefix of the bytes under inspection fails
//! and the longer, more specific layout wins.

use std::collections::BTreeMap;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Attribute;

use crate::model::{Model, Sym, TypeDef, Width, camel, render_type, value_variant_name};

/// One `AnyValue` variant: its name, the value type it carries, and the `cfg`
/// under which it exists.
struct ValueVariant {
	name: proc_macro2::Ident,
	ty: TokenStream,
	cfgs: Vec<Attribute>,
}

/// One step of the dispatch trie.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
enum Probe {
	/// The byte that must appear at the current offset.
	Byte(u8),
	/// A fixed-width field: step over it without decoding.
	Skip(usize),
}

/// The leading probe sequence of a layout: everything that sits at a statically
/// known offset, up to the first variable-width field.
fn probes(syms: &[Sym]) -> Vec<Probe> {
	let mut out = Vec::new();
	for sym in syms {
		match sym {
			Sym::Lit(bytes) => out.extend(bytes.iter().copied().map(Probe::Byte)),
			Sym::Field {
				width: Width::Fixed(n),
				..
			} => out.push(Probe::Skip(*n)),
			// A variable-width field ends static addressing.
			Sym::Field {
				..
			} => break,
		}
	}
	out
}

#[derive(Default)]
struct Node {
	children: BTreeMap<Probe, Node>,
	/// Layouts that end their probe sequence here, or that cannot be
	/// discriminated further.
	here: Vec<usize>,
}

impl Node {
	fn insert(&mut self, seq: &[Probe], idx: usize) {
		match seq.split_first() {
			None => self.here.push(idx),
			Some((first, rest)) => {
				self.children.entry(first.clone()).or_default().insert(rest, idx)
			}
		}
	}
}

pub fn generate(model: &Model) -> TokenStream {
	let stored: Vec<&TypeDef> = model.types.iter().filter(|t| t.is_decodable()).collect();

	let any_key = any_key_enum(&stored);
	let any_value = any_value_enum(&stored);
	let patterns = pattern_table(&stored);
	let decoder = decoder(&stored);
	let describe = describe();

	quote! {
		#any_key
		#any_value
		#patterns
		#decoder
		#describe
	}
}

fn variant(def: &TypeDef) -> proc_macro2::Ident {
	format_ident!("{}", camel(&def.source))
}

fn any_key_enum(stored: &[&TypeDef]) -> TokenStream {
	let variants = stored.iter().map(|def| {
		let v = variant(def);
		let ty = &def.name;
		let lt = def.lifetime_toks();
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		let doc = format!(" `{}`", def.route);
		quote!(#(#cfgs)* #[doc = #doc] #v(#ty #lt),)
	});

	let kind_arms = stored.iter().map(|def| {
		let v = variant(def);
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		quote!(#(#cfgs)* Self::#v(..) => KeyKind::#v,)
	});

	let encode_arms = stored.iter().map(|def| {
		let v = variant(def);
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		quote!(#(#cfgs)* Self::#v(key) => KVKey::encode_key(key),)
	});

	quote! {
		/// Any key the keyspace can store, decoded from its bytes.
		///
		/// Exhaustive by construction: the schema that generates the encoders
		/// generates this enum, so a key type cannot exist without a variant here.
		#[derive(Clone, Debug, PartialEq)]
		pub(crate) enum AnyKey<'a> {
			#(#variants)*
		}

		impl AnyKey<'_> {
			/// Which kind of key this is, without its fields.
			pub(crate) fn kind(&self) -> KeyKind {
				match self {
					#(#kind_arms)*
				}
			}

			/// Re-encodes this key, closing the round trip [`Self::decode`] opens.
			///
			/// Decoding and re-encoding a stored key must reproduce it exactly, which
			/// is what makes a byte string from a scan safe to hand back to the store.
			pub(crate) fn encode_key(&self) -> ::anyhow::Result<Key<'static>> {
				match self {
					#(#encode_arms)*
				}
			}
		}
	}
}

/// One variant per distinct value type, so a scanned entry can be decoded without
/// the caller knowing what it holds.
fn any_value_enum(stored: &[&TypeDef]) -> TokenStream {
	// A value type may be shared by several entries. Its variant carries a `cfg`
	// only when every entry that uses it is gated the same way; otherwise the
	// variant must exist unconditionally, because at least one ungated entry needs
	// it. Getting this wrong makes the enum name a type that has been configured
	// out.
	let mut seen: BTreeMap<String, ValueVariant> = BTreeMap::new();
	for def in stored {
		let Some(value) = &def.value else {
			continue;
		};
		let text = render_type(value);
		let cfgs: Vec<Attribute> = def.attrs.cfgs.clone();
		match seen.get_mut(&text) {
			Some(existing) => {
				if existing.cfgs != cfgs {
					existing.cfgs.clear();
				}
			}
			None => {
				seen.insert(
					text.clone(),
					ValueVariant {
						name: value_variant_name(&text),
						ty: quote!(#value),
						cfgs,
					},
				);
			}
		}
	}

	let variants = seen.values().map(|v| {
		let (name, ty, cfgs) = (&v.name, &v.ty, &v.cfgs);
		quote!(#(#cfgs)* #name(#ty),)
	});
	let decode_arms = stored.iter().filter_map(|def| {
		let value = def.value.as_ref()?;
		let text = render_type(value);
		let name = &seen.get(&text)?.name;
		let v = variant(def);
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		Some(quote! {
			#(#cfgs)*
			Self::#v(key) => AnyValue::#name(
				<#value as KVValue>::kv_decode_value(bytes, KVKey::value_context(key))?
			),
		})
	});

	let debug_arms = seen.values().map(|v| {
		let (name, cfgs) = (&v.name, &v.cfgs);
		let text = name.to_string();
		quote!(#(#cfgs)* Self::#name(..) => f.write_str(#text),)
	});

	quote! {
		/// Any value the keyspace can store, decoded alongside its key.
		///
		/// Each variant carries the decoded value for a caller to match on. A
		/// given caller reads the few variants it cares about, so the rest look
		/// unread from where they are defined.
		#[allow(dead_code)]
		pub(crate) enum AnyValue {
			#(#variants)*
		}

		/// Prints which kind of value this is.
		///
		/// Deliberately not derived: deriving would require `Debug` of every value
		/// type in the keyspace, and the name is what a diagnostic needs. Print the
		/// inner value directly when its contents matter.
		impl ::std::fmt::Debug for AnyValue {
			fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
				match self {
					#(#debug_arms)*
				}
			}
		}

		impl AnyKey<'_> {
			/// Decodes the value stored under this key.
			///
			/// The key supplies its own decode context, so value types that carry
			/// part of their identity in the key (a record's id, for one) are
			/// reconstructed correctly rather than guessed at.
			pub(crate) fn decode_value(&self, bytes: &[u8]) -> ::anyhow::Result<AnyValue> {
				Ok(match self {
					#(#decode_arms)*
				})
			}
		}

		/// Decodes one scanned entry: its key and the value stored under it.
		pub(crate) fn decode_entry<'a>(
			key: &'a [u8],
			value: &[u8],
		) -> ::anyhow::Result<Option<(AnyKey<'a>, AnyValue)>> {
			let Some(key) = AnyKey::decode(key) else {
				return Ok(None);
			};
			let value = key.decode_value(value)?;
			Ok(Some((key, value)))
		}
	}
}

/// A static description of every key layout, for tooling and for `describe`.
fn pattern_table(stored: &[&TypeDef]) -> TokenStream {
	let rows = stored.iter().map(|def| {
		let v = variant(def);
		let route = &def.route;
		// Every tag in the layout, not just the leading one: the leading run is
		// shared by most of the keyspace, so a check over it says almost nothing
		// about any individual key.
		let tags = def.literals.iter().map(|(bytes, _)| proc_macro2::Literal::byte_string(bytes));
		let steps = probes(&def.syms).into_iter().map(|p| match p {
			Probe::Byte(b) => quote!(KeyProbe::Byte(#b)),
			Probe::Skip(n) => quote!(KeyProbe::Skip(#n)),
		});
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		quote! {
			#(#cfgs)*
			KeyPattern {
				kind: KeyKind::#v,
				route: #route,
				tags: &[#(#tags),*],
				probes: &[#(#steps),*],
			},
		}
	});

	quote! {
		/// One step of a key layout that sits at a statically known offset.
		#[derive(Clone, Copy, Debug, PartialEq, Eq)]
		pub(crate) enum KeyProbe {
			/// The byte that must appear at this offset.
			Byte(u8),
			/// A fixed-width field: this many bytes, whatever they are.
			Skip(usize),
		}

		/// The layout of one key kind.
		#[derive(Clone, Copy, Debug)]
		pub(crate) struct KeyPattern {
			pub kind: KeyKind,
			/// Route in the keyspace map notation.
			pub route: &'static str,
			/// Every literal tag in the layout, in encode order.
			pub tags: &'static [&'static [u8]],
			/// The layout up to its first variable-width field, which is as far as
			/// bytes can be matched without decoding them.
			pub probes: &'static [KeyProbe],
		}

		impl KeyPattern {
			/// How many leading bytes of `key` this layout accounts for.
			///
			/// Stops at the first byte that disagrees, or at the end of the statically
			/// addressable part of the layout. Never decodes, so it is safe on
			/// arbitrary bytes.
			pub(crate) fn matched(&self, key: &[u8]) -> usize {
				let mut at = 0usize;
				for probe in self.probes {
					match probe {
						KeyProbe::Byte(b) => {
							if key.get(at) != Some(b) {
								return at;
							}
							at += 1;
						}
						KeyProbe::Skip(n) => {
							if key.len() < at + n {
								return at;
							}
							at += n;
						}
					}
				}
				at
			}
		}

		/// Every key layout in the keyspace, in declaration order.
		pub(crate) static PATTERNS: &[KeyPattern] = &[#(#rows)*];
	}
}

fn decoder(stored: &[&TypeDef]) -> TokenStream {
	let mut root = Node::default();
	for (i, def) in stored.iter().enumerate() {
		root.insert(&probes(&def.syms), i);
	}

	let body = node_tokens(&root, stored, 0);

	quote! {
		impl<'a> AnyKey<'a> {
			/// Identifies raw key bytes and decodes them.
			///
			/// Returns `None` for bytes that belong to no declared key, which is
			/// the signal that a scan has run past the keyspace or that the data
			/// predates a schema change.
			pub(crate) fn decode(bytes: &'a [u8]) -> Option<Self> {
				#body
				None
			}
		}
	}
}

/// Emits the dispatch for one trie node: a match on the byte at `offset`, with
/// the candidates reachable here as the fallback.
fn node_tokens(node: &Node, stored: &[&TypeDef], offset: usize) -> TokenStream {
	// Candidates that stop discriminating at this node are tried here, longest
	// layout first so a more specific extension wins over the key it extends.
	let mut local = node.here.clone();
	local.sort_by_key(|i| std::cmp::Reverse(stored[*i].syms.len()));
	let attempts = local.iter().map(|i| {
		let def = stored[*i];
		let ty = &def.name;
		let v = variant(def);
		let cfgs: Vec<_> = def.attrs.cfgs.iter().collect();
		quote! {
			#(#cfgs)*
			if let Ok(key) = <#ty as KVKeyDecode<'a>>::decode_key(bytes) {
				return Some(AnyKey::#v(key));
			}
		}
	});

	let byte_arms: Vec<TokenStream> = node
		.children
		.iter()
		.filter_map(|(probe, child)| match probe {
			Probe::Byte(b) => {
				let inner = node_tokens(child, stored, offset + 1);
				Some(quote!(Some(&#b) => { #inner }))
			}
			Probe::Skip(_) => None,
		})
		.collect();

	let skip_arms = node.children.iter().filter_map(|(probe, child)| match probe {
		Probe::Skip(n) => Some(node_tokens(child, stored, offset + n)),
		Probe::Byte(_) => None,
	});
	let skip_arms: Vec<_> = skip_arms.collect();

	let dispatch = if byte_arms.is_empty() {
		TokenStream::new()
	} else {
		quote! {
			match bytes.get(#offset) {
				#(#byte_arms)*
				_ => {}
			}
		}
	};

	quote! {
		#dispatch
		#(#skip_arms)*
		#(#attempts)*
	}
}

fn describe() -> TokenStream {
	quote! {
		/// What a run of key bytes turned out to be.
		#[derive(Debug)]
		pub(crate) enum KeyDescription {
			/// The bytes decoded cleanly.
			Known {
				kind: KeyKind,
				route: &'static str,
			},
			/// The bytes matched a known route only part of the way.
			Unknown {
				/// How many leading bytes matched the closest declared layout.
				matched: usize,
				/// The routes that matched that far, and no route matched further.
				candidates: ::std::vec::Vec<&'static str>,
				/// The bytes, escaped for display.
				escaped: ::std::string::String,
			},
		}

		impl ::std::fmt::Display for KeyDescription {
			fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
				match self {
					Self::Known { route, .. } => write!(f, "{route}"),
					Self::Unknown { matched, candidates, escaped } => {
						write!(f, "unrecognised key `{escaped}`")?;
						if *matched > 0 {
							write!(f, "; matched {matched} leading byte(s)")?;
						}
						// One line per key is the useful size here: this is printed once
						// per unrecognised key in a scan report, so an exhaustive list
						// of ties would bury the ones worth reading.
						const SHOWN: usize = 8;
						if !candidates.is_empty() {
							write!(f, "; closest routes: {}", candidates[..candidates.len().min(SHOWN)].join(", "))?;
							if candidates.len() > SHOWN {
								write!(f, " (+{} more)", candidates.len() - SHOWN)?;
							}
						}
						Ok(())
					}
				}
			}
		}

		/// Explains a run of key bytes, whether or not they decode.
		///
		/// Never fails and never panics: a corrupt or foreign key still produces a
		/// readable answer, which is the point. Intended for diagnostics, scans of
		/// unknown data, and leaked-key reports.
		pub(crate) fn describe(bytes: &[u8]) -> KeyDescription {
			if let Some(key) = AnyKey::decode(bytes) {
				let kind = key.kind();
				return KeyDescription::Known { kind, route: kind.route() };
			}

			// Walking each layout rather than comparing leading tags is what makes
			// this answer specific: the leading run is shared by nearly every key,
			// so a tag comparison reports the whole keyspace as equally close.
			let matched = PATTERNS.iter().map(|p| p.matched(bytes)).max().unwrap_or(0);
			let candidates: ::std::vec::Vec<&'static str> = match matched {
				0 => ::std::vec::Vec::new(),
				_ => PATTERNS
					.iter()
					.filter(|p| p.matched(bytes) == matched)
					.map(|p| p.route)
					.collect(),
			};

			let mut escaped = ::std::string::String::new();
			for b in bytes.iter().take(64) {
				escaped.push_str(&::std::ascii::escape_default(*b).to_string());
			}
			if bytes.len() > 64 {
				escaped.push_str("...");
			}

			KeyDescription::Unknown { matched, candidates, escaped }
		}
	}
}

/// The human-readable keyspace map, emitted three ways so it cannot drift: as the
/// generated module's own documentation, as a constant, and as a snapshot the
/// tests compare against a checked-in file.
pub fn keyspace_map(model: &Model) -> String {
	let mut rows: Vec<(String, String, String)> = Vec::new();
	for def in &model.types {
		let value = match &def.value {
			Some(v) => quote!(#v).to_string().replace(' ', ""),
			None => match def.role {
				crate::model::Role::Prefix => "(range)".to_owned(),
				_ => "(subtree)".to_owned(),
			},
		};
		rows.push((def.route.clone(), value, def.name.to_string()));
	}

	let route_width = rows.iter().map(|(r, ..)| r.len()).max().unwrap_or(0);
	let name_width = rows.iter().map(|(.., n)| n.len()).max().unwrap_or(0);

	let mut out = String::new();
	out.push_str("SurrealDB keyspace\n");
	out.push_str("==================\n\n");
	out.push_str(
		"Every route, the generated type that addresses it, and the value it stores.\n\
		 Generated from the `keyspace!` schema; do not edit by hand.\n\n",
	);
	// The type name is right-aligned so every `->` lines up, which puts the reading
	// order route, type, value on one axis.
	for (route, value, name) in rows {
		out.push_str(&format!("{route:<route_width$} {name:>name_width$} ->  {value}\n"));
	}

	// A waived overlap is the one place the layout is knowingly ambiguous, so it
	// belongs in the artifact reviewers read rather than only in the schema.
	if !model.waivers.is_empty() {
		out.push_str("\nWaived overlaps\n---------------\n");
		for waiver in &model.waivers {
			out.push_str(&format!(
				"{} vs {} at `{}`: {}\n",
				waiver.from,
				waiver.to,
				crate::model::render_lit(&waiver.at),
				waiver.tracked
			));
		}
	}
	out
}
