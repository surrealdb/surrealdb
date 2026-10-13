//! Semantic layer: resolves the AST into the flat model that the checker and the
//! code generator both consume.
//!
//! Resolution does three things:
//!
//! 1. Flattens the level nesting. Every generated struct carries the fields of its enclosing levels
//!    followed by its own, in encode order, so a child key is unconstructible without the data
//!    identifying its parents.
//! 2. Derives every Rust name from the schema idents, so the schema never spells a type name.
//! 3. Lowers each type to two parallel programs: an [`Emit`] sequence (what the encoder writes, in
//!    order) and a [`Sym`] sequence (the symbolic byte layout the checker reasons about). Both come
//!    from the same segments, so the layout that is checked is the layout that is written.

use std::cell::Cell;
use std::collections::BTreeMap;

use proc_macro2::{Group, Ident, Span, TokenStream, TokenTree};
use quote::{ToTokens, format_ident, quote};
use syn::spanned::Spanned;
use syn::{Expr, LitInt, Type};

use crate::parse::{DeclWidth, Item, Keyspace, Opts, PassthroughAttrs, Segment, TypeDecl};

/// The reserved lifetime a schema uses to mark a borrowed field type. It is
/// replaced with the generated struct lifetime.
pub const KEY_LIFETIME: &str = "k";

/// Encoded width of a field, as implied by its `storekey` encoding.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Width {
	/// Exactly `n` bytes, no terminator.
	Fixed(usize),
	/// Self-delimiting and variable length: the encoding itself says where the
	/// field ends, either by escaping and terminating its bytes or by a leading
	/// discriminant that fixes the length of what follows.
	Var,
	/// A terminated sequence of self-delimiting elements.
	List,
}

/// One element of the symbolic byte layout. Two keys are provably disjoint when
/// their symbol sequences diverge on differing literal bytes; every other
/// outcome is an extension, an ambiguity, or an outright duplicate.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Sym {
	Lit(Vec<u8>),
	Field {
		name: String,
		width: Width,
	},
}

/// One step of the encoder.
///
/// Sizes are uneven because one variant carries a literal run, but these are
/// built once per schema and never held in bulk, so boxing would only add an
/// allocation.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Emit {
	Lit(Vec<u8>),
	Field(usize),
	/// Encode each element behind `mark_terminator()`, then write the terminator.
	ListTerminated(usize),
	/// As above but omit the terminator, so the result bounds a partial list.
	ListOpen(usize),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
	/// A stored key: has a value type and takes part in disjointness checking.
	Key,
	/// A level root: names a whole subtree, used as a scan bound and a builder.
	Root,
	/// A truncation point derived from a `@` marker.
	Prefix,
}

#[derive(Clone)]
pub struct FieldDef {
	pub name: Ident,
	/// Fully resolved type, with the struct lifetime substituted in.
	pub ty: TokenStream,
	pub width: Width,
	/// Element type, for list fields.
	pub elem: Option<TokenStream>,
	/// Whether the resolved type borrows from the key bytes.
	pub borrowed: bool,
	/// True when the field came from an enclosing level rather than this item.
	pub inherited: bool,
}

/// The `storekey` format parameter a type is encoded under.
///
/// One variant carries a `syn::Type` and so dominates the size, but a schema
/// holds only a handful of these, so boxing would add an allocation for nothing.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Format {
	/// `Encode<()>`, the default format.
	Default,
	/// `Encode<T>` for one specific `T`.
	Pinned(Type),
	/// `impl<F> Encode<F>`: embeddable in keys of any format.
	Generic,
}

impl Format {
	pub fn describe(&self) -> String {
		match self {
			Format::Default => "()".to_owned(),
			Format::Pinned(t) => t.to_token_stream().to_string().replace(' ', ""),
			Format::Generic => "<generic>".to_owned(),
		}
	}
}

pub struct TypeDef {
	pub name: Ident,
	pub role: Role,
	pub attrs: PassthroughAttrs,
	pub fields: Vec<FieldDef>,
	pub emit: Vec<Emit>,
	pub syms: Vec<Sym>,
	pub value: Option<Type>,
	pub ctx: Option<Expr>,
	pub also_range: bool,
	pub format: Format,
	pub derives: Vec<Ident>,
	pub lifetime: bool,
	/// Schema ident, for diagnostics and for the keyspace map.
	pub source: Ident,
	/// Human-readable route, e.g. `/*{ns}*{db}!tb{tb}`.
	pub route: String,
	/// Name of the entry this one extends, resolved into `extends` once every
	/// entry has an index.
	pub extends_name: Option<Ident>,
	pub extends: Option<usize>,
	pub ignore_trailing: bool,
	/// The level root this type hangs off, for builder generation.
	pub parent_root: Option<Ident>,
	/// True when at least one stored key's layout continues past this type's
	/// bytes, so an inclusive upper bound on its last field must use
	/// `next_neighbour` rather than `next`.
	pub extended_by_keys: bool,
	/// A level with no items beneath it. Such a subspace is read only by range, so
	/// nothing inside it establishes that it does not overlap a sibling; the region
	/// itself takes part in the disjointness check instead.
	pub leaf_level: bool,
	/// Method name used for parent-to-child builders.
	pub builder: Ident,
	/// Every literal segment with the span it was written at, so a diagnostic
	/// about a tag byte can point at the tag rather than the whole entry.
	pub literals: Vec<(Vec<u8>, Span)>,
	/// Schema idents of the enclosing levels, outermost first. A waiver naming a
	/// level applies to every type beneath it.
	pub ancestors: Vec<Ident>,
}

impl TypeDef {
	/// Whether real entries live at exactly these bytes.
	pub fn is_stored(&self) -> bool {
		self.value.is_some()
	}

	/// Whether the type takes part in the disjointness relation. Prefixes do not:
	/// they name a byte range, not an entry.
	pub fn is_addressable(&self) -> bool {
		self.role == Role::Key || self.is_stored() || self.leaf_level
	}

	/// Whether raw bytes at this type can be turned back into a typed key and
	/// value. A bound-only subspace cannot: nothing addresses an individual key
	/// inside it, so there is nothing to decode.
	pub fn is_decodable(&self) -> bool {
		self.is_stored()
	}

	pub fn lifetime_toks(&self) -> Option<TokenStream> {
		self.lifetime.then(|| quote!(<'a>))
	}

	/// Fields this type contributes beyond its enclosing level.
	pub fn own_fields(&self) -> impl Iterator<Item = &FieldDef> {
		self.fields.iter().filter(|f| !f.inherited)
	}
}

pub struct ResolvedEnum {
	pub attrs: PassthroughAttrs,
	pub ident: Ident,
	pub repr: Ident,
	pub variants: Vec<(Ident, LitInt)>,
}

pub struct Waiver {
	pub from: Ident,
	pub to: Ident,
	/// The literal bytes the licensed collision happens at. A waiver silences only
	/// divergences at these bytes, so a *different* collision between the same two
	/// declarations still reports even though the pair is named.
	pub at: Vec<u8>,
	/// Why the collision is tolerated. Rendered into the keyspace map, so a waived
	/// overlap is visible in the checked-in artifact rather than only in the schema.
	pub tracked: String,
	pub span: Span,
	/// Set when the checker matches this waiver against a real ambiguity, so
	/// stale waivers can be reported.
	pub used: Cell<bool>,
}

pub struct Model {
	pub enums: Vec<ResolvedEnum>,
	pub types: Vec<TypeDef>,
	pub waivers: Vec<Waiver>,
}

/// Accumulates every diagnostic so one expansion reports all of them.
#[derive(Default)]
pub struct Errors {
	inner: Option<syn::Error>,
}

impl Errors {
	pub fn push(&mut self, span: Span, msg: impl Into<String>) {
		let err = syn::Error::new(span, msg.into());
		match &mut self.inner {
			Some(existing) => existing.combine(err),
			None => self.inner = Some(err),
		}
	}

	#[cfg_attr(not(test), allow(dead_code))]
	pub fn is_empty(&self) -> bool {
		self.inner.is_none()
	}

	pub fn into_result(self) -> syn::Result<()> {
		match self.inner {
			Some(e) => Err(e),
			None => Ok(()),
		}
	}

	/// Rendered messages, one per diagnostic. Used by the in-crate checker tests
	/// in place of compile-fail fixtures.
	#[cfg_attr(not(test), allow(dead_code))]
	pub fn messages(&self) -> Vec<String> {
		match &self.inner {
			Some(e) => e.clone().into_iter().map(|e| e.to_string()).collect(),
			None => Vec::new(),
		}
	}
}

/// A declared field type: its encoded width plus the type to emit.
struct TypeInfo {
	width: DeclWidth,
	expansion: Option<Type>,
}

struct Registry {
	types: BTreeMap<String, TypeInfo>,
}

impl Registry {
	fn build(ks: &Keyspace, errors: &mut Errors) -> Self {
		let mut types: BTreeMap<String, TypeInfo> = BTreeMap::new();
		for decl in &ks.types {
			declare(decl, &mut types, errors);
		}
		// Schema-declared enums are fixed-width by construction: one pinned
		// discriminant of the declared repr and nothing else.
		for e in &ks.enums {
			let width = match e.repr.to_string().as_str() {
				"u8" => 1,
				"u16" => 2,
				"u32" => 4,
				"u64" => 8,
				other => {
					errors.push(
						e.repr.span(),
						format!("unsupported enum repr `{other}`; expected u8, u16, u32 or u64"),
					);
					1
				}
			};
			types.insert(
				e.ident.to_string(),
				TypeInfo {
					width: DeclWidth::Fixed(width),
					expansion: None,
				},
			);
		}
		Registry {
			types,
		}
	}

	/// Resolves a written field type to `(emitted type, width, borrowed)`.
	///
	/// The lookup strips any `Cow<'_, _>` wrapper, so `Cow<'k, TableName>`
	/// resolves through `TableName`. An unresolved type is an R5 violation: the
	/// schema must declare every field type, because a type whose encoded width
	/// is unknown cannot be reasoned about by the disjointness check.
	fn resolve(&self, ty: &Type, errors: &mut Errors) -> (TokenStream, Width, bool) {
		let lookup = strip_cow(ty);
		let Some(name) = type_key(&lookup) else {
			errors.push(
				ty.span(),
				"unsupported field type shape; expected a named type, optionally wrapped in `Cow`",
			);
			return (substitute_key_lifetime(ty), Width::Var, mentions_key_lifetime(ty));
		};

		// Checked on the written type and again on what an alias expands to, because
		// an alias is how field types are normally spelled here and only the
		// expansion says what actually gets encoded.
		if reject_floats(ty, errors) {
			return (substitute_key_lifetime(ty), Width::Fixed(8), false);
		}

		let Some(info) = self.types.get(&name) else {
			errors.push(
				ty.span(),
				format!(
					"field type `{name}` is not declared in the `types` prelude, so its encoded \
					 width is unknown; add `fixed(N) {name};` or `var {name};`"
				),
			);
			return (substitute_key_lifetime(ty), Width::Var, mentions_key_lifetime(ty));
		};

		// A bare alias expands to its declared type; anything spelled out is used
		// verbatim.
		let emitted = match (&info.expansion, is_bare_ident(ty)) {
			(Some(expansion), true) => expansion.clone(),
			_ => ty.clone(),
		};

		if reject_floats(&emitted, errors) {
			return (substitute_key_lifetime(&emitted), Width::Fixed(8), false);
		}

		let width = match info.width {
			DeclWidth::Fixed(n) => Width::Fixed(n),
			DeclWidth::Var => Width::Var,
		};
		(substitute_key_lifetime(&emitted), width, mentions_key_lifetime(&emitted))
	}
}

/// R5: no part of a field's type may be a float. Returns whether one was found.
///
/// `-0.0` and `0.0` encode to different bytes while comparing equal, and `NaN`
/// sorts at an extreme, so a float in a key makes byte order disagree with value
/// equality. The whole type is scanned rather than just its head, because a float
/// nested in a tuple or an array is the same hazard.
fn reject_floats(ty: &Type, errors: &mut Errors) -> bool {
	fn scan(tokens: TokenStream, span: Span, errors: &mut Errors) -> bool {
		let mut found = false;
		for token in tokens {
			match token {
				proc_macro2::TokenTree::Ident(id) if id == "f32" || id == "f64" => {
					errors.push(
						span,
						format!(
							"floating-point fields are not allowed in keys: `{id}`'s `-0.0` and \
							 `0.0` encode to different bytes while comparing equal, and `NaN` sorts \
							 at the extremes, so key order would not agree with value equality"
						),
					);
					found = true;
				}
				proc_macro2::TokenTree::Group(g) => found |= scan(g.stream(), span, errors),
				_ => {}
			}
		}
		found
	}
	scan(quote!(#ty), ty.span(), errors)
}

fn declare(decl: &TypeDecl, types: &mut BTreeMap<String, TypeInfo>, errors: &mut Errors) {
	for (name, expansion) in &decl.names {
		let key = name.to_string();
		if types.contains_key(&key) {
			errors.push(name.span(), format!("field type `{key}` is declared twice"));
			continue;
		}
		types.insert(
			key,
			TypeInfo {
				width: decl.width,
				expansion: expansion.clone(),
			},
		);
	}
}

/// State threaded down the level nesting while flattening.
#[derive(Clone, Default)]
struct Scope {
	fields: Vec<FieldDef>,
	emit: Vec<Emit>,
	syms: Vec<Sym>,
	route: String,
	root: Option<Ident>,
	ancestors: Vec<Ident>,
	literals: Vec<(Vec<u8>, Span)>,
}

pub fn resolve(ks: &Keyspace) -> (Model, Errors) {
	let mut errors = Errors::default();
	let registry = Registry::build(ks, &mut errors);
	let bases = collect_bases(&ks.items);

	let mut model = Model {
		enums: ks
			.enums
			.iter()
			.map(|e| ResolvedEnum {
				attrs: e.attrs.clone(),
				ident: e.ident.clone(),
				repr: e.repr.clone(),
				variants: e.variants.clone(),
			})
			.collect(),
		types: Vec::new(),
		waivers: Vec::new(),
	};

	for item in &ks.items {
		walk(item, &Scope::default(), &registry, &bases, &mut model, &mut errors);
	}

	link_extensions(&mut model, &mut errors);
	mark_extended(&mut model);
	for item in &ks.items {
		collect_waivers(item, &mut model.waivers);
	}

	(model, errors)
}

/// Indexes every entry's own segment list by ident, so an entry declared as an
/// extension can be given its base's layout followed by its own additions. Idents
/// are unique across the whole schema (R1), so a flat index is enough and the
/// declaration order of base and extension does not matter.
/// Expansion is transitive: a base that is itself an extension is expanded first,
/// which is what lets a family nest more than two deep.
fn collect_bases(items: &[Item]) -> BTreeMap<String, Vec<Segment>> {
	let mut raw: BTreeMap<String, (Option<Ident>, Vec<Segment>)> = BTreeMap::new();
	fn visit(items: &[Item], raw: &mut BTreeMap<String, (Option<Ident>, Vec<Segment>)>) {
		for item in items {
			match item {
				Item::Entry(e) => {
					raw.insert(e.ident.to_string(), (e.extends.clone(), e.segments.clone()));
				}
				Item::Level(l) => visit(&l.items, raw),
			}
		}
	}
	visit(items, &mut raw);

	// Resolve each entry against its chain, guarding against a cycle so a
	// self-referential schema reports a diagnostic rather than looping.
	fn expand(
		name: &str,
		raw: &BTreeMap<String, (Option<Ident>, Vec<Segment>)>,
		seen: &mut Vec<String>,
	) -> Vec<Segment> {
		if seen.iter().any(|s| s == name) {
			return Vec::new();
		}
		seen.push(name.to_owned());
		let Some((base, own)) = raw.get(name) else {
			return Vec::new();
		};
		let mut segments: Vec<Segment> = match base {
			Some(base) => expand(&base.to_string(), raw, seen)
				.into_iter()
				// A base's truncation markers belong to the base; re-emitting them
				// would generate a second, identically-named bound type.
				.filter(|s| !matches!(s, Segment::Cut { .. }))
				.collect(),
			None => Vec::new(),
		};
		segments.extend(own.iter().cloned());
		segments
	}

	raw.keys().map(|name| (name.clone(), expand(name, &raw, &mut Vec::new()))).collect()
}

/// An extension's layout is its base's layout plus its own segments.
///
/// The base's truncation markers are dropped: they belong to the base and would
/// otherwise generate a second, identically-named bound type.
fn extended_segments(
	base: &Ident,
	own: &[Segment],
	bases: &BTreeMap<String, Vec<Segment>>,
) -> Vec<Segment> {
	let mut segments: Vec<Segment> = bases
		.get(&base.to_string())
		.map(|segs| segs.iter().filter(|s| !matches!(s, Segment::Cut { .. })).cloned().collect())
		.unwrap_or_default();
	segments.extend(own.iter().cloned());
	segments
}

fn walk(
	item: &Item,
	scope: &Scope,
	registry: &Registry,
	bases: &BTreeMap<String, Vec<Segment>>,
	model: &mut Model,
	errors: &mut Errors,
) {
	match item {
		Item::Entry(entry) => {
			let segments = match &entry.extends {
				Some(base) => extended_segments(base, &entry.segments, bases),
				None => entry.segments.clone(),
			};
			let (mut def, prefixes) = build(
				&entry.ident,
				format_ident!("{}Key", camel(&entry.ident)),
				Role::Key,
				scope,
				&segments,
				&entry.attrs,
				entry.value.clone(),
				&entry.opts,
				registry,
				errors,
			);
			def.extends_name.clone_from(&entry.extends);
			model.types.push(def);
			model.types.extend(prefixes);
		}
		Item::Level(level) => {
			let root_name = format_ident!("{}Root", camel(&level.ident));
			let (def, prefixes) = build(
				&level.ident,
				root_name.clone(),
				Role::Root,
				scope,
				&level.segments,
				&level.attrs,
				level.value.clone(),
				&level.opts,
				registry,
				errors,
			);

			let mut def = def;
			def.leaf_level = level.items.is_empty();
			let inner = Scope {
				fields: def
					.fields
					.iter()
					.cloned()
					.map(|mut f| {
						f.inherited = true;
						f
					})
					.collect(),
				emit: def.emit.clone(),
				syms: def.syms.clone(),
				route: def.route.clone(),
				root: Some(root_name),
				ancestors: {
					let mut chain = scope.ancestors.clone();
					chain.push(level.ident.clone());
					chain
				},
				literals: def.literals.clone(),
			};
			model.types.push(def);
			model.types.extend(prefixes);

			for child in &level.items {
				walk(child, &inner, registry, bases, model, errors);
			}
		}
	}
}

/// A truncation point captured while walking an item's segments.
struct Cut {
	name: Ident,
	fields: usize,
	emit: usize,
	syms: usize,
	route: String,
	open: bool,
}

#[allow(clippy::too_many_arguments)]
fn build(
	source: &Ident,
	name: Ident,
	role: Role,
	scope: &Scope,
	segments: &[Segment],
	attrs: &crate::parse::ItemAttrs,
	value: Option<Type>,
	opts: &Opts,
	registry: &Registry,
	errors: &mut Errors,
) -> (TypeDef, Vec<TypeDef>) {
	let mut fields = scope.fields.clone();
	let mut emit = scope.emit.clone();
	let mut syms = scope.syms.clone();
	let mut route = scope.route.clone();
	let mut cuts: Vec<Cut> = Vec::new();
	let mut last_own_field: Option<Ident> = None;
	let mut literals: Vec<(Vec<u8>, Span)> = scope.literals.clone();

	for segment in segments {
		match segment {
			Segment::Lit {
				bytes,
				span,
			} => {
				if bytes.is_empty() {
					errors.push(*span, "an empty literal contributes no bytes; remove it");
				}
				emit.push(Emit::Lit(bytes.clone()));
				push_lit(&mut syms, bytes);
				literals.push((bytes.clone(), *span));
				route.push_str(&render_lit(bytes));
			}
			Segment::Raw {
				byte,
				span,
			} => {
				emit.push(Emit::Lit(vec![*byte]));
				push_lit(&mut syms, &[*byte]);
				literals.push((vec![*byte], *span));
				route.push_str(&render_lit(&[*byte]));
			}
			Segment::Field {
				name,
				ty,
			} => {
				let idx = fields.len();
				if fields.iter().any(|f| f.name == *name) {
					errors.push(
						name.span(),
						format!(
							"field `{name}` collides with a field inherited from an enclosing \
							 level; rename it"
						),
					);
				}
				match ty {
					crate::parse::FieldTy::Plain(t) => {
						let (toks, width, borrowed) = registry.resolve(t, errors);
						fields.push(FieldDef {
							name: name.clone(),
							ty: toks,
							width,
							elem: None,
							borrowed,
							inherited: false,
						});
						emit.push(Emit::Field(idx));
						syms.push(Sym::Field {
							name: name.to_string(),
							width,
						});
						route.push_str(&format!("{{{name}}}"));
					}
					crate::parse::FieldTy::List {
						elem,
						..
					} => {
						let (elem_toks, _, _) = registry.resolve(elem, errors);
						fields.push(FieldDef {
							name: name.clone(),
							ty: quote!(::std::borrow::Cow<'a, [#elem_toks]>),
							width: Width::List,
							elem: Some(elem_toks),
							borrowed: true,
							inherited: false,
						});
						emit.push(Emit::ListTerminated(idx));
						syms.push(Sym::Field {
							name: name.to_string(),
							width: Width::List,
						});
						route.push_str(&format!("{{{name}..}}"));
					}
				}
				last_own_field = Some(name.clone());
			}
			Segment::Cut {
				name: explicit,
				open,
				span,
			} => {
				if *open && !matches!(emit.last(), Some(Emit::ListTerminated(_))) {
					errors.push(
						*span,
						"`@open` must follow a list field: it exists to bound a partial list by \
						 omitting the list terminator",
					);
				}
				let derived = match explicit {
					Some(explicit) => explicit.clone(),
					None => {
						let base = camel(source);
						let field = last_own_field.as_ref().map(camel).unwrap_or_default();
						let open_part = if *open {
							"Open"
						} else {
							""
						};
						format_ident!("{base}{field}{open_part}Prefix", span = *span)
					}
				};
				// An open cut sits at the same segment as the closed one, so its route
				// has to say which it is: the list placeholder is left unclosed to show
				// that the bound stops inside the list rather than after it. Without
				// that the two bounds are indistinguishable in the keyspace map.
				let route = match *open {
					true => route.strip_suffix('}').unwrap_or(&route).to_owned(),
					false => route.clone(),
				};
				cuts.push(Cut {
					name: derived,
					fields: fields.len(),
					emit: emit.len(),
					syms: syms.len(),
					route,
					open: *open,
				});
			}
		}
	}

	if segments.is_empty() {
		errors.push(source.span(), "an item must declare at least one segment");
	}

	let format = match (&attrs.format, opts.format_generic) {
		(Some(t), false) => Format::Pinned(t.clone()),
		(None, true) => Format::Generic,
		(None, false) => Format::Default,
		(Some(t), true) => {
			errors.push(
				t.span(),
				"`format_generic` and `#[format(..)]` are mutually exclusive: a format-generic \
				 encoder cannot also pin one format",
			);
			Format::Generic
		}
	};

	// Derived prefixes inherit their entry's format by construction. That is what
	// makes it impossible for a prefix and its key to disagree on encoding (R6).
	let prefixes = cuts
		.into_iter()
		.map(|cut| {
			let mut pemit: Vec<Emit> = emit[..cut.emit].to_vec();
			if cut.open
				&& let Some(Emit::ListTerminated(i)) = pemit.pop()
			{
				pemit.push(Emit::ListOpen(i));
			}
			let pfields: Vec<FieldDef> = fields[..cut.fields].to_vec();
			let lifetime = pfields.iter().any(|f| f.borrowed);
			let derives = derive_set(opts);
			let builder = format_ident!("{}", snake(&cut.name));
			TypeDef {
				name: cut.name,
				role: Role::Prefix,
				attrs: attrs.passthrough.clone(),
				fields: pfields,
				emit: pemit,
				syms: syms[..cut.syms].to_vec(),
				value: None,
				ctx: None,
				also_range: true,
				format: format.clone(),
				derives,
				lifetime,
				source: source.clone(),
				route: cut.route,
				extends_name: None,
				extends: None,
				ignore_trailing: false,
				parent_root: scope.root.clone(),
				extended_by_keys: true,
				leaf_level: false,
				builder,
				ancestors: scope.ancestors.clone(),
				literals: literals.clone(),
			}
		})
		.collect();

	let lifetime = fields.iter().any(|f| f.borrowed);
	let derives = derive_set(opts);
	let builder = format_ident!("{}", snake(&name));
	let def = TypeDef {
		name,
		role,
		attrs: attrs.passthrough.clone(),
		fields,
		emit,
		syms,
		value,
		ctx: opts.ctx.clone(),
		also_range: opts.also_range || role == Role::Root,
		format,
		derives,
		lifetime,
		source: source.clone(),
		route,
		extends_name: None,
		extends: None,
		ignore_trailing: opts.ignore_trailing,
		parent_root: scope.root.clone(),
		extended_by_keys: false,
		leaf_level: false,
		builder,
		ancestors: scope.ancestors.clone(),
		literals,
	};

	(def, prefixes)
}

/// The derives every generated type carries, plus and minus the schema's
/// adjustments. `Clone` and `Debug` are load-bearing: `KVKey` requires `Debug`,
/// and callers clone keys to outlive a scan buffer.
fn derive_set(opts: &Opts) -> Vec<Ident> {
	let mut set: Vec<Ident> = ["Clone", "Debug", "PartialEq", "PartialOrd", "Eq"]
		.iter()
		.map(|d| format_ident!("{d}"))
		.collect();

	// Which traits a field type actually implements is not visible here, so the
	// schema opts out explicitly rather than having it guessed from the spelling
	// of a type name.
	for d in &opts.derive_remove {
		set.retain(|x| x != d);
	}
	for d in &opts.derive_add {
		if !set.iter().any(|x| x == d) {
			set.push(d.clone());
		}
	}
	set
}

fn link_extensions(model: &mut Model, errors: &mut Errors) {
	let index: BTreeMap<String, usize> = model
		.types
		.iter()
		.enumerate()
		.filter(|(_, t)| t.role == Role::Key)
		.map(|(i, t)| (t.source.to_string(), i))
		.collect();

	let links: Vec<(usize, Ident)> = model
		.types
		.iter()
		.enumerate()
		.filter_map(|(i, t)| t.extends_name.clone().map(|n| (i, n)))
		.collect();

	for (i, base) in links {
		match index.get(&base.to_string()) {
			Some(&target) if target != i => {
				model.types[i].extends = Some(target);
			}
			Some(_) => errors.push(base.span(), "an entry cannot extend itself"),
			None => errors.push(
				base.span(),
				format!("`{base}` is not a declared entry, so it cannot be extended"),
			),
		}
	}
}

/// Marks every prefix that some stored key's layout continues past. That is what
/// decides `next` versus `next_neighbour` for an inclusive upper bound: when
/// stored keys extend the bound, the bound is only their prefix and `next` would
/// exclude them.
fn mark_extended(model: &mut Model) {
	let key_syms: Vec<Vec<Sym>> =
		model.types.iter().filter(|t| t.is_addressable()).map(|t| t.syms.clone()).collect();

	// Applies to stored keys as well as bounds: a key that another key extends
	// needs an upper bound covering the whole run, not just itself.
	for def in &mut model.types {
		def.extended_by_keys = key_syms.iter().any(|k| extends_bytes(k, &def.syms));
	}
}

/// Whether `long` addresses bytes that continue past all of `short`'s.
///
/// The comparison is over bytes, not over symbols. Adjacent literals are merged
/// into one run, so a layout that continues *inside* the run `short` ends with is
/// still an extension of it — comparing runs for equality would miss exactly the
/// cases [`crate::check::diverge`] classifies as an extension, and the two must
/// agree or a bound gets built with the wrong successor.
pub fn extends_bytes(long: &[Sym], short: &[Sym]) -> bool {
	let Some((tail, head)) = short.split_last() else {
		return !long.is_empty();
	};
	if long.len() < short.len() || long[..head.len()] != *head {
		return false;
	}
	match (&long[head.len()], tail) {
		// The boundary run: either `short` stops part-way through `long`'s run, or
		// the runs match exactly and `long` has more symbols after it.
		(Sym::Lit(l), Sym::Lit(s)) => {
			(l.len() > s.len() && l.starts_with(s)) || (l == s && long.len() > short.len())
		}
		(l, s) => l == s && long.len() > short.len(),
	}
}

fn collect_waivers(item: &Item, out: &mut Vec<Waiver>) {
	let (attrs, ident, children) = match item {
		Item::Level(l) => (&l.attrs, &l.ident, Some(&l.items)),
		Item::Entry(e) => (&e.attrs, &e.ident, None),
	};
	for w in &attrs.waives {
		out.push(Waiver {
			from: ident.clone(),
			to: w.overlaps.clone(),
			at: w.at.clone(),
			tracked: w.tracked.clone(),
			span: w.span,
			used: Cell::new(false),
		});
	}
	for child in children.into_iter().flatten() {
		collect_waivers(child, out);
	}
}

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

/// Appends literal bytes, merging into the previous run so the checker sees one
/// contiguous literal per branch point however the schema spelled it: `"!ns"`
/// and `"!", "ns"` must compare identically.
fn push_lit(syms: &mut Vec<Sym>, bytes: &[u8]) {
	if let Some(Sym::Lit(prev)) = syms.last_mut() {
		prev.extend_from_slice(bytes);
	} else {
		syms.push(Sym::Lit(bytes.to_vec()));
	}
}

/// Renders literal bytes for the keyspace map, printable bytes verbatim.
pub fn render_lit(bytes: &[u8]) -> String {
	let mut out = String::new();
	for b in bytes {
		if b.is_ascii_graphic() {
			out.push(*b as char);
		} else {
			out.push_str(&format!("\\x{b:02x}"));
		}
	}
	out
}

/// A type as one line of text, for comparison and for diagnostics.
pub fn render_type(ty: &Type) -> String {
	quote!(#ty).to_string().replace(' ', "")
}

/// The `AnyValue` variant name a value type generates: its last path segment with
/// anything that cannot appear in an ident removed.
///
/// Lossy on purpose — the variant is what a caller matches on, so it reads as a
/// name rather than a path. R15 rejects a schema where two value types collapse to
/// the same one.
pub fn value_variant_name(rendered: &str) -> Ident {
	let last = rendered.rsplit("::").next().unwrap_or(rendered);
	let cleaned: String = last.chars().filter(|c| c.is_alphanumeric()).collect();
	let mut chars = cleaned.chars();
	match chars.next() {
		None => format_ident!("Unit"),
		Some(head) => format_ident!("{}{}", head.to_ascii_uppercase(), chars.as_str()),
	}
}

// ---------------------------------------------------------------------------
// Type helpers
// ---------------------------------------------------------------------------

fn is_bare_ident(ty: &Type) -> bool {
	matches!(ty, Type::Path(p) if p.qself.is_none() && p.path.segments.len() == 1
		&& p.path.segments[0].arguments.is_empty())
}

/// Strips `Cow<'_, T>` wrappers so the width lookup lands on the inner type.
fn strip_cow(ty: &Type) -> Type {
	if let Type::Path(p) = ty
		&& let Some(last) = p.path.segments.last()
		&& last.ident == "Cow"
		&& let syn::PathArguments::AngleBracketed(args) = &last.arguments
	{
		for arg in &args.args {
			if let syn::GenericArgument::Type(inner) = arg {
				return strip_cow(inner);
			}
		}
	}
	ty.clone()
}

/// Lookup key for the type registry: the final path segment, or a synthetic name
/// for the shapes a schema may spell inline.
fn type_key(ty: &Type) -> Option<String> {
	match ty {
		Type::Path(p) => p.path.segments.last().map(|s| s.ident.to_string()),
		Type::Slice(s) => type_key(&s.elem).map(|inner| format!("[{inner}]")),
		Type::Array(a) => type_key(&a.elem).map(|inner| format!("[{inner};N]")),
		Type::Tuple(t) if t.elems.is_empty() => Some("()".to_owned()),
		_ => None,
	}
}

/// Whether a written type borrows, i.e. mentions the reserved schema lifetime.
fn mentions_key_lifetime(ty: &Type) -> bool {
	fn scan(tokens: TokenStream) -> bool {
		let mut iter = tokens.into_iter().peekable();
		while let Some(token) = iter.next() {
			match token {
				TokenTree::Group(g) if scan(g.stream()) => {
					return true;
				}
				TokenTree::Punct(p) if p.as_char() == '\'' => {
					if matches!(iter.peek(), Some(TokenTree::Ident(id)) if id == KEY_LIFETIME) {
						return true;
					}
				}
				_ => {}
			}
		}
		false
	}
	scan(ty.to_token_stream())
}

/// Replaces the reserved schema lifetime with the generated struct lifetime.
///
/// Done on tokens rather than on the rendered text, so a longer lifetime that
/// merely starts with the same letter is left alone.
fn substitute_key_lifetime(ty: &Type) -> TokenStream {
	fn rewrite(tokens: TokenStream) -> TokenStream {
		let mut out = TokenStream::new();
		let mut iter = tokens.into_iter().peekable();
		while let Some(token) = iter.next() {
			match token {
				TokenTree::Group(g) => {
					let inner = rewrite(g.stream());
					let mut replacement = Group::new(g.delimiter(), inner);
					replacement.set_span(g.span());
					out.extend([TokenTree::Group(replacement)]);
				}
				TokenTree::Punct(p) if p.as_char() == '\'' => {
					let is_key =
						matches!(iter.peek(), Some(TokenTree::Ident(id)) if id == KEY_LIFETIME);
					if is_key {
						iter.next();
						out.extend(quote!('a));
					} else {
						out.extend([TokenTree::Punct(p)]);
					}
				}
				other => out.extend([other]),
			}
		}
		out
	}
	rewrite(ty.to_token_stream())
}

pub fn camel(ident: &Ident) -> String {
	let s = ident.to_string();
	let mut out = String::with_capacity(s.len());
	let mut upper = true;
	for c in s.chars() {
		if c == '_' {
			upper = true;
		} else if upper {
			out.extend(c.to_uppercase());
			upper = false;
		} else {
			out.push(c);
		}
	}
	out
}

pub fn snake(ident: &Ident) -> String {
	let s = ident.to_string();
	let mut out = String::with_capacity(s.len() + 4);
	for (i, c) in s.chars().enumerate() {
		if c.is_uppercase() {
			if i != 0 {
				out.push('_');
			}
			out.extend(c.to_lowercase());
		} else {
			out.push(c);
		}
	}
	out
}
