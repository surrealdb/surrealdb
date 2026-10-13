//! Validation layer: the rules that make a keyspace impossible to get wrong.
//!
//! Every rule runs against the resolved [`Model`], never against tokens, so each
//! one is a plain function over data and is unit-tested directly. Diagnostics are
//! accumulated rather than returned early, so one compile reports every problem,
//! and every conflict is reported twice: once on each of the two declarations
//! involved.
//!
//! # Why pairwise symbol comparison is sound
//!
//! Every `storekey` field encoding is self-delimiting given its static type:
//! fixed-width types occupy a known number of bytes, and variable-width types
//! carry their own end — usually by escaping bytes `<= 0x01` and terminating with
//! `0x00`, and otherwise by a leading discriminant that fixes what follows (an
//! `Option` is delimited this way). So the byte boundaries
//! between a key's fields are recoverable from the layout alone, and two keys can
//! be compared symbol by symbol rather than byte offset by byte offset. Comparing
//! byte offsets would be wrong: a variable-width field shifts everything after
//! it.
//!
//! That argument rests on the declared widths being the real ones. Nothing here
//! can verify it — the widths come from the `types` prelude, and the encoders
//! belong to types this crate never sees — so the generated selftests check each
//! declared width against the actual codec at run time.
//!
//! The rules, and what each one prevents:
//!
//! - **R1** duplicate names, duplicate schema idents, identical layouts.
//! - **R2** sibling literal tags must be mutually byte-prefix-free, so no tag can shadow a longer
//!   one. Applies to whole subtrees as well as to keys: two levels claiming the same bytes make a
//!   subtree delete reach into the other's data even when every key is disjoint.
//! - **R3** a literal must never meet a bare field at a branch point. This is the rule that catches
//!   a subspace tag colliding with user-controlled names.
//! - **R4** literal tag bytes must be `>= 0x02`, keeping them distinct from `storekey`'s terminator
//!   (`0x00`) and escape (`0x01`).
//! - **R5** every field type must be declared, and floats are rejected outright — in an alias
//!   expansion as well as where written. Enforced during resolution, where types are looked up.
//! - **R6** a derived prefix always shares its entry's format. Structural: the prefix is built from
//!   the entry, and this rule re-verifies it.
//! - **R7** one key's bytes may be a strict prefix of another's only when the longer one declares
//!   `extends`.
//! - **R8** every type used to build a bound must begin with a literal byte other than `0xFF`, so
//!   the exclusive upper bound always exists and the generated bounds cannot panic.
//! - **R9** a waiver that matches nothing is an error, so waivers cannot outlive the collision they
//!   document. A waiver covers one named collision at one named position, not everything two
//!   subtrees might ever do to each other.
//! - **R10** conflicts are reported across all `cfg` states, so a conditionally compiled family
//!   cannot collide with the keys it coexists with.
//! - **R11** `ctx` is only meaningful on a stored key; the `()`-context case is left to the
//!   compiler, with a diagnostic attached by the generated code.
//! - **R12** an enum's discriminants must be distinct, or two logically different keys encode to
//!   the same bytes and one decodes back as the other.
//! - **R13** a level root whose subtree holds keys of another `storekey` format must be
//!   `format_generic`, which is what makes the compiler prove its own bytes are the same under
//!   every format and so that its range really does cover that subtree.
//! - **R14** every inherited field of a key must exist on the level root that builds it, so a
//!   builder can never invent the data identifying a parent.
//! - **R15** value types must have distinct generated variant names, so the reverse decoder's enum
//!   is a compile error's worth of ambiguity short of no ambiguity at all.

use std::collections::BTreeMap;

use proc_macro2::Ident;

use crate::model::{Errors, Format, Model, Role, Sym, TypeDef, Width, render_lit};

/// The result of walking two layouts in lockstep.
#[derive(Debug, PartialEq, Eq)]
pub enum Divergence {
	/// The layouts separate on differing literal bytes: provably disjoint.
	Disjoint,
	/// Identical layouts: the same bytes address both.
	Identical,
	/// One layout's bytes are a strict prefix of the other's.
	Extension {
		/// Which of the two arguments is the shorter, prefix side.
		shorter: Side,
	},
	/// A literal met a bare field, so a value of that field can spell the tag.
	LiteralMeetsField {
		literal: Vec<u8>,
	},
	/// Two different fields occupy the same position, so neither the reader nor
	/// the writer can tell which layout applies.
	FieldMeetsField {
		left: String,
		right: String,
	},
}

/// Which argument of [`diverge`] a result refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
	Left,
	Right,
}

/// A cursor over a symbol sequence that can stop part-way through a literal run,
/// which is what lets the walk compare `"!i"` against `"!ix"` correctly.
struct Cursor<'a> {
	syms: &'a [Sym],
	index: usize,
	offset: usize,
}

enum Peek<'a> {
	Lit(&'a [u8]),
	Field(&'a str, Width),
}

impl<'a> Cursor<'a> {
	fn new(syms: &'a [Sym]) -> Self {
		Cursor {
			syms,
			index: 0,
			offset: 0,
		}
	}

	fn peek(&self) -> Option<Peek<'a>> {
		match self.syms.get(self.index)? {
			Sym::Lit(bytes) => Some(Peek::Lit(&bytes[self.offset..])),
			Sym::Field {
				name,
				width,
			} => Some(Peek::Field(name, *width)),
		}
	}

	/// Consumes `n` bytes of the current literal run, stepping to the next symbol
	/// when the run is exhausted.
	fn take_lit(&mut self, n: usize) {
		self.offset += n;
		if let Some(Sym::Lit(bytes)) = self.syms.get(self.index)
			&& self.offset >= bytes.len()
		{
			self.index += 1;
			self.offset = 0;
		}
	}

	fn take_field(&mut self) {
		self.index += 1;
		self.offset = 0;
	}
}

/// Walks two layouts in lockstep and classifies where they part company.
pub fn diverge(a: &[Sym], b: &[Sym]) -> Divergence {
	let mut left = Cursor::new(a);
	let mut right = Cursor::new(b);

	loop {
		match (left.peek(), right.peek()) {
			(None, None) => return Divergence::Identical,
			(None, Some(_)) => {
				return Divergence::Extension {
					shorter: Side::Left,
				};
			}
			(Some(_), None) => {
				return Divergence::Extension {
					shorter: Side::Right,
				};
			}
			(Some(Peek::Lit(la)), Some(Peek::Lit(lb))) => {
				let shared = la.iter().zip(lb).take_while(|(x, y)| x == y).count();
				if shared < la.len() && shared < lb.len() {
					return Divergence::Disjoint;
				}
				left.take_lit(shared);
				right.take_lit(shared);
			}
			(Some(Peek::Lit(la)), Some(Peek::Field(..))) => {
				return Divergence::LiteralMeetsField {
					literal: la.to_vec(),
				};
			}
			(Some(Peek::Field(..)), Some(Peek::Lit(lb))) => {
				return Divergence::LiteralMeetsField {
					literal: lb.to_vec(),
				};
			}
			(Some(Peek::Field(na, wa)), Some(Peek::Field(nb, wb))) => {
				if na == nb && wa == wb {
					left.take_field();
					right.take_field();
				} else {
					return Divergence::FieldMeetsField {
						left: na.to_owned(),
						right: nb.to_owned(),
					};
				}
			}
		}
	}
}

pub fn check(model: &Model, errors: &mut Errors) {
	non_empty(model, errors);
	unique_names(model, errors);
	value_variant_names(model, errors);
	tag_bytes(model, errors);
	enum_discriminants(model, errors);
	prefix_formats(model, errors);
	subtree_formats(model, errors);
	range_bounds(model, errors);
	context_placement(model, errors);
	builder_fields(model, errors);
	pairwise(model, errors);
	regions(model, errors);
	unused_waivers(model, errors);
}

/// A schema that declares no stored key generates nothing useful, and silently
/// expanding to an empty module hides a mistake such as a misplaced brace.
fn non_empty(model: &Model, errors: &mut Errors) {
	if !model.types.iter().any(|t| t.is_addressable()) {
		errors.push(
			proc_macro2::Span::call_site(),
			"this keyspace declares no stored keys; every entry needs a `=> ValueType` binding",
		);
	}
}

/// R1: generated names and schema idents must both be unique. Names are derived,
/// so a collision means two schema idents that differ only in separators, or a
/// prefix name that shadows an entry name.
fn unique_names(model: &Model, errors: &mut Errors) {
	let mut by_name: BTreeMap<String, &TypeDef> = BTreeMap::new();
	for def in &model.types {
		let name = def.name.to_string();
		if let Some(first) = by_name.get(&name) {
			errors.push(
				def.name.span(),
				format!(
					"generated type `{name}` is declared twice: `{}` and `{}` derive the same \
					 name; rename one of the schema entries",
					first.source, def.source
				),
			);
			errors.push(first.name.span(), format!("`{name}` first generated here"));
		} else {
			by_name.insert(name, def);
		}
	}

	let mut sources: BTreeMap<String, &Ident> = BTreeMap::new();
	for def in model.types.iter().filter(|d| d.role != Role::Prefix) {
		let key = def.source.to_string();
		if let Some(first) = sources.get(&key) {
			errors.push(def.source.span(), format!("`{key}` is declared twice in the keyspace"));
			errors.push(first.span(), format!("`{key}` first declared here"));
		} else {
			sources.insert(key, &def.source);
		}
	}
}

/// R15: two value types must not derive the same `AnyValue` variant name.
///
/// The variant name is the cleaned last path segment, so two types with the same
/// leaf name in different modules mint two variants called the same thing. That is
/// an `E0428` on generated tokens, which points at the invocation rather than at
/// either `=>`; reporting it here names both entries.
fn value_variant_names(model: &Model, errors: &mut Errors) {
	let mut by_variant: BTreeMap<String, (String, &TypeDef)> = BTreeMap::new();
	for def in model.types.iter().filter(|d| d.is_decodable()) {
		let Some(value) = &def.value else {
			continue;
		};
		let rendered = crate::model::render_type(value);
		let variant = crate::model::value_variant_name(&rendered).to_string();
		match by_variant.get(&variant) {
			// The same type used twice shares one variant, which is the point.
			Some((first, _)) if *first == rendered => {}
			Some((first, other)) => {
				report_pair(
					errors,
					other,
					def,
					format!(
						"value types `{first}` and `{rendered}` both generate the `AnyValue` variant \
						 `{variant}`; give one of them a distinct leaf type name"
					),
				);
			}
			None => {
				by_variant.insert(variant, (rendered, def));
			}
		}
	}
}

/// R12: an enum's discriminants must be distinct.
///
/// Two variants sharing a discriminant encode to the same bytes, so two logically
/// distinct keys claim the same bytes and every key holding the later variant
/// decodes back as the earlier one. `rustc` only warns about the unreachable
/// decode arm, which is not enough for something this quiet.
fn enum_discriminants(model: &Model, errors: &mut Errors) {
	for e in &model.enums {
		let mut seen: BTreeMap<String, &Ident> = BTreeMap::new();
		for (name, disc) in &e.variants {
			let value = disc.base10_digits().to_owned();
			match seen.get(&value) {
				Some(first) => {
					errors.push(
						disc.span(),
						format!(
							"`{}::{name}` and `{}::{first}` both encode as `{value}`, so keys \
							 holding either one are indistinguishable and `{name}` decodes back as \
							 `{first}`",
							e.ident, e.ident
						),
					);
					errors.push(first.span(), format!("`{first}` already uses `{value}`"));
				}
				None => {
					seen.insert(value, name);
				}
			}
		}
	}
}

/// R4: literal tag bytes must be `>= 0x02` so they can never be mistaken for
/// `storekey`'s terminator (`0x00`) or escape (`0x01`) byte, which is what keeps
/// the symbolic comparison in [`diverge`] equivalent to comparing raw bytes.
fn tag_bytes(model: &Model, errors: &mut Errors) {
	for def in model.types.iter().filter(|d| d.role != Role::Prefix) {
		for (bytes, span) in &def.literals {
			if let Some(bad) = bytes.iter().find(|b| **b < 0x02) {
				errors.push(
					*span,
					format!(
						"literal byte `0x{bad:02x}` cannot be used as a tag; tag bytes must be \
						 `>= 0x02` so they can never be confused with storekey's terminator \
						 (0x00) or escape (0x01) byte"
					),
				);
			}
		}
	}
}

/// R6: a derived prefix must encode under the same format as the entry it was cut
/// from, otherwise a scan bound and the keys it is meant to bound could disagree
/// on how a field is encoded. Prefixes are built from their entry, so this rule
/// verifies a structural property rather than a schema-authored one.
fn prefix_formats(model: &Model, errors: &mut Errors) {
	let by_source: BTreeMap<String, &Format> = model
		.types
		.iter()
		.filter(|d| d.role != Role::Prefix)
		.map(|d| (d.source.to_string(), &d.format))
		.collect();

	for def in model.types.iter().filter(|d| d.role == Role::Prefix) {
		let Some(entry) = by_source.get(&def.source.to_string()) else {
			continue;
		};
		if entry.describe() != def.format.describe() {
			errors.push(
				def.name.span(),
				format!(
					"prefix `{}` encodes under format `{}` but its entry `{}` uses `{}`; a scan \
					 bound must share its entry's format",
					def.name,
					def.format.describe(),
					def.source,
					entry.describe()
				),
			);
		}
	}
}

/// R13: a level root whose subtree holds keys of a different `storekey` format
/// must be `format_generic`.
///
/// A root's range is only a bound on its subtree if the root's own bytes are the
/// prefix those keys actually encode. When a descendant pins another format, that
/// holds exactly when every field in the root's own layout encodes the same under
/// both — which this crate cannot see, but the compiler can: `format_generic`
/// emits `impl<F> Encode<F>`, which only compiles when each field is itself
/// format-generic. R6 covers the derived-prefix case, where the format is
/// inherited by construction; this covers the inheritance that crosses a level.
fn subtree_formats(model: &Model, errors: &mut Errors) {
	for root in model.types.iter().filter(|d| d.role == Role::Root) {
		if matches!(root.format, Format::Generic) {
			continue;
		}
		for def in model.types.iter().filter(|d| d.role != Role::Prefix && nested(root, d)) {
			if def.format.describe() == root.format.describe() {
				continue;
			}
			errors.push(
				def.source.span(),
				format!(
					"`{}` encodes under format `{}` but its enclosing level `{}` uses `{}`, so \
					 `{}`'s range is not guaranteed to cover this key; declare `{}` as \
					 `(format_generic)` so its own bytes are the same under either format",
					def.source,
					def.format.describe(),
					root.source,
					root.format.describe(),
					root.name,
					root.source,
				),
			);
			errors.push(root.source.span(), format!("`{}` is declared here", root.source));
		}
	}
}

/// R14: a key's inherited fields must all exist on the level root that builds it.
///
/// The generated builder fills inherited fields from the root it is called on, so
/// a field the root does not have would have to be invented. A key with an
/// invented `ns` or `tb` is well formed and addresses another tenant's subspace,
/// which is the one failure mode no amount of downstream care catches.
fn builder_fields(model: &Model, errors: &mut Errors) {
	for def in &model.types {
		let Some(root_name) = &def.parent_root else {
			continue;
		};
		if def.name == *root_name {
			continue;
		}
		let Some(root) = model.types.iter().find(|t| t.name == *root_name) else {
			continue;
		};
		for field in def.fields.iter().filter(|f| f.inherited) {
			if root.fields.iter().any(|rf| rf.name == field.name) {
				continue;
			}
			errors.push(
				def.source.span(),
				format!(
					"`{}` inherits field `{}`, but the level it hangs off (`{}`) does not carry it, \
					 so a builder would have to invent the data identifying a parent",
					def.source, field.name, root.source
				),
			);
		}
	}
}

/// R8: every type used to build a bound must start with a literal byte other than
/// `0xFF`. The exclusive upper bound of a prefix range is that prefix with its
/// last non-`0xFF` byte incremented, which exists only when some byte is not
/// `0xFF`. Guaranteeing it here is what lets the generated bounds be infallible
/// instead of returning an `Option` or panicking.
///
/// Covers stored keys that other keys extend as well as declared bounds: those get
/// `skip_extensions` and `range_subtree`, which are built the same way.
fn range_bounds(model: &Model, errors: &mut Errors) {
	for def in
		model.types.iter().filter(|d| d.also_range || (d.extended_by_keys && d.is_decodable()))
	{
		match def.syms.first() {
			None => errors.push(
				def.name.span(),
				format!(
					"`{}` needs an exclusive upper bound but encodes to no bytes; an empty prefix \
					 has no successor",
					def.name
				),
			),
			Some(Sym::Field {
				name,
				..
			}) => errors.push(
				def.name.span(),
				format!(
					"`{}` needs an exclusive upper bound but starts with field `{name}`; it must \
					 start with a literal tag so the successor is known to exist",
					def.name
				),
			),
			Some(Sym::Lit(bytes)) => {
				if bytes.first() == Some(&0xFF) {
					errors.push(
						def.name.span(),
						format!(
							"`{}` starts with byte `0xff`, which has no successor, so its exclusive \
							 upper bound cannot be built",
							def.name
						),
					);
				}
			}
		}
	}
}

/// R11: `ctx` supplies `KVValue::KeyContext` from the key, so it only makes sense
/// on a stored key. Whether a value *requires* one is a property of its
/// `KVValue` impl, which the macro cannot see; the generated `value_context` body
/// carries a `#[diagnostic::on_unimplemented]` bound that names the fix instead.
fn context_placement(model: &Model, errors: &mut Errors) {
	for def in model.types.iter().filter(|d| d.role == Role::Key && d.value.is_none()) {
		errors.push(
			def.source.span(),
			format!(
				"entry `{}` binds no value type; add `=> ValueType` so the key addresses \
				 something, or declare it as a level if it is only a subspace",
				def.source
			),
		);
	}
	for def in &model.types {
		if def.ctx.is_some() && def.value.is_none() {
			errors.push(
				def.name.span(),
				format!(
					"`{}` sets `ctx` but stores no value; `ctx` only applies to entries with a \
					 value type",
					def.source
				),
			);
		}
	}
}

/// R2, R3, R7, R10: compares every pair of addressable layouts.
///
/// Prefixes are excluded: they name a byte range rather than an entry, so being a
/// prefix of a key is their purpose. Pairs are compared regardless of `cfg`, the
/// conservative choice: a conditionally compiled family must not collide with the
/// keys it coexists with when that `cfg` is on (R10).
fn pairwise(model: &Model, errors: &mut Errors) {
	let addressable: Vec<&TypeDef> = model.types.iter().filter(|d| d.is_addressable()).collect();

	for (i, a) in addressable.iter().enumerate() {
		for b in &addressable[i + 1..] {
			match diverge(&a.syms, &b.syms) {
				Divergence::Disjoint => {}
				Divergence::Identical => {
					report_pair(
						errors,
						a,
						b,
						format!(
							"`{}` and `{}` encode to the same bytes, so a stored entry cannot be \
							 attributed to either",
							a.source, b.source
						),
					);
				}
				Divergence::Extension {
					shorter,
				} => {
					let (short, long) = match shorter {
						Side::Left => (*a, *b),
						Side::Right => (*b, *a),
					};
					extension(model, short, long, errors);
				}
				Divergence::LiteralMeetsField {
					literal,
				} => {
					if waived(model, a, b, &literal) {
						continue;
					}
					report_pair(
						errors,
						a,
						b,
						format!(
							"`{}` and `{}` overlap: the tag `{}` sits where the other key has a \
							 field, so a field value spelling those bytes lands inside the wrong \
							 subspace; give one of them a distinct tag, or record the collision with \
							 `#[waive(overlaps = {}, at = \"{}\", tracked = \"..\")]`",
							a.source,
							b.source,
							render_lit(&literal),
							a.source,
							render_lit(&literal),
						),
					);
				}
				// Not waivable: unlike a tag landing in a field's value space, this is
				// two layouts that are indistinguishable everywhere rather than in one
				// band, so there is no position a waiver could name.
				Divergence::FieldMeetsField {
					left,
					right,
				} => {
					report_pair(
						errors,
						a,
						b,
						format!(
							"`{}` and `{}` branch on fields `{left}` and `{right}` at the same \
							 position with nothing to tell them apart; insert a distinct literal \
							 tag before one of them",
							a.source, b.source
						),
					);
				}
			}
		}
	}
}

/// R2, R3, R10 for whole subtrees: two level roots must not claim overlapping
/// bytes.
///
/// [`pairwise`] compares things that address an entry, which a level root with
/// children does not. But a root names the byte range a subtree scan or a subtree
/// delete covers, and two overlapping roots make one of those reach into the
/// other's data however carefully the individual keys were laid out — disjoint
/// keys are not disjoint regions. Nesting is the one legitimate overlap: an inner
/// level is inside its enclosing one by construction.
fn regions(model: &Model, errors: &mut Errors) {
	let roots: Vec<&TypeDef> = model.types.iter().filter(|d| d.role == Role::Root).collect();

	for (i, a) in roots.iter().enumerate() {
		for b in &roots[i + 1..] {
			// Already compared as entries, and a childless level is addressable.
			if a.is_addressable() && b.is_addressable() {
				continue;
			}
			if nested(a, b) || nested(b, a) {
				continue;
			}
			if diverge(&a.syms, &b.syms) == Divergence::Disjoint {
				continue;
			}
			report_pair(
				errors,
				a,
				b,
				format!(
					"the `{}` and `{}` subtrees overlap, so a range over one covers keys belonging \
					 to the other; give one of them a distinct tag",
					a.source, b.source
				),
			);
		}
	}
}

/// Whether `inner` is declared inside `outer`.
fn nested(outer: &TypeDef, inner: &TypeDef) -> bool {
	inner.ancestors.contains(&outer.source)
}

/// R7: one key's bytes may be a strict prefix of another's only by declaration.
/// The longer key must say `extends`, which is also what makes a single decoder
/// able to read both.
///
/// Whether a bare base is itself valid needs no declaration: it is valid exactly
/// when the base is separately declared a stored key, which the decoder already
/// knows.
fn extension(model: &Model, shorter: &TypeDef, longer: &TypeDef, errors: &mut Errors) {
	if !declares(model, longer, shorter) {
		report_pair(
			errors,
			shorter,
			longer,
			format!(
				"`{}`'s bytes are a strict prefix of `{}`'s, so a scan cannot tell where one ends \
				 and the other begins; declare it with `{} = {} + [..]`",
				shorter.source, longer.source, longer.source, shorter.source
			),
		);
	}
}

/// Whether `longer` reaches `shorter` through its declared extension chain.
///
/// A family may nest: each entry names only its immediate base, so a
/// prefix relationship two or more links away is still declared. The walk is
/// bounded by the number of types, so a cycle cannot spin here.
fn declares(model: &Model, longer: &TypeDef, shorter: &TypeDef) -> bool {
	let mut current = longer;
	for _ in 0..model.types.len() {
		let Some(base) = current.extends.and_then(|i| model.types.get(i)) else {
			return false;
		};
		if base.source == shorter.source {
			return true;
		}
		current = base;
	}
	false
}

/// R9: a waiver must match a real ambiguity. Waivers document a collision that
/// already exists on disk; once the collision is gone the waiver must go too, or
/// it silently licenses a future one.
fn unused_waivers(model: &Model, errors: &mut Errors) {
	for waiver in &model.waivers {
		if !waiver.used.get() {
			errors.push(
				waiver.span,
				format!(
					"`{}` waives an overlap with `{}` at `{}`, and there is no such overlap; remove \
					 the waiver",
					waiver.from,
					waiver.to,
					render_lit(&waiver.at)
				),
			);
		}
	}
}

/// Whether a declared waiver covers this collision.
///
/// A waiver may name a level, in which case it reaches every key beneath it,
/// because a tag colliding with a level's field space collides with all of that
/// level's contents. What keeps that from being a blanket licence over two
/// subtrees is the position: only a collision at the bytes the waiver names is
/// silenced, so a *different* collision between the same two declarations still
/// reports, and the waiver still goes stale when the one it documents is fixed.
fn waived(model: &Model, a: &TypeDef, b: &TypeDef, at: &[u8]) -> bool {
	let mut found = false;
	for waiver in &model.waivers {
		if waiver.at != at {
			continue;
		}
		let forward = names(a, &waiver.from) && names(b, &waiver.to);
		let backward = names(b, &waiver.from) && names(a, &waiver.to);
		if forward || backward {
			waiver.used.set(true);
			found = true;
		}
	}
	found
}

fn names(def: &TypeDef, ident: &Ident) -> bool {
	def.source == *ident || def.ancestors.iter().any(|a| a == ident)
}

/// Reports a conflict on both declarations, so the error names the pair rather
/// than leaving the reader to find the other half.
fn report_pair(errors: &mut Errors, a: &TypeDef, b: &TypeDef, message: String) {
	errors.push(b.source.span(), message);
	errors.push(a.source.span(), format!("`{}` is declared here", a.source));
}
