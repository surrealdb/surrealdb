//! The attribute table of a derived type, parsed and checked.
//!
//! Every refusal here is a compile error with a span on the offending field, variant or attribute,
//! so a schema mistake never reaches a build.

use std::collections::BTreeSet;

use proc_macro2::Span;
use syn::ext::IdentExt;
use syn::spanned::Spanned;
use syn::{
	Attribute, Data, DataEnum, DeriveInput, Error, Expr, ExprArray, ExprLit, Fields,
	GenericArgument, Ident, Lit, LitInt, LitStr, Member, Path, PathArguments, Result, Token, Type,
	Variant as SynVariant, parse_quote,
};

/// A release as the derive sees it: `major.minor`, ordered like the runtime's `Release`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Rel(pub u16, pub u16);

/// The release of every item that existed when the stored grammar shipped, and the lowest `since`
/// a schema may name.
pub(crate) const FIRST: Rel = Rel(4, 0);

impl Rel {
	fn parse(lit: &LitStr) -> Result<Self> {
		let text = lit.value();
		let bad = || Error::new(lit.span(), "`since` is a release of the form \"major.minor\"");
		let (major, minor) = text.split_once('.').ok_or_else(bad)?;
		let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
		if !digits(major) || !digits(minor) {
			return Err(bad());
		}
		let rel = Rel(major.parse().map_err(|_| bad())?, minor.parse().map_err(|_| bad())?);
		if rel < FIRST {
			return Err(Error::new(
				lit.span(),
				"`since` is below 4.0, the first release with a stored schema",
			));
		}
		Ok(rel)
	}
}

/// The attributes of the type itself.
pub(crate) struct TypeAttrs {
	pub since: Rel,
	pub reserved: Vec<u32>,
	pub legacy: Option<Path>,
	pub revision_embedded: bool,
	pub floor_fixed: bool,
	pub transparent: bool,
	/// The `fn(&Self) -> Result<(), SchemaError>` every constructor of the type runs on the
	/// value it builds before returning it.
	pub validate: Option<Path>,
}

/// What the encoder does with a field's absence.
// A derive builds one model per type at compile time; the size of a syn node in a variant costs
// nothing, and depends on the syn features the build unifies.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Presence {
	/// Always written; absence is refused.
	Required,
	/// An `Option`: absent for `None`. Holds the type inside the `Option`.
	Opt(Type),
	/// Absent when the value equals what the path returns.
	Default(Path),
}

pub(crate) struct Supersedes {
	/// The number the field replaces, which holds the narrower type.
	pub old: u32,
	/// `fn(&Field) -> Option<Old>`: the narrow form of the value, when it has one.
	pub narrow: Path,
}

pub(crate) struct Field {
	pub member: Member,
	pub name: String,
	pub ty: Type,
	pub n: u32,
	pub since: Rel,
	pub ignorable: bool,
	pub independent: Option<String>,
	pub presence: Presence,
	/// The field is an `Unrevisioned<_>`, which the `revision` encoding leaves out.
	pub unrevisioned: bool,
	pub supersedes: Option<Supersedes>,
	pub span: Span,
}

impl Field {
	pub(crate) fn critical(&self) -> bool {
		!self.ignorable
	}
}

/// The fields of a struct or of a struct-like variant, with the numbers retired beside them.
pub(crate) struct FieldSet {
	pub fields: Vec<Field>,
	pub reserved: Vec<u32>,
}

impl FieldSet {
	/// Every number the set reads as a field: each field's, and each superseded one.
	pub(crate) fn known(&self) -> Vec<u32> {
		let mut out = Vec::new();
		for f in &self.fields {
			out.push(f.n);
			if let Some(s) = &f.supersedes {
				out.push(s.old);
			}
		}
		out.sort_unstable();
		out
	}
}

// A derive builds one model per type at compile time; the size of a syn node in a variant costs
// nothing, and depends on the syn features the build unifies.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Payload {
	Unit,
	/// One element, encoded as its item.
	Single(Type),
	/// Several elements, in a positional map: element `i` has number `2 * (i + 1)`.
	Positional(Vec<Type>),
	Fields(FieldSet),
}

pub(crate) struct Variant {
	pub ident: Ident,
	pub name: String,
	/// `None` for a legacy variant.
	pub n: Option<u32>,
	pub since: Rel,
	pub payload: Payload,
	pub span: Span,
}

// A derive builds one model per type at compile time; the size of a syn node in a variant costs
// nothing, and depends on the syn features the build unifies.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Body {
	Struct(FieldSet),
	/// A single-field tuple struct, encoded as its field.
	Transparent(Type),
	Enum {
		variants: Vec<Variant>,
		reserved: Vec<u32>,
	},
}

pub(crate) struct Model {
	pub ident: Ident,
	pub attrs: TypeAttrs,
	pub body: Body,
}

pub(crate) fn parse(input: &DeriveInput) -> Result<Model> {
	if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
		return Err(Error::new(input.generics.span(), "`Stored` does not support generic types"));
	}
	let attrs = type_attrs(&input.attrs, input.ident.span())?;
	let body = match &input.data {
		Data::Struct(data) => {
			if attrs.transparent {
				transparent(&data.fields, input.ident.span())?
			} else {
				match &data.fields {
					Fields::Named(named) => {
						Body::Struct(field_set(named.named.iter(), &attrs, attrs.reserved.clone())?)
					}
					Fields::Unit | Fields::Unnamed(_) => {
						return Err(Error::new(
							input.ident.span(),
							"`Stored` needs a struct with named fields, or `transparent` on a \
							 single-field tuple struct",
						));
					}
				}
			}
		}
		Data::Enum(data) => {
			if attrs.transparent {
				return Err(Error::new(input.ident.span(), "`transparent` applies to structs"));
			}
			enum_body(data, &attrs)?
		}
		Data::Union(_) => {
			return Err(Error::new(input.ident.span(), "`Stored` does not support unions"));
		}
	};
	Ok(Model {
		ident: input.ident.clone(),
		attrs,
		body,
	})
}

fn stored_attrs(attrs: &[Attribute]) -> impl Iterator<Item = &Attribute> {
	attrs.iter().filter(|a| a.path().is_ident("stored"))
}

fn number(lit: &LitInt) -> Result<u32> {
	let n: u64 = lit.base10_parse()?;
	match u32::try_from(n) {
		Ok(n) if n != 0 => Ok(n),
		_ => Err(Error::new(lit.span(), "a number is between 1 and u32::MAX")),
	}
}

fn numbers(array: &ExprArray) -> Result<Vec<u32>> {
	let mut seen = BTreeSet::new();
	let mut out = Vec::new();
	for e in &array.elems {
		let n = if let Expr::Lit(ExprLit {
			lit: Lit::Int(lit),
			..
		}) = e
		{
			number(lit)?
		} else {
			return Err(Error::new(e.span(), "a reserved number is an integer literal"));
		};
		if !seen.insert(n) {
			return Err(Error::new(e.span(), format!("the number {n} is reserved twice")));
		}
		out.push(n);
	}
	Ok(out)
}

fn type_attrs(attrs: &[Attribute], span: Span) -> Result<TypeAttrs> {
	let mut since = None;
	let mut reserved = Vec::new();
	let mut legacy = None;
	let (mut revision_embedded, mut floor_fixed, mut transparent) = (false, false, false);
	let mut validate = None;
	for attr in stored_attrs(attrs) {
		attr.parse_nested_meta(|meta| {
			let once = |taken: bool| {
				if taken {
					Err(meta.error("the attribute is given twice"))
				} else {
					Ok(())
				}
			};
			if meta.path.is_ident("since") {
				once(since.is_some())?;
				since = Some(Rel::parse(&meta.value()?.parse::<LitStr>()?)?);
			} else if meta.path.is_ident("reserved") {
				once(!reserved.is_empty())?;
				reserved = numbers(&meta.value()?.parse::<ExprArray>()?)?;
			} else if meta.path.is_ident("legacy") {
				once(legacy.is_some())?;
				legacy = Some(meta.value()?.parse::<LitStr>()?.parse::<Path>()?);
			} else if meta.path.is_ident("revision_embedded") {
				once(revision_embedded)?;
				revision_embedded = true;
			} else if meta.path.is_ident("floor_fixed") {
				once(floor_fixed)?;
				floor_fixed = true;
			} else if meta.path.is_ident("transparent") {
				once(transparent)?;
				transparent = true;
			} else if meta.path.is_ident("validate") {
				once(validate.is_some())?;
				validate = Some(if meta.input.peek(Token![=]) {
					meta.value()?.parse::<LitStr>()?.parse::<Path>()?
				} else {
					parse_quote!(Self::validate)
				});
			} else {
				return Err(meta.error("an unknown `stored` attribute on a type"));
			}
			Ok(())
		})?;
	}
	let since = since.ok_or_else(|| {
		Error::new(
			span,
			"a stored type declares the release it first shipped in: `since = \"4.0\"`",
		)
	})?;
	Ok(TypeAttrs {
		since,
		reserved,
		legacy,
		revision_embedded,
		floor_fixed,
		transparent,
		validate,
	})
}

fn transparent(fields: &Fields, span: Span) -> Result<Body> {
	match fields {
		Fields::Unnamed(f) if f.unnamed.len() == 1 => {
			let field = &f.unnamed[0];
			if stored_attrs(&field.attrs).next().is_some() {
				return Err(Error::new(
					field.span(),
					"the field of a `transparent` type has no attributes",
				));
			}
			Ok(Body::Transparent(field.ty.clone()))
		}
		Fields::Named(_) | Fields::Unnamed(_) | Fields::Unit => {
			Err(Error::new(span, "`transparent` needs a tuple struct with exactly one field"))
		}
	}
}

/// The type inside `name<…>` when `ty` is spelled so, with one type argument.
fn wrapped<'t>(ty: &'t Type, name: &str) -> Option<&'t Type> {
	let Type::Path(p) = ty else {
		return None;
	};
	let last = p.path.segments.last()?;
	if last.ident != name {
		return None;
	}
	let PathArguments::AngleBracketed(args) = &last.arguments else {
		return None;
	};
	match args.args.first() {
		Some(GenericArgument::Type(inner)) if args.args.len() == 1 => Some(inner),
		_ => None,
	}
}

fn field_set<'f>(
	fields: impl Iterator<Item = &'f syn::Field>,
	ty: &TypeAttrs,
	reserved: Vec<u32>,
) -> Result<FieldSet> {
	let mut used: BTreeSet<u32> = BTreeSet::new();
	let mut out = Vec::new();
	for f in fields {
		let field = parse_field(f, ty)?;
		for n in std::iter::once(field.n).chain(field.supersedes.iter().map(|s| s.old)) {
			if reserved.contains(&n) {
				return Err(Error::new(field.span, format!("the number {n} is reserved")));
			}
			if !used.insert(n) {
				return Err(Error::new(field.span, format!("the number {n} is used twice")));
			}
		}
		out.push(field);
	}
	Ok(FieldSet {
		fields: out,
		reserved,
	})
}

fn parse_field(f: &syn::Field, ty: &TypeAttrs) -> Result<Field> {
	let span = f.span();
	let member = match &f.ident {
		Some(ident) => Member::Named(ident.clone()),
		None => return Err(Error::new(span, "a stored field has a name")),
	};
	let name = member_name(&member);
	let (mut n, mut since, mut ignorable, mut independent) = (None, None, false, None);
	let (mut default, mut old, mut narrow) = (None, None, None);
	for attr in stored_attrs(&f.attrs) {
		attr.parse_nested_meta(|meta| {
			let once = |taken: bool| {
				if taken {
					Err(meta.error("the attribute is given twice"))
				} else {
					Ok(())
				}
			};
			if meta.path.is_ident("n") {
				once(n.is_some())?;
				n = Some(number(&meta.value()?.parse::<LitInt>()?)?);
			} else if meta.path.is_ident("since") {
				once(since.is_some())?;
				since = Some(Rel::parse(&meta.value()?.parse::<LitStr>()?)?);
			} else if meta.path.is_ident("ignorable") {
				once(ignorable)?;
				ignorable = true;
			} else if meta.path.is_ident("independent") {
				once(independent.is_some())?;
				independent = Some(meta.value()?.parse::<LitStr>()?.value());
			} else if meta.path.is_ident("default") {
				once(default.is_some())?;
				default = Some(meta.value()?.parse::<LitStr>()?.parse::<Path>()?);
			} else if meta.path.is_ident("supersedes") {
				once(old.is_some())?;
				old = Some(number(&meta.value()?.parse::<LitInt>()?)?);
			} else if meta.path.is_ident("narrow") {
				once(narrow.is_some())?;
				narrow = Some(meta.value()?.parse::<LitStr>()?.parse::<Path>()?);
			} else {
				return Err(meta.error("an unknown `stored` attribute on a field"));
			}
			Ok(())
		})?;
	}
	let n = n.ok_or_else(|| {
		Error::new(span, format!("the field `{name}` needs a number: `#[stored(n = N)]`"))
	})?;
	let since = since.unwrap_or(ty.since);
	if since < ty.since {
		return Err(Error::new(span, "a field cannot be older than its type"));
	}

	// How the field's absence reads, from its spelled type.
	let (unrevisioned, inner) = match wrapped(&f.ty, "Unrevisioned") {
		Some(inner) => (true, inner),
		None => (false, &f.ty),
	};
	let presence = if let Some(opt) = wrapped(inner, "Option") {
		if default.is_some() {
			return Err(Error::new(span, "`default` does not apply to an `Option` field"));
		}
		Presence::Opt(opt.clone())
	} else if unrevisioned {
		if default.is_some() {
			return Err(Error::new(
				span,
				"an `Unrevisioned` field already defaults to its default",
			));
		}
		Presence::Default(parse_quote!(::core::default::Default::default))
	} else if let Some(path) = default {
		Presence::Default(path)
	} else {
		Presence::Required
	};

	// Class lives in the number: even is critical, odd is ignorable.
	if ignorable {
		if n % 2 == 0 {
			return Err(Error::new(span, "an ignorable field has an odd number"));
		}
		if !matches!(presence, Presence::Opt(_)) {
			return Err(Error::new(
				span,
				"an ignorable field is an `Option` (or an `Unrevisioned` one), so its absence \
				 means nothing to an earlier release",
			));
		}
		if independent.as_deref().is_none_or(|s| s.trim().is_empty()) {
			return Err(Error::new(
				span,
				"an ignorable field records why it depends on no other field: \
				 `independent = \"reason\"`",
			));
		}
	} else {
		if n % 2 == 1 {
			return Err(Error::new(span, "a critical field has an even number"));
		}
		if independent.is_some() {
			return Err(Error::new(span, "`independent` applies to an ignorable field"));
		}
	}

	// A field added after its type cannot be required: entries written before it existed must
	// still read.
	if since > ty.since && matches!(presence, Presence::Required) {
		return Err(Error::new(
			span,
			"a field added after its type is an `Option`, has a `default`, or is `Unrevisioned`: \
			 an entry written before it existed must still read",
		));
	}
	if ty.floor_fixed && !ignorable && since > ty.since {
		return Err(Error::new(
			span,
			"a `floor_fixed` type gains no critical field after its first release",
		));
	}

	let supersedes = match (old, narrow) {
		(None, None) => None,
		(Some(old), Some(narrow)) => {
			if ignorable
				|| unrevisioned
				|| !matches!(presence, Presence::Required | Presence::Default(_))
			{
				return Err(Error::new(
					span,
					"`supersedes` applies to a critical required or `default` field",
				));
			}
			if old == n {
				return Err(Error::new(span, "a field cannot supersede its own number"));
			}
			Some(Supersedes {
				old,
				narrow,
			})
		}
		_ => return Err(Error::new(span, "`supersedes` and `narrow` come together")),
	};

	Ok(Field {
		member,
		name,
		ty: f.ty.clone(),
		n,
		since,
		ignorable,
		independent,
		presence,
		unrevisioned,
		supersedes,
		span,
	})
}

fn member_name(m: &Member) -> String {
	match m {
		Member::Named(i) => i.unraw().to_string(),
		Member::Unnamed(i) => i.index.to_string(),
	}
}

fn enum_body(data: &DataEnum, ty: &TypeAttrs) -> Result<Body> {
	let mut variants = Vec::new();
	let mut used: BTreeSet<u32> = BTreeSet::new();
	let mut any_legacy = false;
	for v in &data.variants {
		let variant = variant(v, ty)?;
		match variant.n {
			Some(n) => {
				if ty.reserved.contains(&n) {
					return Err(Error::new(variant.span, format!("the number {n} is reserved")));
				}
				if !used.insert(n) {
					return Err(Error::new(variant.span, format!("the number {n} is used twice")));
				}
			}
			None => any_legacy = true,
		}
		variants.push(variant);
	}
	match (any_legacy, &ty.legacy) {
		(true, None) => {
			return Err(Error::new(
				data.variants.span(),
				"a legacy variant needs the type's `legacy = \"path\"` hook, which maps it to a \
				 current variant",
			));
		}
		(false, Some(path)) => {
			return Err(Error::new(path.span(), "the `legacy` hook has no legacy variant"));
		}
		_ => {}
	}
	Ok(Body::Enum {
		variants,
		reserved: ty.reserved.clone(),
	})
}

fn variant(v: &SynVariant, ty: &TypeAttrs) -> Result<Variant> {
	let span = v.span();
	let (mut n, mut since, mut legacy) = (None, None, false);
	let mut reserved = Vec::new();
	for attr in stored_attrs(&v.attrs) {
		attr.parse_nested_meta(|meta| {
			let once = |taken: bool| {
				if taken {
					Err(meta.error("the attribute is given twice"))
				} else {
					Ok(())
				}
			};
			if meta.path.is_ident("n") {
				once(n.is_some())?;
				n = Some(number(&meta.value()?.parse::<LitInt>()?)?);
			} else if meta.path.is_ident("since") {
				once(since.is_some())?;
				since = Some(Rel::parse(&meta.value()?.parse::<LitStr>()?)?);
			} else if meta.path.is_ident("legacy") {
				once(legacy)?;
				legacy = true;
			} else if meta.path.is_ident("reserved") {
				once(!reserved.is_empty())?;
				reserved = numbers(&meta.value()?.parse::<ExprArray>()?)?;
			} else {
				return Err(meta.error("an unknown `stored` attribute on a variant"));
			}
			Ok(())
		})?;
	}
	let name = v.ident.unraw().to_string();
	let since = since.unwrap_or(ty.since);
	if since < ty.since {
		return Err(Error::new(span, "a variant cannot be older than its type"));
	}
	if legacy {
		if n.is_some() || since != ty.since || !reserved.is_empty() {
			return Err(Error::new(
				span,
				"a legacy variant has no number, release or reserved list",
			));
		}
		return Ok(Variant {
			ident: v.ident.clone(),
			name,
			n: None,
			since,
			payload: Payload::Unit,
			span,
		});
	}
	let n = n.ok_or_else(|| {
		Error::new(span, format!("the variant `{name}` needs a number: `#[stored(n = N)]`"))
	})?;
	if ty.floor_fixed && since > ty.since {
		return Err(Error::new(
			span,
			"a `floor_fixed` type gains no variant after its first release",
		));
	}
	let payload = match &v.fields {
		Fields::Unit => Payload::Unit,
		Fields::Unnamed(f) => {
			for field in &f.unnamed {
				if stored_attrs(&field.attrs).next().is_some() {
					return Err(Error::new(
						field.span(),
						"the elements of a tuple variant have no attributes",
					));
				}
			}
			match f.unnamed.len() {
				0 => {
					return Err(Error::new(
						span,
						"an empty tuple variant has no payload: write it as a unit variant",
					));
				}
				1 => Payload::Single(f.unnamed[0].ty.clone()),
				_ => Payload::Positional(f.unnamed.iter().map(|f| f.ty.clone()).collect()),
			}
		}
		Fields::Named(f) => {
			let scoped = TypeAttrs {
				since,
				reserved: reserved.clone(),
				legacy: None,
				revision_embedded: ty.revision_embedded,
				floor_fixed: ty.floor_fixed,
				transparent: false,
				validate: None,
			};
			Payload::Fields(field_set(f.named.iter(), &scoped, reserved)?)
		}
	};
	Ok(Variant {
		ident: v.ident.clone(),
		name,
		n: Some(n),
		since,
		payload,
		span,
	})
}
