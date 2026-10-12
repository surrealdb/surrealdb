//! The schema lock: the frozen numbering of every catalog type in the stored grammar.
//!
//! `catalog-schema.lock` lists, for every derived type reachable from a catalog key, its fields
//! and variants with their numbers, wire types, classes and the release each arrived in. The text
//! is rendered from the static schemas the `Stored` derive declares, so it cannot drift from the
//! types, and it is checked against an earlier lock so that nothing a released build wrote can be
//! reinterpreted.
//!
//! # Format
//!
//! ```text
//! type Index enum since=4.0 reserved=[]
//!   1 Idx unit since=4.0
//!   5 Count item:option<text:cond> since=4.0
//! type Node struct since=4.0 reserved=[] floor_fixed
//!   2 id uuid critical since=4.0 absent=required
//! type StoredViewDefinition enum since=4.0 reserved=[]
//!   - Materialized legacy
//!   1 Clauses fields since=4.0
//!     2 fields text:fields critical since=4.0 absent=required
//! type CondText transparent:text:cond since=4.0
//! ```
//!
//! Types sort by name and members by number; a type's legacy variants, which have no number, come
//! first. A field line ends with `supersedes=M`, `unrevisioned` and `independent="reason"` when the
//! field has them. A variant payload is `unit`, `item:<type>`, `positional:<type>;<type>...` or
//! `fields`, the last followed by the variant's field lines.
//!
//! # Rules
//!
//! [`check`] holds a new lock to an earlier one. The rule names are in [`rules`].
//!
//! 1. A `(type, number)` of the old lock is in the new lock with the same name, wire type, class,
//!    release, absence and supersession, or is listed as reserved. A number is never reused or
//!    retyped, and a change in what absence means takes a new number.
//! 2. Reserved numbers stay reserved and are never used.
//! 3. A field that moves to a new number supersedes the old one, which stays listed. The superseded
//!    number is the lower one, because the encoder writes the oldest number that expresses a value.
//! 4. Critical fields have even numbers. Ignorable fields have odd numbers, are optional, have no
//!    default and state why an earlier release loses nothing by skipping them.
//! 5. A number or type new to the lock has the current release.
//! 6. A variant keeps its number, name, release and payload.
//! 7. A type is never removed, and `revision_embedded` and `floor_fixed` are never dropped.
//! 8. A `floor_fixed` type holds only `floor_fixed` types, gains no critical field and has no line
//!    newer than itself.
//! 9. A field added to an existing type or variant is never required.
//! 10. A required field is never retired to the reserved list.
//!
//! A planned code is a number held for a kind that is not defined yet, such as the physical kinds
//! listed in `PLANNED_PHYSICAL_KINDS`. It is not on a `reserved` list, because the kind it is held
//! for will be given exactly that number: until then the number is a gap, which a reader refuses as
//! an unknown variant. A planned number is given only to the kind it names, and a kind with a
//! planned name takes no other number. `tests/catalog_schema_lock.rs` holds the schemas to this.
//!
//! Semantic independence of an ignorable field and a change of meaning that keeps every number
//! and type are not machine-checkable. They are reviewed in the diff of the lock.

use std::collections::BTreeMap;
use std::fmt;

use surrealdb_expr::val::stored::schema::{
	Absent, Class, FieldSchema, Payload, Release, Shape, StoredField, TypeSchema, VariantSchema,
};

use super::vtable::CATALOG_TYPES;
use crate::conformance::TableConformance;
use crate::feature::StoredFeatureLevel;
use crate::generation::{GenerationMark, TableMigrations};
use crate::layout::LayoutDescriptor;
use crate::node::NodeRelease;

/// The names of the rules [`check`] reports, as [`LockViolation::rule`] carries them.
pub mod rules {
	/// Rule 1.
	pub const REUSE: &str = "a number is never reused or retyped";
	/// Rule 2.
	pub const RESERVED: &str = "a reserved number stays reserved and is never used";
	/// Rule 3.
	pub const SUPERSEDES: &str =
		"a field that moves to a new number supersedes the old one, which stays listed";
	/// Rule 4, numbers.
	pub const PARITY: &str =
		"a number is nonzero, even for a critical field and odd for an ignorable one";
	/// Rule 4, ignorable shape.
	pub const IGNORABLE: &str =
		"an ignorable field is optional, has no default and states its independence";
	/// Rule 5.
	pub const SINCE: &str = "a number or type new to the lock has the current release";
	/// A line older than the type that holds it.
	pub const SINCE_ORDER: &str = "a line is never older than its type";
	/// Rule 6.
	pub const VARIANT: &str = "a variant keeps its number, name, release and payload";
	/// Rule 7, a removed type.
	pub const DELETED: &str = "a type is never removed from the lock";
	/// Rule 7, a dropped marker.
	pub const MARKER: &str = "revision_embedded and floor_fixed are never dropped";
	/// Rule 8, closure.
	pub const FIXED_CLOSURE: &str = "a floor_fixed type holds only floor_fixed types";
	/// Rule 8, growth.
	pub const FIXED_GROWTH: &str =
		"a floor_fixed type gains no critical field and has no line newer than itself";
	/// Rule 9.
	pub const NEW_REQUIRED: &str = "a field added to an existing type is never required";
	/// Rule 10.
	pub const REQUIRED_RESERVED: &str = "a required field is never retired";
	/// A lock that does not parse.
	pub const MALFORMED: &str = "the lock is malformed";
}

/// A rule a lock breaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockViolation {
	/// The type, or `Type::Variant` for a struct variant, the rule is broken in. A malformed lock
	/// names its line and what is wrong with it instead.
	pub ty: String,
	/// The field or variant number, when the rule concerns one.
	pub n: Option<u32>,
	/// One of [`rules`].
	pub rule: &'static str,
}

impl fmt::Display for LockViolation {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.n {
			Some(n) => write!(f, "{} #{n}: {}", self.ty, self.rule),
			None => write!(f, "{}: {}", self.ty, self.rule),
		}
	}
}

const HEADER: &str = "\
# The frozen field numbers of the catalog's stored grammar. Numbers are allocated on main only; a backport
# copies main's lines verbatim. Regenerate with
# CATALOG_SCHEMA=OVERWRITE cargo test --locked -p surrealdb-catalog --test catalog_schema_lock
# The format and the rules a change must keep are in surrealdb/catalog/src/stored/lock.rs.
";

/// Every schema reachable from [`CATALOG_TYPES`], the feature level record, the table layout
/// descriptor, the node release record, and the generation mark, generation ring and conformance
/// records, with identical repeats dropped.
///
/// Two different schemas that share a name are both kept, so that [`render`] shows the collision
/// and the lock refuses it.
pub fn all_schemas() -> Vec<&'static TypeSchema> {
	let mut found = Vec::new();
	for ty in CATALOG_TYPES {
		(ty.schemas)(&mut found);
	}
	StoredFeatureLevel::collect_schemas(&mut found);
	LayoutDescriptor::collect_schemas(&mut found);
	NodeRelease::collect_schemas(&mut found);
	GenerationMark::collect_schemas(&mut found);
	TableMigrations::collect_schemas(&mut found);
	TableConformance::collect_schemas(&mut found);
	let mut seen: Vec<(&str, TypeLine)> = Vec::new();
	let mut unique = Vec::new();
	for schema in found {
		let entry = (schema.name, TypeLine::of(schema));
		if !seen.contains(&entry) {
			seen.push(entry);
			unique.push(schema);
		}
	}
	unique
}

/// The lock text for `schemas`, sorted by type name and then number.
pub fn render(schemas: &[&'static TypeSchema]) -> String {
	let mut types: Vec<(&str, TypeLine)> =
		schemas.iter().map(|schema| (schema.name, TypeLine::of(schema))).collect();
	types.sort_by(|a, b| a.0.cmp(b.0));
	types.dedup();
	let mut out = String::from(HEADER);
	for (name, line) in &types {
		line.print(name, &mut out);
	}
	out
}

/// The rules a new lock must keep against an earlier one.
///
/// `old` is the lock of a released or merged build and `new` the lock being proposed. Both are
/// parsed, so a hand-edited lock that no longer parses is refused. The rules that need no earlier
/// lock are applied to `new` alone.
pub fn check(old: &str, new: &str) -> Result<(), Vec<LockViolation>> {
	let old = Lock::parse(old).map_err(|error| vec![error.at("old lock")])?;
	let new = Lock::parse(new).map_err(|error| vec![error.at("new lock")])?;
	let mut out = Vec::new();
	new.check_own(&mut out);
	new.check_against(&old, &mut out);
	if out.is_empty() {
		Ok(())
	} else {
		Err(out)
	}
}

/// Applies the rules that need no earlier lock to `text`.
pub fn check_alone(text: &str) -> Result<(), Vec<LockViolation>> {
	let lock = Lock::parse(text).map_err(|error| vec![error.at("lock")])?;
	let mut out = Vec::new();
	lock.check_own(&mut out);
	if out.is_empty() {
		Ok(())
	} else {
		Err(out)
	}
}

// ---------------------------------------------------------------------------------------------
// The parsed lock
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct Lock {
	types: BTreeMap<String, TypeLine>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TypeLine {
	since: Release,
	reserved: Vec<u32>,
	floor_fixed: bool,
	revision_embedded: bool,
	shape: ShapeLine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ShapeLine {
	Struct(Vec<FieldLine>),
	Enum(Vec<VariantLine>),
	Transparent(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FieldLine {
	n: u32,
	name: String,
	ty: String,
	critical: bool,
	since: Release,
	absent: AbsentLine,
	supersedes: Option<u32>,
	unrevisioned: bool,
	independent: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AbsentLine {
	Required,
	None,
	Default(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VariantLine {
	/// `None` for a legacy variant.
	n: Option<u32>,
	name: String,
	since: Release,
	payload: PayloadLine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PayloadLine {
	Unit,
	Item(String),
	Positional(Vec<String>),
	Fields(Vec<FieldLine>),
}

/// A line of the lock that does not parse.
struct ParseError {
	line: usize,
	reason: &'static str,
}

impl ParseError {
	fn at(self, which: &str) -> LockViolation {
		LockViolation {
			ty: format!("{which}, line {}: {}", self.line, self.reason),
			n: None,
			rule: rules::MALFORMED,
		}
	}
}

fn type_name_of(write: fn(&mut String)) -> String {
	let mut out = String::new();
	write(&mut out);
	out
}

impl TypeLine {
	fn of(schema: &TypeSchema) -> Self {
		let (reserved, shape) = match schema.shape {
			Shape::Struct(fields) => {
				(sorted(schema.reserved), ShapeLine::Struct(field_lines(fields)))
			}
			Shape::Enum(variants) => {
				(sorted(schema.reserved), ShapeLine::Enum(variant_lines(variants, schema.since)))
			}
			Shape::Transparent(ty) => (Vec::new(), ShapeLine::Transparent(type_name_of(ty))),
		};
		Self {
			since: schema.since,
			reserved,
			floor_fixed: schema.floor_fixed,
			revision_embedded: schema.embedded_in_revision,
			shape,
		}
	}

	fn print(&self, name: &str, out: &mut String) {
		out.push_str("type ");
		out.push_str(name);
		match &self.shape {
			ShapeLine::Struct(_) => out.push_str(" struct"),
			ShapeLine::Enum(_) => out.push_str(" enum"),
			ShapeLine::Transparent(ty) => {
				out.push_str(" transparent:");
				out.push_str(ty);
			}
		}
		out.push_str(&format!(" since={}", self.since));
		if !matches!(self.shape, ShapeLine::Transparent(_)) {
			let reserved: Vec<String> = self.reserved.iter().map(u32::to_string).collect();
			out.push_str(&format!(" reserved=[{}]", reserved.join(",")));
		}
		if self.floor_fixed {
			out.push_str(" floor_fixed");
		}
		if self.revision_embedded {
			out.push_str(" revision_embedded");
		}
		out.push('\n');
		match &self.shape {
			ShapeLine::Struct(fields) => {
				for field in fields {
					field.print(2, out);
				}
			}
			ShapeLine::Enum(variants) => {
				for variant in variants {
					variant.print(out);
				}
			}
			ShapeLine::Transparent(_) => {}
		}
	}

	/// Every wire type name the type mentions.
	fn type_names(&self) -> Vec<&str> {
		let mut out = Vec::new();
		match &self.shape {
			ShapeLine::Struct(fields) => out.extend(fields.iter().map(|f| f.ty.as_str())),
			ShapeLine::Enum(variants) => {
				for variant in variants {
					match &variant.payload {
						PayloadLine::Unit => {}
						PayloadLine::Item(ty) => out.push(ty),
						PayloadLine::Positional(types) => {
							out.extend(types.iter().map(String::as_str))
						}
						PayloadLine::Fields(fields) => {
							out.extend(fields.iter().map(|f| f.ty.as_str()))
						}
					}
				}
			}
			ShapeLine::Transparent(ty) => out.push(ty),
		}
		out
	}
}

fn sorted(numbers: &[u32]) -> Vec<u32> {
	let mut out = numbers.to_vec();
	out.sort_unstable();
	out
}

fn field_lines(fields: &[FieldSchema]) -> Vec<FieldLine> {
	let mut out: Vec<FieldLine> = fields.iter().map(FieldLine::of).collect();
	out.sort_by_key(|field| field.n);
	out
}

fn variant_lines(variants: &[VariantSchema], type_since: Release) -> Vec<VariantLine> {
	let mut out: Vec<VariantLine> =
		variants.iter().map(|variant| VariantLine::of(variant, type_since)).collect();
	out.sort_by(|a, b| a.n.cmp(&b.n).then_with(|| a.name.cmp(&b.name)));
	out
}

impl FieldLine {
	fn of(field: &FieldSchema) -> Self {
		Self {
			n: field.n,
			name: field.name.to_owned(),
			ty: field.type_name(),
			critical: field.class == Class::Critical,
			since: field.since,
			absent: match field.absent {
				Absent::Required => AbsentLine::Required,
				Absent::None => AbsentLine::None,
				Absent::Default(path) => AbsentLine::Default(path.to_owned()),
			},
			supersedes: field.supersedes,
			unrevisioned: field.unrevisioned,
			independent: field.independent.map(str::to_owned),
		}
	}

	fn print(&self, indent: usize, out: &mut String) {
		out.push_str(&" ".repeat(indent));
		let class = if self.critical {
			"critical"
		} else {
			"ignorable"
		};
		let absent = match &self.absent {
			AbsentLine::Required => "required".to_owned(),
			AbsentLine::None => "none".to_owned(),
			AbsentLine::Default(path) => format!("default({path})"),
		};
		out.push_str(&format!(
			"{} {} {} {class} since={} absent={absent}",
			self.n, self.name, self.ty, self.since
		));
		if let Some(old) = self.supersedes {
			out.push_str(&format!(" supersedes={old}"));
		}
		if self.unrevisioned {
			out.push_str(" unrevisioned");
		}
		if let Some(reason) = &self.independent {
			out.push_str(" independent=");
			out.push_str(&quote(reason));
		}
		out.push('\n');
	}

	/// Whether two lines write the same bytes the same way. The reason an ignorable field gives
	/// is documentation, not wire format.
	fn same_wire(&self, other: &Self) -> bool {
		self.name == other.name
			&& self.ty == other.ty
			&& self.critical == other.critical
			&& self.since == other.since
			&& self.absent == other.absent
			&& self.supersedes == other.supersedes
			&& self.unrevisioned == other.unrevisioned
	}
}

impl VariantLine {
	fn of(variant: &VariantSchema, type_since: Release) -> Self {
		let payload = match variant.payload {
			Payload::Unit => PayloadLine::Unit,
			Payload::Item(ty) => PayloadLine::Item(type_name_of(ty)),
			Payload::Positional(types) => {
				PayloadLine::Positional(types.iter().map(|ty| type_name_of(*ty)).collect())
			}
			Payload::Fields(fields) => PayloadLine::Fields(field_lines(fields)),
		};
		Self {
			n: variant.n,
			name: variant.name.to_owned(),
			// A legacy variant has no release of its own.
			since: if variant.n.is_some() {
				variant.since
			} else {
				type_since
			},
			payload,
		}
	}

	fn print(&self, out: &mut String) {
		let Some(n) = self.n else {
			out.push_str(&format!("  - {} legacy\n", self.name));
			return;
		};
		let payload = match &self.payload {
			PayloadLine::Unit => "unit".to_owned(),
			PayloadLine::Item(ty) => format!("item:{ty}"),
			PayloadLine::Positional(types) => format!("positional:{}", types.join(";")),
			PayloadLine::Fields(_) => "fields".to_owned(),
		};
		out.push_str(&format!("  {n} {} {payload} since={}\n", self.name, self.since));
		if let PayloadLine::Fields(fields) = &self.payload {
			for field in fields {
				field.print(4, out);
			}
		}
	}
}

/// A string in double quotes, with `\`, `"` and line feeds escaped.
fn quote(text: &str) -> String {
	let mut out = String::with_capacity(text.len() + 2);
	out.push('"');
	for c in text.chars() {
		match c {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			c => out.push(c),
		}
	}
	out.push('"');
	out
}

/// The text a [`quote`]d string holds, or `None` for anything else.
fn unquote(text: &str) -> Option<String> {
	let inner = text.strip_prefix('"')?.strip_suffix('"')?;
	let mut out = String::with_capacity(inner.len());
	let mut chars = inner.chars();
	while let Some(c) = chars.next() {
		match c {
			'\\' => match chars.next()? {
				'"' => out.push('"'),
				'\\' => out.push('\\'),
				'n' => out.push('\n'),
				_ => return None,
			},
			'"' => return None,
			c => out.push(c),
		}
	}
	Some(out)
}

// ---------------------------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------------------------

impl Lock {
	fn parse(text: &str) -> Result<Self, ParseError> {
		let mut types = BTreeMap::new();
		let mut current: Option<(String, TypeLine)> = None;
		for (index, raw) in text.lines().enumerate() {
			let line = index + 1;
			let fail = |reason: &'static str| ParseError {
				line,
				reason,
			};
			if raw.trim().is_empty() || raw.starts_with('#') {
				continue;
			}
			let body = raw.trim_start_matches(' ');
			let indent = raw.len() - body.len();
			if body.starts_with('\t') || body.trim_end() != body {
				return Err(fail("whitespace other than leading spaces"));
			}
			match indent {
				0 => {
					if let Some((name, ty)) = current.take()
						&& types.insert(name, ty).is_some()
					{
						return Err(fail("a type appears twice"));
					}
					current = Some(parse_type(body).map_err(fail)?);
				}
				2 => {
					let Some((_, ty)) = current.as_mut() else {
						return Err(fail("a member before any type"));
					};
					match &mut ty.shape {
						ShapeLine::Struct(fields) => fields.push(parse_field(body).map_err(fail)?),
						ShapeLine::Enum(variants) => {
							variants.push(parse_variant(body, ty.since).map_err(fail)?)
						}
						ShapeLine::Transparent(_) => {
							return Err(fail("a transparent type has no members"));
						}
					}
				}
				4 => {
					let Some((_, ty)) = current.as_mut() else {
						return Err(fail("a member before any type"));
					};
					let ShapeLine::Enum(variants) = &mut ty.shape else {
						return Err(fail("a nested field outside an enum"));
					};
					let Some(VariantLine {
						payload: PayloadLine::Fields(fields),
						..
					}) = variants.last_mut()
					else {
						return Err(fail("a nested field outside a variant with fields"));
					};
					fields.push(parse_field(body).map_err(fail)?);
				}
				_ => return Err(fail("unexpected indentation")),
			}
		}
		if let Some((name, ty)) = current.take()
			&& types.insert(name, ty).is_some()
		{
			return Err(ParseError {
				line: text.lines().count(),
				reason: "a type appears twice",
			});
		}
		Ok(Self {
			types,
		})
	}
}

fn parse_u32(text: &str) -> Result<u32, &'static str> {
	if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
		return Err("a number is decimal digits");
	}
	text.parse().map_err(|_| "a number does not fit u32")
}

fn parse_release(text: &str) -> Result<Release, &'static str> {
	let value = text.strip_prefix("since=").ok_or("a line states its release as since=M.m")?;
	Release::parse(value).ok_or("a release is M.m")
}

fn parse_type(body: &str) -> Result<(String, TypeLine), &'static str> {
	let mut tokens = body.split_whitespace();
	if tokens.next() != Some("type") {
		return Err("a type line starts with `type`");
	}
	let name = tokens.next().ok_or("a type line names its type")?;
	let kind = tokens.next().ok_or("a type line states its kind")?;
	let mut since = None;
	let mut reserved = Vec::new();
	let mut floor_fixed = false;
	let mut revision_embedded = false;
	for token in tokens {
		if token.starts_with("since=") {
			since = Some(parse_release(token)?);
		} else if let Some(list) = token.strip_prefix("reserved=") {
			let list = list
				.strip_prefix('[')
				.and_then(|list| list.strip_suffix(']'))
				.ok_or("a reserved list is [n,n,...]")?;
			if !list.is_empty() {
				for number in list.split(',') {
					reserved.push(parse_u32(number)?);
				}
			}
		} else if token == "floor_fixed" {
			floor_fixed = true;
		} else if token == "revision_embedded" {
			revision_embedded = true;
		} else {
			return Err("an unknown word on a type line");
		}
	}
	let shape = match kind {
		"struct" => ShapeLine::Struct(Vec::new()),
		"enum" => ShapeLine::Enum(Vec::new()),
		other => match other.strip_prefix("transparent:") {
			Some(ty) if !ty.is_empty() => ShapeLine::Transparent(ty.to_owned()),
			_ => return Err("a type is a struct, an enum or transparent:<type>"),
		},
	};
	Ok((
		name.to_owned(),
		TypeLine {
			since: since.ok_or("a type line states its release")?,
			reserved,
			floor_fixed,
			revision_embedded,
			shape,
		},
	))
}

fn parse_field(body: &str) -> Result<FieldLine, &'static str> {
	// The reason is the last item and may hold spaces.
	let (head, independent) = match body.split_once(" independent=") {
		Some((head, quoted)) => {
			(head, Some(unquote(quoted).ok_or("an independence reason is a quoted string")?))
		}
		None => (body, None),
	};
	let mut tokens = head.split_whitespace();
	let n = parse_u32(tokens.next().ok_or("a field line starts with its number")?)?;
	let name = tokens.next().ok_or("a field line names its field")?;
	let ty = tokens.next().ok_or("a field line states its type")?;
	let critical = match tokens.next() {
		Some("critical") => true,
		Some("ignorable") => false,
		_ => return Err("a field is critical or ignorable"),
	};
	let since = parse_release(tokens.next().ok_or("a field line states its release")?)?;
	let absent = tokens
		.next()
		.and_then(|token| token.strip_prefix("absent="))
		.ok_or("a field line states its absence as absent=...")?;
	let absent = match absent {
		"required" => AbsentLine::Required,
		"none" => AbsentLine::None,
		other => {
			let path = other
				.strip_prefix("default(")
				.and_then(|rest| rest.strip_suffix(')'))
				.ok_or("absence is required, none or default(path)")?;
			AbsentLine::Default(path.to_owned())
		}
	};
	let mut supersedes = None;
	let mut unrevisioned = false;
	for token in tokens {
		if let Some(old) = token.strip_prefix("supersedes=") {
			supersedes = Some(parse_u32(old)?);
		} else if token == "unrevisioned" {
			unrevisioned = true;
		} else {
			return Err("an unknown word on a field line");
		}
	}
	Ok(FieldLine {
		n,
		name: name.to_owned(),
		ty: ty.to_owned(),
		critical,
		since,
		absent,
		supersedes,
		unrevisioned,
		independent,
	})
}

fn parse_variant(body: &str, type_since: Release) -> Result<VariantLine, &'static str> {
	let mut tokens = body.split_whitespace();
	let first = tokens.next().ok_or("a variant line starts with its number")?;
	if first == "-" {
		let name = tokens.next().ok_or("a legacy variant names itself")?;
		if tokens.next() != Some("legacy") || tokens.next().is_some() {
			return Err("a legacy variant line is `- Name legacy`");
		}
		return Ok(VariantLine {
			n: None,
			name: name.to_owned(),
			since: type_since,
			payload: PayloadLine::Unit,
		});
	}
	let n = parse_u32(first)?;
	let name = tokens.next().ok_or("a variant line names its variant")?;
	let payload = tokens.next().ok_or("a variant line states its payload")?;
	let since = parse_release(tokens.next().ok_or("a variant line states its release")?)?;
	if tokens.next().is_some() {
		return Err("an unknown word on a variant line");
	}
	let payload = match payload {
		"unit" => PayloadLine::Unit,
		"fields" => PayloadLine::Fields(Vec::new()),
		other => {
			if let Some(ty) = other.strip_prefix("item:") {
				if ty.is_empty() {
					return Err("an item payload names its type");
				}
				PayloadLine::Item(ty.to_owned())
			} else if let Some(types) = other.strip_prefix("positional:") {
				if types.is_empty() {
					return Err("a positional payload names its types");
				}
				PayloadLine::Positional(types.split(';').map(str::to_owned).collect())
			} else {
				return Err("a payload is unit, item:<type>, positional:<types> or fields");
			}
		}
	};
	Ok(VariantLine {
		n: Some(n),
		name: name.to_owned(),
		since,
		payload,
	})
}

// ---------------------------------------------------------------------------------------------
// Rules that need no earlier lock
// ---------------------------------------------------------------------------------------------

fn violation(ty: &str, n: Option<u32>, rule: &'static str) -> LockViolation {
	LockViolation {
		ty: ty.to_owned(),
		n,
		rule,
	}
}

impl Lock {
	fn check_own(&self, out: &mut Vec<LockViolation>) {
		for (name, ty) in &self.types {
			match &ty.shape {
				ShapeLine::Struct(fields) => {
					for field in fields {
						if ty.reserved.contains(&field.n) {
							out.push(violation(name, Some(field.n), rules::RESERVED));
						}
					}
					check_fields(name, fields, ty.since, out);
				}
				ShapeLine::Enum(variants) => check_variants(name, ty, variants, out),
				ShapeLine::Transparent(_) => {}
			}
			if ty.floor_fixed {
				self.check_floor_fixed(name, ty, out);
			}
		}
	}

	/// Rule 8 without an earlier lock.
	fn check_floor_fixed(&self, name: &str, ty: &TypeLine, out: &mut Vec<LockViolation>) {
		for text in ty.type_names() {
			for token in text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
				if let Some(held) = self.types.get(token)
					&& !held.floor_fixed
				{
					out.push(violation(&format!("{name} -> {token}"), None, rules::FIXED_CLOSURE));
				}
			}
		}
		match &ty.shape {
			ShapeLine::Struct(fields) => {
				for field in fields {
					if field.critical && field.since > ty.since {
						out.push(violation(name, Some(field.n), rules::FIXED_GROWTH));
					}
				}
			}
			ShapeLine::Enum(variants) => {
				for variant in variants {
					if variant.since > ty.since {
						out.push(violation(name, variant.n, rules::FIXED_GROWTH));
					}
					if let PayloadLine::Fields(fields) = &variant.payload {
						for field in fields {
							if field.critical && field.since > ty.since {
								out.push(violation(
									&format!("{name}::{}", variant.name),
									Some(field.n),
									rules::FIXED_GROWTH,
								));
							}
						}
					}
				}
			}
			ShapeLine::Transparent(_) => {}
		}
	}
}

/// Rules 3 and 4 and the release order for the fields of one struct or struct variant.
fn check_fields(
	label: &str,
	fields: &[FieldLine],
	type_since: Release,
	out: &mut Vec<LockViolation>,
) {
	let mut numbers: BTreeMap<u32, &FieldLine> = BTreeMap::new();
	for field in fields {
		if numbers.insert(field.n, field).is_some() {
			out.push(violation(label, Some(field.n), rules::REUSE));
		}
	}
	for field in fields {
		let n = Some(field.n);
		if field.n == 0 || field.critical == (field.n % 2 == 1) {
			out.push(violation(label, n, rules::PARITY));
		}
		if !field.critical {
			let optional = field.ty.starts_with("option<");
			let absence_ok = match &field.absent {
				AbsentLine::None => true,
				// The default an `Unrevisioned` field implies is not a user default.
				AbsentLine::Default(path) => field.unrevisioned && path == "Default::default",
				AbsentLine::Required => false,
			};
			let stated = field.independent.as_deref().is_some_and(|reason| !reason.is_empty());
			if !optional || !absence_ok || !stated {
				out.push(violation(label, n, rules::IGNORABLE));
			}
		}
		if field.since < type_since {
			out.push(violation(label, n, rules::SINCE_ORDER));
		}
		if let Some(old) = field.supersedes {
			let linked =
				numbers.get(&old).is_some_and(|older| older.name == field.name && old < field.n);
			if !linked {
				out.push(violation(label, n, rules::SUPERSEDES));
			}
		}
		// Of several lines for one field, only the lowest is free of a link to an earlier one.
		let lowest =
			fields.iter().filter(|other| other.name == field.name).map(|other| other.n).min();
		if lowest != Some(field.n) {
			let linked = field
				.supersedes
				.is_some_and(|old| numbers.get(&old).is_some_and(|older| older.name == field.name));
			if !linked {
				out.push(violation(label, n, rules::SUPERSEDES));
			}
		}
	}
}

/// Variant numbers, releases and the fields of struct variants for one enum.
fn check_variants(
	name: &str,
	ty: &TypeLine,
	variants: &[VariantLine],
	out: &mut Vec<LockViolation>,
) {
	let mut numbers = BTreeMap::new();
	for variant in variants {
		let Some(n) = variant.n else {
			continue;
		};
		if numbers.insert(n, ()).is_some() {
			out.push(violation(name, Some(n), rules::REUSE));
		}
		if n == 0 {
			out.push(violation(name, Some(n), rules::PARITY));
		}
		if ty.reserved.contains(&n) {
			out.push(violation(name, Some(n), rules::RESERVED));
		}
		if variant.since < ty.since {
			out.push(violation(name, Some(n), rules::SINCE_ORDER));
		}
		if let PayloadLine::Fields(fields) = &variant.payload {
			check_fields(&format!("{name}::{}", variant.name), fields, ty.since, out);
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Rules against an earlier lock
// ---------------------------------------------------------------------------------------------

impl Lock {
	fn check_against(&self, old: &Lock, out: &mut Vec<LockViolation>) {
		for (name, before) in &old.types {
			match self.types.get(name) {
				None => out.push(violation(name, None, rules::DELETED)),
				Some(after) => compare_type(name, before, after, out),
			}
		}
		for (name, after) in &self.types {
			if !old.types.contains_key(name) {
				check_new_type(name, after, out);
			}
		}
	}
}

fn compare_type(name: &str, before: &TypeLine, after: &TypeLine, out: &mut Vec<LockViolation>) {
	if before.since != after.since {
		out.push(violation(name, None, rules::REUSE));
	}
	if before.floor_fixed && !after.floor_fixed {
		out.push(violation(name, None, rules::MARKER));
	}
	if before.revision_embedded && !after.revision_embedded {
		out.push(violation(name, None, rules::MARKER));
	}
	for reserved in &before.reserved {
		if !after.reserved.contains(reserved) {
			out.push(violation(name, Some(*reserved), rules::RESERVED));
		}
	}
	match (&before.shape, &after.shape) {
		(ShapeLine::Struct(before_fields), ShapeLine::Struct(after_fields)) => {
			compare_fields(name, before_fields, after_fields, &after.reserved, out);
			if after.floor_fixed {
				for field in after_fields {
					let new = !before_fields.iter().any(|old| old.n == field.n);
					if new && field.critical {
						out.push(violation(name, Some(field.n), rules::FIXED_GROWTH));
					}
				}
			}
		}
		(ShapeLine::Enum(before_variants), ShapeLine::Enum(after_variants)) => {
			compare_variants(
				name,
				before_variants,
				after_variants,
				&after.reserved,
				after.floor_fixed,
				out,
			);
		}
		(ShapeLine::Transparent(before_ty), ShapeLine::Transparent(after_ty)) => {
			if before_ty != after_ty {
				out.push(violation(name, None, rules::REUSE));
			}
		}
		_ => out.push(violation(name, None, rules::REUSE)),
	}
}

/// Rules 1, 3, 5, 9 and 10 for the fields of one struct or struct variant that both locks hold.
fn compare_fields(
	label: &str,
	before: &[FieldLine],
	after: &[FieldLine],
	reserved: &[u32],
	out: &mut Vec<LockViolation>,
) {
	let after_by_n: BTreeMap<u32, &FieldLine> = after.iter().map(|f| (f.n, f)).collect();
	for field in before {
		match after_by_n.get(&field.n) {
			Some(now) => {
				if !field.same_wire(now) {
					out.push(violation(label, Some(field.n), rules::REUSE));
				}
			}
			None => {
				let moved = after.iter().any(|now| now.name == field.name);
				if moved {
					out.push(violation(label, Some(field.n), rules::SUPERSEDES));
				} else if !reserved.contains(&field.n) {
					out.push(violation(label, Some(field.n), rules::REUSE));
				} else if field.absent == AbsentLine::Required {
					out.push(violation(label, Some(field.n), rules::REQUIRED_RESERVED));
				}
			}
		}
	}
	for field in after {
		if before.iter().any(|old| old.n == field.n) {
			continue;
		}
		if field.since != Release::CURRENT {
			out.push(violation(label, Some(field.n), rules::SINCE));
		}
		if field.absent == AbsentLine::Required {
			out.push(violation(label, Some(field.n), rules::NEW_REQUIRED));
		}
	}
}

/// Rules 1, 5 and 6 for the variants of one enum that both locks hold.
fn compare_variants(
	name: &str,
	before: &[VariantLine],
	after: &[VariantLine],
	reserved: &[u32],
	floor_fixed: bool,
	out: &mut Vec<LockViolation>,
) {
	let after_by_n: BTreeMap<u32, &VariantLine> =
		after.iter().filter_map(|v| v.n.map(|n| (n, v))).collect();
	for variant in before {
		let Some(n) = variant.n else {
			// A legacy variant is how 3.x data is read, so it stays.
			if !after.iter().any(|now| now.n.is_none() && now.name == variant.name) {
				out.push(violation(name, None, rules::VARIANT));
			}
			continue;
		};
		let Some(now) = after_by_n.get(&n) else {
			if !reserved.contains(&n) {
				out.push(violation(name, Some(n), rules::VARIANT));
			}
			continue;
		};
		if now.name != variant.name || now.since != variant.since {
			out.push(violation(name, Some(n), rules::VARIANT));
			continue;
		}
		match (&variant.payload, &now.payload) {
			(PayloadLine::Unit, PayloadLine::Unit) => {}
			(PayloadLine::Item(a), PayloadLine::Item(b)) if a == b => {}
			(PayloadLine::Positional(a), PayloadLine::Positional(b)) if a == b => {}
			(PayloadLine::Fields(a), PayloadLine::Fields(b)) => {
				let label = format!("{name}::{}", variant.name);
				compare_fields(&label, a, b, &[], out);
				if floor_fixed {
					for field in b {
						if field.critical && !a.iter().any(|old| old.n == field.n) {
							out.push(violation(&label, Some(field.n), rules::FIXED_GROWTH));
						}
					}
				}
			}
			_ => out.push(violation(name, Some(n), rules::VARIANT)),
		}
	}
	for variant in after {
		match variant.n {
			None => {
				if !before.iter().any(|old| old.n.is_none() && old.name == variant.name) {
					out.push(violation(name, None, rules::VARIANT));
				}
			}
			Some(n) => {
				if !before.iter().any(|old| old.n == Some(n)) && variant.since != Release::CURRENT {
					out.push(violation(name, Some(n), rules::SINCE));
				}
			}
		}
	}
}

/// Rule 5 for a type the earlier lock does not hold. Its fields may be required: no earlier
/// release has an entry of it.
fn check_new_type(name: &str, ty: &TypeLine, out: &mut Vec<LockViolation>) {
	if ty.since != Release::CURRENT {
		out.push(violation(name, None, rules::SINCE));
	}
	let mut field_since = |label: &str, fields: &[FieldLine]| {
		for field in fields {
			if field.since != Release::CURRENT {
				out.push(violation(label, Some(field.n), rules::SINCE));
			}
		}
	};
	match &ty.shape {
		ShapeLine::Struct(fields) => field_since(name, fields),
		ShapeLine::Enum(variants) => {
			for variant in variants {
				if let PayloadLine::Fields(fields) = &variant.payload {
					field_since(&format!("{name}::{}", variant.name), fields);
				}
			}
			for variant in variants {
				if variant.n.is_some() && variant.since != Release::CURRENT {
					out.push(violation(name, variant.n, rules::SINCE));
				}
			}
		}
		ShapeLine::Transparent(_) => {}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A lock with one of everything a rule concerns. `@` stands for the current release.
	const BASE: &str = "\
type Embedded struct since=@ reserved=[] revision_embedded
  2 x int critical since=@ absent=required
type Fixed struct since=@ reserved=[] floor_fixed
  2 head u16 critical since=@ absent=required
type Holder struct since=@ reserved=[] floor_fixed
  2 fixed Fixed critical since=@ absent=required
type Mode enum since=@ reserved=[3]
  - Old legacy
  1 Off unit since=@
  2 On item:string since=@
  4 Both fields since=@
    2 left string critical since=@ absent=required
type Text transparent:string since=@
type Thing struct since=@ reserved=[5]
  1 note option<string> ignorable since=@ absent=none independent=\"a note is for people\"
  2 id uuid critical since=@ absent=required
  4 name string critical since=@ absent=required
  6 tags option<list<string>> critical since=@ absent=none
";

	/// The last field line of `Thing`, after which a test adds lines.
	const TAGS: &str = "  6 tags option<list<string>> critical since=@ absent=none\n";

	/// `text` with `@` replaced by the current release.
	fn at_current(text: &str) -> String {
		text.replace('@', &Release::CURRENT.to_string())
	}

	/// The base lock with the unique occurrence of `from` replaced by `to`, both written with `@`.
	fn edited(from: &str, to: &str) -> String {
		let base = at_current(BASE);
		let from = at_current(from);
		assert_eq!(base.matches(&from).count(), 1, "`{from}` is not unique in the base lock");
		base.replacen(&from, &at_current(to), 1)
	}

	/// The base lock with `lines` added after the unique line `after`.
	fn added(after: &str, lines: &str) -> String {
		edited(after, &format!("{after}{lines}"))
	}

	/// The base lock with the unique line `line` removed.
	fn removed(line: &str) -> String {
		edited(line, "")
	}

	/// The rules `check` reports for `new` against the base lock, sorted and without repeats.
	fn broken(new: &str) -> Vec<&'static str> {
		names(check(&at_current(BASE), new))
	}

	/// The rules `check_alone` reports for `text`, written with `@`, sorted and without repeats.
	fn broken_alone(text: &str) -> Vec<&'static str> {
		names(check_alone(&at_current(text)))
	}

	fn names(result: Result<(), Vec<LockViolation>>) -> Vec<&'static str> {
		let mut rules: Vec<&'static str> =
			result.err().unwrap_or_default().iter().map(|violation| violation.rule).collect();
		rules.sort_unstable();
		rules.dedup();
		rules
	}

	const NOTHING: [&str; 0] = [];

	#[test]
	fn the_base_lock_keeps_every_rule() {
		assert_eq!(broken_alone(BASE), NOTHING);
		assert_eq!(broken(&at_current(BASE)), NOTHING);
	}

	#[test]
	fn the_rendered_lock_parses_back_to_the_schemas() {
		let schemas = all_schemas();
		let text = render(&schemas);
		let lock = match Lock::parse(&text) {
			Ok(lock) => lock,
			Err(error) => panic!("line {}: {}", error.line, error.reason),
		};
		for schema in schemas {
			assert_eq!(lock.types.get(schema.name), Some(&TypeLine::of(schema)), "{}", schema.name);
		}
	}

	// Compatible changes: each reports nothing.

	#[test]
	fn an_optional_critical_field_may_be_added() {
		let new = added(TAGS, "  8 extra option<int> critical since=@ absent=none\n");
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn an_ignorable_field_that_states_its_independence_may_be_added() {
		let new = added(
			TAGS,
			"  7 hint option<string> ignorable since=@ absent=none independent=\"a hint is for people\"\n",
		);
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn an_optional_field_may_be_retired_to_the_reserved_list() {
		let new = removed(TAGS).replacen("reserved=[5]", "reserved=[5,6]", 1);
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn a_field_may_move_to_a_number_that_supersedes_the_old_one() {
		let new =
			added(TAGS, "  8 name option<string> critical since=@ absent=none supersedes=4\n");
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn a_variant_and_a_type_may_be_added() {
		let new = added(
			"    2 left string critical since=@ absent=required\n",
			"  5 Many item:list<string> since=@\n",
		) + &at_current(
			"type Zebra struct since=@ reserved=[]\n  2 stripes int critical since=@ absent=required\n",
		);
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn a_variant_may_be_retired_to_the_reserved_list() {
		let new = removed("  1 Off unit since=@\n").replacen("reserved=[3]", "reserved=[1,3]", 1);
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn an_ignorable_reason_may_be_reworded() {
		let new = edited("a note is for people", "a note is read by people alone");
		assert_eq!(broken(&new), NOTHING);
	}

	// Rule 1: a number is never reused or retyped.

	#[test]
	fn rule_reuse_refuses_a_retyped_field() {
		assert_eq!(broken(&edited("  4 name string", "  4 name int")), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_renamed_field() {
		assert_eq!(broken(&edited("  4 name string", "  4 title string")), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_field_whose_class_changes() {
		let new = edited(
			"  6 tags option<list<string>> critical",
			"  6 tags option<list<string>> ignorable",
		);
		assert!(broken(&new).contains(&rules::REUSE), "{:?}", broken(&new));
	}

	#[test]
	fn rule_reuse_refuses_a_change_in_what_absence_means() {
		let new = edited(
			TAGS,
			"  6 tags option<list<string>> critical since=@ absent=default(Vec::new)\n",
		);
		assert_eq!(broken(&new), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_field_dropped_without_reserving_its_number() {
		assert_eq!(broken(&removed(TAGS)), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_number_listed_twice() {
		let new = added(TAGS, "  6 more option<int> critical since=@ absent=none\n");
		assert_eq!(names(check_alone(&new)), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_variant_number_listed_twice() {
		let new = added("  2 On item:string since=@\n", "  2 Up unit since=@\n");
		assert_eq!(names(check_alone(&new)), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_type_whose_release_changes() {
		let new = edited(
			"type Text transparent:string since=@",
			"type Text transparent:string since=99.0",
		);
		assert_eq!(broken(&new), [rules::REUSE]);
	}

	#[test]
	fn rule_reuse_refuses_a_type_whose_kind_changes() {
		let new = edited("type Text transparent:string", "type Text transparent:bytes");
		assert_eq!(broken(&new), [rules::REUSE]);
		let new = edited(
			"type Embedded struct since=@ reserved=[] revision_embedded\n  2 x int critical since=@ absent=required\n",
			"type Embedded enum since=@ reserved=[] revision_embedded\n  2 X unit since=@\n",
		);
		assert_eq!(broken(&new), [rules::REUSE]);
	}

	// Rule 2: reserved numbers stay reserved and are never used.

	#[test]
	fn rule_reserved_refuses_a_field_on_a_reserved_number() {
		let new = added(
			"  4 name string critical since=@ absent=required\n",
			"  5 again option<string> ignorable since=@ absent=none independent=\"why\"\n",
		);
		assert_eq!(names(check_alone(&new)), [rules::RESERVED]);
	}

	#[test]
	fn rule_reserved_refuses_a_variant_on_a_reserved_number() {
		let new = added("  2 On item:string since=@\n", "  3 Again unit since=@\n");
		assert_eq!(names(check_alone(&new)), [rules::RESERVED]);
	}

	#[test]
	fn rule_reserved_refuses_a_number_taken_off_the_reserved_list() {
		let new = edited(
			"type Thing struct since=@ reserved=[5]",
			"type Thing struct since=@ reserved=[]",
		);
		assert_eq!(broken(&new), [rules::RESERVED]);
		let new =
			edited("type Mode enum since=@ reserved=[3]", "type Mode enum since=@ reserved=[]");
		assert_eq!(broken(&new), [rules::RESERVED]);
	}

	// Rule 3: a field that moves supersedes the old number, which stays listed.

	#[test]
	fn rule_supersedes_refuses_a_moved_field_whose_old_number_is_dropped() {
		let new = edited(
			"  4 name string critical since=@ absent=required\n",
			"  8 name option<string> critical since=@ absent=none supersedes=4\n",
		);
		assert!(broken(&new).contains(&rules::SUPERSEDES), "{:?}", broken(&new));
	}

	#[test]
	fn rule_supersedes_refuses_a_second_number_with_no_link() {
		let new = added(TAGS, "  8 name option<string> critical since=@ absent=none\n");
		assert_eq!(names(check_alone(&new)), [rules::SUPERSEDES]);
	}

	#[test]
	fn rule_supersedes_refuses_a_link_to_a_higher_number() {
		let text = "\
type T struct since=@ reserved=[]
  2 a option<int> critical since=@ absent=none supersedes=4
  4 a option<int> critical since=@ absent=none
";
		assert_eq!(broken_alone(text), [rules::SUPERSEDES]);
	}

	#[test]
	fn rule_supersedes_refuses_a_link_to_another_field() {
		let new = added(TAGS, "  8 extra option<int> critical since=@ absent=none supersedes=4\n");
		assert_eq!(names(check_alone(&new)), [rules::SUPERSEDES]);
	}

	// Rule 4: parity of numbers, and the shape of an ignorable field.

	#[test]
	fn rule_parity_refuses_a_critical_field_on_an_odd_number() {
		let new = added(TAGS, "  7 extra option<int> critical since=@ absent=none\n");
		assert_eq!(names(check_alone(&new)), [rules::PARITY]);
	}

	#[test]
	fn rule_parity_refuses_an_ignorable_field_on_an_even_number() {
		let new = added(
			TAGS,
			"  8 extra option<int> ignorable since=@ absent=none independent=\"why\"\n",
		);
		assert_eq!(names(check_alone(&new)), [rules::PARITY]);
	}

	#[test]
	fn rule_parity_refuses_number_zero() {
		let text =
			"type T struct since=@ reserved=[]\n  0 a int critical since=@ absent=required\n";
		assert_eq!(broken_alone(text), [rules::PARITY]);
		let text = "type T enum since=@ reserved=[]\n  0 A unit since=@\n";
		assert_eq!(broken_alone(text), [rules::PARITY]);
	}

	#[test]
	fn rule_ignorable_refuses_a_field_that_is_not_optional() {
		let new = edited("  1 note option<string> ignorable", "  1 note string ignorable");
		assert_eq!(names(check_alone(&new)), [rules::IGNORABLE]);
	}

	#[test]
	fn rule_ignorable_refuses_a_required_or_defaulted_field() {
		let new = edited("ignorable since=@ absent=none", "ignorable since=@ absent=required");
		assert_eq!(names(check_alone(&new)), [rules::IGNORABLE]);
		let new = edited("ignorable since=@ absent=none", "ignorable since=@ absent=default(make)");
		assert_eq!(names(check_alone(&new)), [rules::IGNORABLE]);
	}

	#[test]
	fn rule_ignorable_admits_the_default_of_an_unrevisioned_field() {
		let new = edited(
			"ignorable since=@ absent=none independent=",
			"ignorable since=@ absent=default(Default::default) unrevisioned independent=",
		);
		assert_eq!(names(check_alone(&new)), NOTHING);
	}

	#[test]
	fn rule_ignorable_refuses_a_field_that_states_no_independence() {
		let new = edited(" independent=\"a note is for people\"", "");
		assert_eq!(names(check_alone(&new)), [rules::IGNORABLE]);
		let new = edited("independent=\"a note is for people\"", "independent=\"\"");
		assert_eq!(names(check_alone(&new)), [rules::IGNORABLE]);
	}

	// Rule 5: a number or type new to the lock has the current release.

	#[test]
	fn rule_since_refuses_a_new_field_of_another_release() {
		let new = added(TAGS, "  8 extra option<int> critical since=99.0 absent=none\n");
		assert_eq!(broken(&new), [rules::SINCE]);
	}

	#[test]
	fn rule_since_refuses_a_new_variant_of_another_release() {
		let new = added("  2 On item:string since=@\n", "  5 Late unit since=99.0\n");
		assert_eq!(broken(&new), [rules::SINCE]);
	}

	#[test]
	fn rule_since_refuses_a_new_type_of_another_release() {
		let new = at_current(BASE)
			+ "type Zebra struct since=99.0 reserved=[]\n  2 stripes int critical since=99.0 absent=required\n";
		assert_eq!(broken(&new), [rules::SINCE]);
		let new = at_current(BASE)
			+ &at_current("type Zebra enum since=@ reserved=[]\n  1 Old unit since=99.0\n");
		assert_eq!(broken(&new), [rules::SINCE]);
	}

	#[test]
	fn rule_since_order_refuses_a_line_older_than_its_type() {
		let text =
			"type T struct since=99.0 reserved=[]\n  2 a int critical since=@ absent=required\n";
		assert_eq!(broken_alone(text), [rules::SINCE_ORDER]);
		let text = "type T enum since=99.0 reserved=[]\n  1 A unit since=@\n";
		assert_eq!(broken_alone(text), [rules::SINCE_ORDER]);
	}

	// Rule 6: a variant keeps its number, name, release and payload.

	#[test]
	fn rule_variant_refuses_a_renamed_variant() {
		assert_eq!(broken(&edited("  1 Off unit", "  1 Down unit")), [rules::VARIANT]);
	}

	#[test]
	fn rule_variant_refuses_a_changed_payload() {
		assert_eq!(broken(&edited("  2 On item:string", "  2 On item:int")), [rules::VARIANT]);
		assert_eq!(broken(&edited("  2 On item:string", "  2 On unit")), [rules::VARIANT]);
	}

	#[test]
	fn rule_variant_refuses_a_variant_dropped_without_reserving_its_number() {
		assert_eq!(broken(&removed("  1 Off unit since=@\n")), [rules::VARIANT]);
	}

	#[test]
	fn rule_variant_refuses_a_dropped_or_added_legacy_variant() {
		assert_eq!(broken(&removed("  - Old legacy\n")), [rules::VARIANT]);
		assert_eq!(broken(&added("  - Old legacy\n", "  - Older legacy\n")), [rules::VARIANT]);
	}

	#[test]
	fn rule_variant_holds_the_fields_of_a_struct_variant_to_the_field_rules() {
		let new = edited("    2 left string critical", "    2 left int critical");
		assert_eq!(broken(&new), [rules::REUSE]);
		let new = added(
			"    2 left string critical since=@ absent=required\n",
			"    4 right string critical since=@ absent=required\n",
		);
		assert_eq!(broken(&new), [rules::NEW_REQUIRED]);
	}

	// Rule 7: a type is never removed, and its markers are never dropped.

	#[test]
	fn rule_deleted_refuses_a_removed_type() {
		assert_eq!(broken(&removed("type Text transparent:string since=@\n")), [rules::DELETED]);
	}

	#[test]
	fn rule_marker_refuses_a_dropped_floor_fixed() {
		let new = edited(
			"type Fixed struct since=@ reserved=[] floor_fixed",
			"type Fixed struct since=@ reserved=[]",
		);
		// Holder, which is floor_fixed, now also holds a type that is not.
		assert_eq!(broken(&new), [rules::FIXED_CLOSURE, rules::MARKER]);
	}

	#[test]
	fn rule_marker_refuses_a_dropped_revision_embedded() {
		let new = edited(
			"type Embedded struct since=@ reserved=[] revision_embedded",
			"type Embedded struct since=@ reserved=[]",
		);
		assert_eq!(broken(&new), [rules::MARKER]);
	}

	// Rule 8: a floor_fixed type holds only floor_fixed types and does not grow.

	#[test]
	fn rule_fixed_closure_refuses_a_floor_fixed_type_that_holds_another() {
		let new = edited("  2 fixed Fixed critical", "  2 fixed option<Thing> critical");
		assert_eq!(names(check_alone(&new)), [rules::FIXED_CLOSURE]);
	}

	#[test]
	fn rule_fixed_growth_refuses_a_critical_field_added_to_a_floor_fixed_type() {
		let new = added(
			"  2 head u16 critical since=@ absent=required\n",
			"  4 more option<u16> critical since=@ absent=none\n",
		);
		assert_eq!(broken(&new), [rules::FIXED_GROWTH]);
	}

	#[test]
	fn rule_fixed_growth_admits_an_ignorable_field_added_to_a_floor_fixed_type() {
		let new = added(
			"  2 head u16 critical since=@ absent=required\n",
			"  3 note option<string> ignorable since=@ absent=none independent=\"why\"\n",
		);
		assert_eq!(broken(&new), NOTHING);
	}

	#[test]
	fn rule_fixed_growth_refuses_a_line_newer_than_a_floor_fixed_type() {
		let text = "type F struct since=@ reserved=[] floor_fixed\n  2 a option<int> critical since=99.0 absent=none\n";
		assert_eq!(broken_alone(text), [rules::FIXED_GROWTH]);
		let text = "type F enum since=@ reserved=[] floor_fixed\n  1 A unit since=99.0\n";
		assert_eq!(broken_alone(text), [rules::FIXED_GROWTH]);
	}

	#[test]
	fn rule_fixed_growth_refuses_a_critical_field_added_to_a_floor_fixed_variant() {
		let old = at_current(
			"type F enum since=@ reserved=[] floor_fixed\n  1 A fields since=@\n    2 a int critical since=@ absent=required\n",
		);
		let new = old.clone() + &at_current("    4 b option<int> critical since=@ absent=none\n");
		assert_eq!(names(check(&old, &new)), [rules::FIXED_GROWTH]);
	}

	// Rule 9: a field added to an existing type is never required.

	#[test]
	fn rule_new_required_refuses_a_required_field_added_to_an_existing_type() {
		let new = added(TAGS, "  8 extra string critical since=@ absent=required\n");
		assert_eq!(broken(&new), [rules::NEW_REQUIRED]);
	}

	#[test]
	fn rule_new_required_admits_required_fields_of_a_new_type() {
		let new = at_current(BASE)
			+ &at_current(
				"type Zebra struct since=@ reserved=[]\n  2 stripes int critical since=@ absent=required\n",
			);
		assert_eq!(broken(&new), NOTHING);
	}

	// Rule 10: a required field is never retired to the reserved list.

	#[test]
	fn rule_required_reserved_refuses_a_retired_required_field() {
		let new = removed("  4 name string critical since=@ absent=required\n").replacen(
			"reserved=[5]",
			"reserved=[4,5]",
			1,
		);
		assert_eq!(broken(&new), [rules::REQUIRED_RESERVED]);
	}

	// A lock that does not parse.

	#[test]
	fn a_malformed_lock_is_refused() {
		for text in [
			"type T struct reserved=[]\n",
			"type T struct since=4 reserved=[]\n",
			"type T widget since=@\n",
			"  2 a int critical since=@ absent=required\n",
			"type T struct since=@ reserved=[]\n  2 a int sometimes since=@ absent=required\n",
			"type T struct since=@ reserved=[]\n  2 a int critical since=@ absent=maybe\n",
			"type T struct since=@ reserved=[]\n  2 a int critical since=@ absent=required extra\n",
			"type T struct since=@ reserved=[]\n   2 a int critical since=@ absent=required\n",
			"type T struct since=@ reserved=[]\n\t2 a int critical since=@ absent=required\n",
			"type T struct since=@ reserved=[] \n",
			"type T transparent:string since=@\n  2 a int critical since=@ absent=required\n",
			"type T struct since=@ reserved=[]\ntype T struct since=@ reserved=[]\n",
			"type T enum since=@ reserved=[]\n  1 A widget since=@\n",
			"type T enum since=@ reserved=[]\n  1 A unit since=@\n    2 a int critical since=@ absent=required\n",
			"type T struct since=@ reserved=[]\n  1 a option<int> ignorable since=@ absent=none independent=unquoted\n",
		] {
			assert_eq!(broken_alone(text), [rules::MALFORMED], "{text}");
			assert_eq!(
				names(check(&at_current(text), &at_current(BASE))),
				[rules::MALFORMED],
				"{text}"
			);
		}
	}

	#[test]
	fn a_quoted_reason_round_trips() {
		for reason in ["plain", "with \"quotes\"", "a back\\slash", "two\nlines", ""] {
			assert_eq!(unquote(&quote(reason)).as_deref(), Some(reason));
		}
		assert_eq!(unquote("\"bad \\x escape\""), None);
		assert_eq!(unquote("\"unescaped \" quote\""), None);
		assert_eq!(unquote("no quotes"), None);
	}
}
