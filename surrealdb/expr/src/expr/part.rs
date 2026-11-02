#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Ordering;

use common::fmt::{EscapeKwFreeIdent, Fmt};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::method::is_builtin_method;
use crate::expr::{CoverStmts, Expr, Idiom, Literal, Lookup};

#[derive(Clone, Debug, Eq, PartialEq, surrealdb_stored_derive::DynHash)]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Strand),
	Where(Expr),
	Lookup(Box<Lookup>),
	Value(Expr),
	/// TODO: Remove, start and move it out of part to eliminate invalid state.
	Start(Expr),
	Method(Strand, Vec<Expr>),
	Destructure(Vec<DestructurePart>),
	Optional,
	Recurse(Recurse, Option<Idiom>, Option<RecurseInstruction>),
	Doc,
	RepeatRecurse,
}

impl Part {
	/// Returns a part which is equivalent to `[1]` if called with integer `1`.
	pub fn index_int(idx: i64) -> Self {
		Part::Value(Expr::Literal(Literal::Integer(idx)))
	}

	pub fn is_index(&self) -> bool {
		matches!(self, Part::Value(Expr::Literal(Literal::Integer(_))) | Part::First | Part::Last)
	}

	/// Returns the idex if this part would have been `Part::Index(x)` before
	/// that field was removed.
	///
	/// TODO: Remove this method once we work out the kinks with removing
	/// `Part::Index(x)` and only having `Part::Value(x)`
	///
	/// Already marked as deprecated for the full release to remind that this
	/// behavior should be fixed.
	pub fn as_old_index(&self) -> Option<usize> {
		let Part::Value(Expr::Literal(l)) = self else {
			return None;
		};
		match l {
			Literal::Integer(i) => Some(*i as usize),
			Literal::Float(f) => Some(*f as usize),
			Literal::Decimal(d) => Some(usize::try_from(*d).unwrap_or_default()),
			Literal::BigDecimal(d) => {
				Some(d.to_u64().and_then(|u| usize::try_from(u).ok()).unwrap_or_default())
			}
			Literal::I8(i) => Some(*i as usize),
			Literal::U8(u) => Some(*u as usize),
			Literal::I16(i) => Some(*i as usize),
			Literal::U16(u) => Some(*u as usize),
			Literal::I32(i) => Some(*i as usize),
			Literal::U32(u) => Some(*u as usize),
			Literal::U64(u) => Some(*u as usize),
			Literal::F16(f) => Some(f.to_f32() as usize),
			Literal::F32(f) => Some(*f as usize),
			Literal::None
			| Literal::Null
			| Literal::UnboundedRange
			| Literal::Bool(_)
			| Literal::String(_)
			| Literal::Bytes(_)
			| Literal::Regex(_)
			| Literal::RecordId(_)
			| Literal::Array(_)
			| Literal::Set(_)
			| Literal::Object(_)
			| Literal::Duration(_)
			| Literal::Datetime(_)
			| Literal::Date(_)
			| Literal::Time(_)
			| Literal::Timestamp(_)
			| Literal::Interval(_)
			| Literal::Uuid(_)
			| Literal::Geometry(_)
			| Literal::File(_) => None,
		}
	}

	/// Whether evaluating this part can be done on a read-only transaction.
	///
	/// Matched exhaustively on purpose: a new variant that carries an
	/// expression must be classified here rather than defaulting to read-only,
	/// which would let a mutation reach a read-only transaction.
	pub fn read_only(&self) -> bool {
		match self {
			Part::Start(v) => v.read_only(),
			Part::Where(v) => v.read_only(),
			Part::Value(v) => v.read_only(),
			// A builtin method stages no document write, which is what this
			// predicate answers about; an unregistered name invokes a closure
			// stored on the receiver — a runtime value whose body is invisible
			// here — so it must over-approximate to writable.
			//
			// "Stages no document write" is narrower than "changes nothing":
			// `f'bucket:k'.put(..)` dispatches to `file::put` and writes to a
			// bucket. That is outside the transaction and takes no savepoint,
			// so it does not belong here — it is refused, where it must be, by
			// `NoWriteFrame::refuses_builtin_call` at the dispatch site.
			Part::Method(name, v) => is_builtin_method(name) && v.iter().all(Expr::read_only),
			Part::Lookup(v) => v.read_only(),
			Part::Destructure(v) => v.iter().all(DestructurePart::read_only),
			Part::Recurse(_, alias, instruction) => {
				alias.as_ref().map(|x| x.read_only()).unwrap_or(true)
					&& instruction.as_ref().map(|x| x.read_only()).unwrap_or(true)
			}
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Optional
			| Part::Doc
			| Part::RepeatRecurse => true,
		}
	}
	/// Returns a yield if an alias is specified
	pub fn alias(&self) -> Option<&Idiom> {
		if let Part::Lookup(v) = self {
			v.alias.as_ref()
		} else {
			None
		}
	}

	fn recursion_plan(&self) -> Option<RecursionPlan> {
		match self {
			Part::RepeatRecurse => Some(RecursionPlan::Repeat),
			Part::Destructure(parts) => {
				for (j, p) in parts.iter().enumerate() {
					let plan = match p {
						DestructurePart::Aliased(field, v) => v.find_recursion_plan().map(|plan| {
							(
								field.to_owned(),
								plan.0.to_vec(),
								Box::new(plan.1.clone()),
								plan.2.to_vec(),
							)
						}),
						DestructurePart::Destructure(field, parts) => {
							Part::Destructure(parts.to_owned()).recursion_plan().map(|plan| {
								(
									field.to_owned(),
									vec![Part::Field(field.to_owned())],
									Box::new(plan),
									vec![],
								)
							})
						}
						DestructurePart::All(_)
						| DestructurePart::Field(_)
						| DestructurePart::Rest => None,
					};

					if let Some((field, before, plan, after)) = plan {
						let mut parts = parts.clone();
						parts.remove(j);
						return Some(RecursionPlan::Destructure {
							parts,
							field,
							before,
							plan,
							after,
						});
					}
				}

				None
			}
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Value(_)
			| Part::Start(_)
			| Part::Method(_, _)
			| Part::Optional
			| Part::Recurse(_, _, _)
			| Part::Doc => None,
		}
	}

	pub fn to_raw_string(&self) -> String {
		match self {
			Part::Start(v) => v.to_raw_string(),
			Part::Field(v) => {
				let mut s = ".".to_string();
				EscapeKwFreeIdent(v.as_str()).fmt_sql(&mut s, SqlFormat::SingleLine);
				s
			}
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Where(_)
			| Part::Lookup(_)
			| Part::Value(_)
			| Part::Method(_, _)
			| Part::Destructure(_)
			| Part::Optional
			| Part::Recurse(_, _, _)
			| Part::Doc
			| Part::RepeatRecurse => self.to_sql(),
		}
	}

	// Helper function to get a numeric discriminant for ordering
	fn discriminant_value(&self) -> u8 {
		match self {
			Part::Field(_) => 0,
			Part::All => 1,
			Part::Flatten => 2,
			Part::Last => 3,
			Part::First => 4,
			Part::Where(_) => 5,
			Part::Lookup(_) => 6,
			Part::Value(_) => 7,
			Part::Start(_) => 8,
			Part::Method(_, _) => 9,
			Part::Destructure(_) => 10,
			Part::Optional => 11,
			Part::Recurse(_, _, _) => 12,
			Part::Doc => 13,
			Part::RepeatRecurse => 14,
		}
	}
}

impl ToSql for Part {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Part::All => f.push_str(".*"),
			Part::Last => f.push_str("[$]"),
			Part::First => f.push_str("[0]"),
			Part::Start(v) => v.fmt_sql(f, fmt),
			Part::Field(v) => write_sql!(f, fmt, ".{}", EscapeKwFreeIdent(v.as_str())),
			Part::Flatten => f.push('…'),
			Part::Where(v) => write_sql!(f, fmt, "[WHERE {v}]"),
			Part::Lookup(v) => v.fmt_sql(f, fmt),
			Part::Value(v) => write_sql!(f, fmt, "[{v}]"),
			Part::Method(v, a) => {
				write_sql!(
					f,
					fmt,
					".{}({})",
					EscapeKwFreeIdent(v.as_str()),
					Fmt::comma_separated(a.iter().map(CoverStmts))
				)
			}
			Part::Destructure(v) => {
				f.push_str(".{");
				if !fmt.is_pretty() {
					f.push(' ');
				}
				if !v.is_empty() {
					let fmt = fmt.increment();
					write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(v));
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
			Part::Optional => f.push_str(".?"),
			Part::Recurse(v, nest, instruction) => {
				write_sql!(f, fmt, ".{{{v}");
				if let Some(instruction) = instruction {
					write_sql!(f, fmt, "+{instruction}");
				}
				f.push('}');

				if let Some(nest) = nest {
					f.push('(');
					for p in nest.0.iter() {
						p.fmt_sql(f, fmt);
					}
					f.push(')');
				}
			}
			Part::Doc => f.push('@'),
			Part::RepeatRecurse => f.push_str(".@"),
		}
	}
}

impl PartialOrd for Part {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		let self_disc = self.discriminant_value();
		let other_disc = other.discriminant_value();

		match self_disc.cmp(&other_disc) {
			Ordering::Equal => {
				// Same variant, compare by content
				match (self, other) {
					(Part::Field(a), Part::Field(b)) => a.partial_cmp(b),
					(Part::Method(name_a, args_a), Part::Method(name_b, args_b)) => {
						// Compare method name first, then argument count
						match name_a.partial_cmp(name_b) {
							Some(Ordering::Equal) => args_a.len().partial_cmp(&args_b.len()),
							other => other,
						}
					}
					// For variants without meaningful internal ordering, consider them equal
					// when they're the same variant (All, Flatten, Last, First, Optional, Doc,
					// RepeatRecurse)
					(Part::All, Part::All)
					| (Part::Flatten, Part::Flatten)
					| (Part::Last, Part::Last)
					| (Part::First, Part::First)
					| (Part::Optional, Part::Optional)
					| (Part::Doc, Part::Doc)
					| (Part::RepeatRecurse, Part::RepeatRecurse) => Some(Ordering::Equal),
					// For complex variants (Where, Lookup, Value, Start, Destructure, Recurse),
					// we can't easily compare their contents, so consider them equal when same
					// variant This is acceptable for FETCH clause sorting since these are
					// rarely used
					(Part::Where(_), Part::Where(_))
					| (Part::Lookup(_), Part::Lookup(_))
					| (Part::Value(_), Part::Value(_))
					| (Part::Start(_), Part::Start(_))
					| (Part::Destructure(_), Part::Destructure(_))
					| (Part::Recurse(_, _, _), Part::Recurse(_, _, _)) => Some(Ordering::Equal),

					_ => None,
				}
			}
			ordering @ (Ordering::Less | Ordering::Greater) => Some(ordering),
		}
	}
}

// ------------------------------

#[derive(Clone, Debug)]
pub enum RecursionPlan {
	Repeat,
	Destructure {
		// The destructure parts
		parts: Vec<DestructurePart>,
		// Which field contains the repeat symbol
		field: Strand,
		// Path before the repeat symbol
		before: Vec<Part>,
		// The recursion plan
		plan: Box<RecursionPlan>,
		// Path after the repeat symbol
		after: Vec<Part>,
	},
}

pub trait FindRecursionPlan<'a> {
	fn find_recursion_plan(&'a self) -> Option<(&'a [Part], RecursionPlan, &'a [Part])>;
}

impl<'a> FindRecursionPlan<'a> for &'a [Part] {
	fn find_recursion_plan(&'a self) -> Option<(&'a [Part], RecursionPlan, &'a [Part])> {
		for (i, p) in self.iter().enumerate() {
			if let Some(plan) = p.recursion_plan() {
				return Some((&self[..i], plan, &self[(i + 1)..]));
			}
		}

		None
	}
}

impl<'a> FindRecursionPlan<'a> for &'a Idiom {
	fn find_recursion_plan(&'a self) -> Option<(&'a [Part], RecursionPlan, &'a [Part])> {
		for (i, p) in self.iter().enumerate() {
			if let Some(plan) = p.recursion_plan() {
				return Some((&self[..i], plan, &self[(i + 1)..]));
			}
		}

		None
	}
}

// ------------------------------

pub trait SplitByRepeatRecurse<'a> {
	fn split_by_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])>;
}

impl<'a> SplitByRepeatRecurse<'a> for &'a [Part] {
	fn split_by_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])> {
		self.iter()
			.position(|p| matches!(p, Part::RepeatRecurse))
			// We exclude the `@` repeat recurse symbol here, because
			// it ensures we will loop the idiom path, instead of using
			// `.get()` to recurse
			.map(|i| (&self[..i], &self[(i + 1)..]))
	}
}

impl<'a> SplitByRepeatRecurse<'a> for &'a Idiom {
	fn split_by_repeat_recurse(&'a self) -> Option<(&'a [Part], &'a [Part])> {
		self.iter()
			.position(|p| matches!(p, Part::RepeatRecurse))
			// We exclude the `@` repeat recurse symbol here, because
			// it ensures we will loop the idiom path, instead of using
			// `.get()` to recurse
			.map(|i| (&self[..i], &self[(i + 1)..]))
	}
}

// ------------------------------

pub trait Next<'a> {
	fn next(&'a self) -> &'a [Part];
}

impl<'a> Next<'a> for &'a [Part] {
	fn next(&'a self) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[1..],
		}
	}
}

// ------------------------------

pub trait NextMethod<'a> {
	fn next_method(&'a self) -> &'a [Part];
}

impl<'a> NextMethod<'a> for &'a [Part] {
	fn next_method(&'a self) -> &'a [Part] {
		match self.iter().position(|p| matches!(p, Part::Method(_, _))) {
			None => &[],
			Some(i) => &self[i..],
		}
	}
}

impl<'a> NextMethod<'a> for &'a Idiom {
	fn next_method(&'a self) -> &'a [Part] {
		match self.iter().position(|p| matches!(p, Part::Method(_, _))) {
			None => &[],
			Some(i) => &self[i..],
		}
	}
}

// ------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DestructurePart {
	All(Strand),
	Field(Strand),
	Aliased(Strand, Idiom),
	Destructure(Strand, Vec<DestructurePart>),
	/// `...`: every field of the destructured object not named by another
	/// entry in the same pattern, kept as-is. Only valid as the pattern's
	/// last entry.
	Rest,
}

impl DestructurePart {
	/// Whether evaluating this destructure entry can be done on a read-only
	/// transaction.
	pub fn read_only(&self) -> bool {
		match self {
			DestructurePart::All(_) | DestructurePart::Field(_) | DestructurePart::Rest => true,
			DestructurePart::Aliased(_, v) => v.read_only(),
			DestructurePart::Destructure(_, v) => v.iter().all(DestructurePart::read_only),
		}
	}

	/// The name a caller matching this entry against an object would look up.
	///
	/// Never called on [`DestructurePart::Rest`]: it names no single field,
	/// and every caller that walks a pattern's entries filters it out first.
	pub fn field(&self) -> &str {
		match self {
			DestructurePart::All(v) => v.as_str(),
			DestructurePart::Field(v) => v.as_str(),
			DestructurePart::Aliased(v, _) => v.as_str(),
			DestructurePart::Destructure(v, _) => v.as_str(),
			DestructurePart::Rest => "",
		}
	}

	/// The path from the destructured value to what this entry contributes.
	///
	/// Never called on [`DestructurePart::Rest`]: unlike every other entry it
	/// contributes the destructured value's own remaining fields rather than
	/// one path reachable from it.
	pub fn path(&self) -> Vec<Part> {
		match self {
			DestructurePart::All(v) => vec![Part::Field(v.clone()), Part::All],
			DestructurePart::Field(v) => vec![Part::Field(v.clone())],
			DestructurePart::Aliased(_, v) => v.0.clone(),
			DestructurePart::Destructure(f, d) => {
				vec![Part::Field(f.clone()), Part::Destructure(d.clone())]
			}
			DestructurePart::Rest => Vec::new(),
		}
	}

	pub fn idiom(&self) -> Idiom {
		Idiom(self.path())
	}
}

impl ToSql for DestructurePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			DestructurePart::All(fd) => write_sql!(f, fmt, "{}.*", EscapeKwFreeIdent(fd.as_str())),
			DestructurePart::Field(fd) => write_sql!(f, fmt, "{}", EscapeKwFreeIdent(fd.as_str())),
			DestructurePart::Aliased(fd, v) => {
				write_sql!(f, fmt, "{}: {v}", EscapeKwFreeIdent(fd.as_str()))
			}
			DestructurePart::Destructure(fd, d) => {
				write_sql!(
					f,
					fmt,
					"{}{}",
					EscapeKwFreeIdent(fd.as_str()),
					Part::Destructure(d.clone())
				)
			}
			DestructurePart::Rest => f.push_str("..."),
		}
	}
}

// ------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
}

impl ToSql for Recurse {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Recurse::Fixed(v) => f.push_str(&v.to_string()),
			Recurse::Range(beg, end) => match (beg, end) {
				(None, None) => f.push_str(".."),
				(Some(beg), None) => {
					f.push_str(&beg.to_string());
					f.push_str("..");
				}
				(None, Some(end)) => {
					f.push_str("..");
					f.push_str(&end.to_string());
				}
				(Some(beg), Some(end)) => {
					f.push_str(&beg.to_string());
					f.push_str("..");
					f.push_str(&end.to_string());
				}
			},
		}
	}
}

// ------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RecurseInstruction {
	Path {
		// Do we include the starting point in the paths?
		inclusive: bool,
	},
	Collect {
		// Do we include the starting point in the collection?
		inclusive: bool,
	},
	Shortest {
		// What ending node are we looking for?
		expects: Expr,
		// Do we include the starting point in the collection?
		inclusive: bool,
	},
}

impl RecurseInstruction {
	/// Whether evaluating this recursion instruction can be done on a
	/// read-only transaction.
	pub fn read_only(&self) -> bool {
		match self {
			RecurseInstruction::Path {
				..
			}
			| RecurseInstruction::Collect {
				..
			} => true,
			RecurseInstruction::Shortest {
				expects,
				..
			} => expects.read_only(),
		}
	}
}

impl ToSql for RecurseInstruction {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Self::Path {
				inclusive,
			} => {
				f.push_str("path");

				if *inclusive {
					f.push_str("+inclusive");
				}
			}
			Self::Collect {
				inclusive,
			} => {
				f.push_str("collect");

				if *inclusive {
					f.push_str("+inclusive");
				}
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				write_sql!(f, fmt, "shortest={expects}");

				if *inclusive {
					f.push_str("+inclusive");
				}
			}
		}
	}
}
