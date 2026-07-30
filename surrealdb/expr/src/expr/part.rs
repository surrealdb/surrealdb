use std::cmp::Ordering;

use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Idiom, Literal, Lookup};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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
		match self {
			Part::Value(Expr::Literal(l)) => match l {
				crate::expr::Literal::Integer(i) => Some(*i as usize),
				crate::expr::Literal::Float(f) => Some(*f as usize),
				crate::expr::Literal::Decimal(d) => Some(usize::try_from(*d).unwrap_or_default()),
				_ => None,
			},
			_ => None,
		}
	}

	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		match self {
			Part::Start(v) => v.read_only(),
			Part::Where(v) => v.read_only(),
			Part::Value(v) => v.read_only(),
			Part::Method(_, v) => v.iter().all(Expr::read_only),
			_ => true,
		}
	}
	/// Returns a yield if an alias is specified
	pub fn alias(&self) -> Option<&Idiom> {
		match self {
			Part::Lookup(v) => v.alias.as_ref(),
			_ => None,
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
						_ => None,
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
			_ => None,
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
			_ => self.to_sql(),
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
		let part: crate::sql::part::Part = self.clone().into();
		part.fmt_sql(f, fmt);
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
			ordering => Some(ordering),
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
}

impl DestructurePart {
	pub fn field(&self) -> &str {
		match self {
			DestructurePart::All(v) => v.as_str(),
			DestructurePart::Field(v) => v.as_str(),
			DestructurePart::Aliased(v, _) => v.as_str(),
			DestructurePart::Destructure(v, _) => v.as_str(),
		}
	}

	pub fn path(&self) -> Vec<Part> {
		match self {
			DestructurePart::All(v) => vec![Part::Field(v.clone()), Part::All],
			DestructurePart::Field(v) => vec![Part::Field(v.clone())],
			DestructurePart::Aliased(_, v) => v.0.clone(),
			DestructurePart::Destructure(f, d) => {
				vec![Part::Field(f.clone()), Part::Destructure(d.clone())]
			}
		}
	}

	pub fn idiom(&self) -> Idiom {
		Idiom(self.path())
	}
}

impl ToSql for DestructurePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::part::DestructurePart = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

// ------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
}

impl ToSql for Recurse {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let recurse: crate::sql::part::Recurse = self.clone().into();
		recurse.fmt_sql(f, fmt);
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

impl ToSql for RecurseInstruction {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::part::RecurseInstruction = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
