use crate::{
	cnf::IDIOM_RECURSION_LIMIT,
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	exe::try_join_all_buffered,
	sql::{fmt::Fmt, strand::no_nul_bytes, Graph, Ident, Idiom, Number, Value},
};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;
use std::str;

use super::{
	fmt::{is_pretty, pretty_indent},
	value::idiom_recursion::{clean_iteration, compute_idiom_recursion, is_final, Recursion},
};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Ident),
	Index(Number),
	Where(Value),
	Graph(Graph),
	Value(Value),
	Start(Value),
	Method(#[serde(with = "no_nul_bytes")] String, Vec<Value>),
	#[revision(start = 2)]
	Destructure(Vec<DestructurePart>),
	Optional,
	#[revision(start = 3)]
	Recurse(Recurse, Option<Idiom>),
	#[revision(start = 3)]
	Doc,
	#[revision(start = 3)]
	RepeatRecurse,
}

impl From<i32> for Part {
	fn from(v: i32) -> Self {
		Self::Index(v.into())
	}
}

impl From<isize> for Part {
	fn from(v: isize) -> Self {
		Self::Index(v.into())
	}
}

impl From<usize> for Part {
	fn from(v: usize) -> Self {
		Self::Index(v.into())
	}
}

impl From<String> for Part {
	fn from(v: String) -> Self {
		Self::Field(v.into())
	}
}

impl From<Number> for Part {
	fn from(v: Number) -> Self {
		Self::Index(v)
	}
}

impl From<Ident> for Part {
	fn from(v: Ident) -> Self {
		Self::Field(v)
	}
}

impl From<Graph> for Part {
	fn from(v: Graph) -> Self {
		Self::Graph(v)
	}
}

impl From<&str> for Part {
	fn from(v: &str) -> Self {
		match v.parse::<isize>() {
			Ok(v) => Self::from(v),
			_ => Self::from(v.to_owned()),
		}
	}
}

impl Part {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Part::Start(v) => v.writeable(),
			Part::Where(v) => v.writeable(),
			Part::Value(v) => v.writeable(),
			Part::Method(_, v) => v.iter().any(Value::writeable),
			_ => false,
		}
	}
	/// Returns a yield if an alias is specified
	pub(crate) fn alias(&self) -> Option<&Idiom> {
		match self {
			Part::Graph(v) => v.alias.as_ref(),
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
								Box::new(plan.1.to_owned()),
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
}

impl fmt::Display for Part {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Part::All => f.write_str("[*]"),
			Part::Last => f.write_str("[$]"),
			Part::First => f.write_str("[0]"),
			Part::Start(v) => write!(f, "{v}"),
			Part::Field(v) => write!(f, ".{v}"),
			Part::Flatten => f.write_str("…"),
			Part::Index(v) => write!(f, "[{v}]"),
			Part::Where(v) => write!(f, "[WHERE {v}]"),
			Part::Graph(v) => write!(f, "{v}"),
			Part::Value(v) => write!(f, "[{v}]"),
			Part::Method(v, a) => write!(f, ".{v}({})", Fmt::comma_separated(a)),
			Part::Destructure(v) => {
				f.write_str(".{")?;
				if !is_pretty() {
					f.write_char(' ')?;
				}
				if !v.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(v))?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			}
			Part::Optional => write!(f, "?"),
			Part::Recurse(v, nest) => {
				write!(f, ".{{{v}}}")?;
				if let Some(nest) = nest {
					write!(f, "({nest})")?;
				}

				Ok(())
			}
			Part::Doc => write!(f, "@"),
			Part::RepeatRecurse => write!(f, ".@"),
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
		field: Ident,
		// Path before the repeat symbol
		before: Vec<Part>,
		// The recursion plan
		plan: Box<RecursionPlan>,
		// Path after the repeat symbol
		after: Vec<Part>,
	},
}

impl<'a> RecursionPlan {
	pub async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		rec: Recursion<'a>,
	) -> Result<Value, Error> {
		match rec.current {
			Value::Array(value) => stk
				.scope(|scope| {
					let futs = value.iter().map(|value| {
						scope.run(|stk| {
							let rec = rec.with_current(value);
							self.compute_inner(stk, ctx, opt, doc, rec)
						})
					});
					try_join_all_buffered(futs)
				})
				.await
				.map(Into::into),
			_ => stk.run(|stk| self.compute_inner(stk, ctx, opt, doc, rec)).await,
		}
	}

	pub async fn compute_inner(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		rec: Recursion<'a>,
	) -> Result<Value, Error> {
		match self {
			Self::Repeat => compute_idiom_recursion(stk, ctx, opt, doc, rec).await,
			Self::Destructure {
				parts,
				field,
				before,
				plan,
				after,
			} => {
				let v = stk.run(|stk| rec.current.get(stk, ctx, opt, doc, before)).await?;
				let v = plan.compute(stk, ctx, opt, doc, rec.with_current(&v)).await?;
				let v = stk.run(|stk| v.get(stk, ctx, opt, doc, after)).await?;
				let v = clean_iteration(v);

				if rec.iterated < rec.min && is_final(&v) {
					// We do not use get_final here, because it's not a result
					// the user will see, it's rather about path elimination
					// By returning NONE, an array to be eliminated will be
					// filled with NONE, and thus eliminated
					return Ok(Value::None);
				}

				let path = &[Part::Destructure(parts.to_owned())];
				match stk.run(|stk| rec.current.get(stk, ctx, opt, doc, path)).await? {
					Value::Object(mut obj) => {
						obj.insert(field.to_raw(), v);
						Ok(Value::Object(obj))
					}
					Value::None => Ok(Value::None),
					v => Err(Error::Unreachable(format!(
						"Expected an object or none, found {}.",
						v.kindof()
					))),
				}
			}
		}
	}
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

pub trait Skip<'a> {
	fn skip(&'a self, amount: usize) -> &'a [Part];
}

impl<'a> Skip<'a> for &'a [Part] {
	fn skip(&'a self, amount: usize) -> &'a [Part] {
		match self.len() {
			0 => &[],
			_ => &self[amount..],
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum DestructurePart {
	All(Ident),
	Field(Ident),
	Aliased(Ident, Idiom),
	Destructure(Ident, Vec<DestructurePart>),
}

impl DestructurePart {
	pub fn field(&self) -> &Ident {
		match self {
			DestructurePart::All(v) => v,
			DestructurePart::Field(v) => v,
			DestructurePart::Aliased(v, _) => v,
			DestructurePart::Destructure(v, _) => v,
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
}

impl fmt::Display for DestructurePart {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			DestructurePart::All(fd) => write!(f, "{fd}.*"),
			DestructurePart::Field(fd) => write!(f, "{fd}"),
			DestructurePart::Aliased(fd, v) => write!(f, "{fd}: {v}"),
			DestructurePart::Destructure(fd, d) => {
				write!(f, "{fd}{}", Part::Destructure(d.clone()))
			}
		}
	}
}

// ------------------------------

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
}

impl TryInto<(u32, Option<u32>)> for Recurse {
	type Error = Error;
	fn try_into(self) -> Result<(u32, Option<u32>), Error> {
		let v = match self {
			Recurse::Fixed(v) => (v, Some(v)),
			Recurse::Range(min, max) => {
				let min = min.unwrap_or(1);
				(min, max)
			}
		};

		match v {
			(min, _) if min < 1 => Err(Error::InvalidBound {
				found: min.to_string(),
				expected: "at least 1".into(),
			}),
			(_, Some(max)) if max > (*IDIOM_RECURSION_LIMIT as u32) => Err(Error::InvalidBound {
				found: max.to_string(),
				expected: format!("{} at most", *IDIOM_RECURSION_LIMIT),
			}),
			v => Ok(v),
		}
	}
}

impl fmt::Display for Recurse {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Recurse::Fixed(v) => write!(f, "{v}"),
			Recurse::Range(beg, end) => match (beg, end) {
				(None, None) => write!(f, ".."),
				(Some(beg), None) => write!(f, "{beg}.."),
				(None, Some(end)) => write!(f, "..{end}"),
				(Some(beg), Some(end)) => write!(f, "{beg}..{end}"),
			},
		}
	}
}
