use std::fmt;
use std::fmt::Write;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::cnf::IDIOM_RECURSION_LIMIT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::exe::try_join_all_buffered;
use crate::expr::fmt::{Fmt, is_pretty, pretty_indent};
use crate::expr::idiom::recursion::{
	self, Recursion, clean_iteration, compute_idiom_recursion, is_final,
};
use crate::expr::{Expr, FlowResultExt as _, Ident, Idiom, Literal, Lookup, Value};
use crate::val::{Array, RecordId};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Part {
	All,
	Flatten,
	Last,
	First,
	Field(Ident),
	Where(Expr),
	Lookup(Lookup),
	Value(Expr),
	/// TODO: Remove, start and move it out of part to elimite invalid state.
	Start(Expr),
	Method(String, Vec<Expr>),
	Destructure(Vec<DestructurePart>),
	Optional,
	Recurse(Recurse, Option<Idiom>, Option<RecurseInstruction>),
	Doc,
	RepeatRecurse,
}

impl Part {
	/// Returns a part which is equivalent to `.bla` if called with string
	/// `bla`.
	pub fn field(field: String) -> Option<Self> {
		Some(Part::Field(Ident::new(field)?))
	}

	/// Returns a part which is equivalent to `[1]` if called with integer `1`.
	pub fn index_int(idx: i64) -> Self {
		Part::Value(Expr::Literal(Literal::Integer(idx)))
	}

	pub(crate) fn is_index(&self) -> bool {
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
	#[deprecated(since = "3.0.0")]
	pub(crate) fn as_old_index(&self) -> Option<usize> {
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
	pub(crate) fn read_only(&self) -> bool {
		match self {
			Part::Start(v) => v.read_only(),
			Part::Where(v) => v.read_only(),
			Part::Value(v) => v.read_only(),
			Part::Method(_, v) => v.iter().all(Expr::read_only),
			_ => true,
		}
	}
	/// Returns a yield if an alias is specified
	pub(crate) fn alias(&self) -> Option<&Idiom> {
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

	pub(crate) fn to_raw_string(&self) -> String {
		match self {
			Part::Start(v) => v.to_raw_string(),
			Part::Field(v) => format!(".{}", v.to_raw_string()),
			_ => self.to_string(),
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
			Part::Where(v) => write!(f, "[WHERE {v}]"),
			Part::Lookup(v) => write!(f, "{v}"),
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
			Part::Recurse(v, nest, instruction) => {
				write!(f, ".{{{v}")?;
				if let Some(instruction) = instruction {
					write!(f, "+{instruction}")?;
				}
				write!(f, "}}")?;

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
	) -> Result<Value> {
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
	) -> Result<Value> {
		match self {
			Self::Repeat => compute_idiom_recursion(stk, ctx, opt, doc, rec).await,
			Self::Destructure {
				parts,
				field,
				before,
				plan,
				after,
			} => {
				let v = stk
					.run(|stk| rec.current.get(stk, ctx, opt, doc, before))
					.await
					.catch_return()?;
				let v = plan.compute(stk, ctx, opt, doc, rec.with_current(&v)).await?;
				let v = stk.run(|stk| v.get(stk, ctx, opt, doc, after)).await.catch_return()?;
				let v = clean_iteration(v);

				if rec.iterated < rec.min && is_final(&v) {
					// We do not use get_final here, because it's not a result
					// the user will see, it's rather about path elimination
					// By returning NONE, an array to be eliminated will be
					// filled with NONE, and thus eliminated
					return Ok(Value::None);
				}

				let path = &[Part::Destructure(parts.to_owned())];
				match stk
					.run(|stk| rec.current.get(stk, ctx, opt, doc, path))
					.await
					.catch_return()?
				{
					Value::Object(mut obj) => {
						obj.insert(field.to_raw_string(), v);
						Ok(Value::Object(obj))
					}
					Value::None => Ok(Value::None),
					v => Err(anyhow::Error::new(Error::unreachable(format_args!(
						"Expected an object or none, found {}.",
						v.kind_of()
					)))),
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
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

	pub fn idiom(&self) -> Idiom {
		Idiom(self.path())
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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Recurse {
	Fixed(u32),
	Range(Option<u32>, Option<u32>),
}

impl TryInto<(u32, Option<u32>)> for Recurse {
	type Error = anyhow::Error;

	fn try_into(self) -> Result<(u32, Option<u32>)> {
		let v = match self {
			Recurse::Fixed(v) => (v, Some(v)),
			Recurse::Range(min, max) => {
				let min = min.unwrap_or(1);
				(min, max)
			}
		};

		match v {
			(min, _) if min < 1 => Err(anyhow::Error::new(Error::InvalidBound {
				found: min.to_string(),
				expected: "at least 1".into(),
			})),
			(_, Some(max)) if max > (*IDIOM_RECURSION_LIMIT as u32) => {
				Err(anyhow::Error::new(Error::InvalidBound {
					found: max.to_string(),
					expected: format!("{} at most", *IDIOM_RECURSION_LIMIT),
				}))
			}
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

#[allow(clippy::too_many_arguments)]
async fn walk_paths(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	recursion: Recursion<'_>,
	finished: &mut Vec<Value>,
	inclusive: bool,
	expects: Option<&Value>,
) -> Result<Value> {
	let mut open: Vec<Value> = vec![];
	let paths = match recursion.current {
		Value::Array(v) => &v.0,
		v => &vec![v.to_owned()],
	};

	for path in paths.iter() {
		let path = match path {
			Value::Array(v) => &v.0,
			v => &vec![v.to_owned()],
		};
		let Some(last) = path.last() else {
			continue;
		};
		let res =
			stk.run(|stk| last.get(stk, ctx, opt, doc, recursion.path)).await.catch_return()?;

		if recursion::is_final(&res) || &res == last {
			if expects.is_none()
				&& (recursion.iterated > 1 || inclusive)
				&& recursion.iterated >= recursion.min
			{
				finished.push(path.to_owned().into());
			}
			continue;
		}

		let steps = match res {
			Value::Array(v) => v.0,
			v => vec![v],
		};

		let reached_max = recursion.max.is_some_and(|max| recursion.iterated >= max);
		for step in steps.iter() {
			let val = if recursion.iterated == 1 && !inclusive {
				Value::from(vec![step.to_owned()])
			} else {
				let mut path = path.to_owned();
				path.push(step.to_owned());
				Value::from(path)
			};
			if let Some(expects) = expects {
				if step == expects {
					let steps = match val {
						Value::Array(v) => v.0,
						v => vec![v],
					};
					for step in steps {
						finished.push(step);
					}
					return Ok(Value::None);
				}
			}
			if reached_max {
				if (Option::<&Value>::None).is_none() {
					finished.push(val);
				}
			} else {
				open.push(val);
			}
		}
	}

	Ok(Value::Array(Array(open)))
}

impl RecurseInstruction {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		rec: Recursion<'_>,
		finished: &mut Vec<Value>,
	) -> Result<Value> {
		match self {
			Self::Path {
				inclusive,
			} => walk_paths(stk, ctx, opt, doc, rec, finished, *inclusive, None).await,
			Self::Shortest {
				expects,
				inclusive,
			} => {
				let expects = stk
					.run(|stk| expects.compute(stk, ctx, opt, doc))
					.await
					.catch_return()?
					.coerce_to::<RecordId>()?
					.into();
				walk_paths(stk, ctx, opt, doc, rec, finished, *inclusive, Some(&expects)).await
			}
			Self::Collect {
				inclusive,
			} => {
				// If we are inclusive, we add the starting point to the collection
				if rec.iterated == 1 && *inclusive {
					match rec.current {
						Value::Array(v) => {
							for v in v.iter() {
								if !finished.contains(v) {
									finished.push(v.to_owned());
								}
							}
						}
						v => {
							if !finished.contains(v) {
								finished.push(v.to_owned());
							}
						}
					};
				}

				// Apply the recursed path to the current values
				let res = stk
					.run(|stk| rec.current.get(stk, ctx, opt, doc, rec.path))
					.await
					.catch_return()?;
				// Clean the iteration
				let res = clean_iteration(res);

				// Persist any new values from the result
				match &res {
					Value::Array(v) => {
						for v in v.iter() {
							if !finished.contains(v) {
								finished.push(v.to_owned());
							}
						}
					}
					v => {
						if !finished.contains(v) {
							finished.push(v.to_owned());
						}
					}
				};

				// Continue
				Ok(res)
			}
		}
	}
}

impl fmt::Display for RecurseInstruction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Path {
				inclusive,
			} => {
				write!(f, "path")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
			Self::Collect {
				inclusive,
			} => {
				write!(f, "collect")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
			Self::Shortest {
				expects,
				inclusive,
			} => {
				write!(f, "shortest={expects}")?;

				if *inclusive {
					write!(f, "+inclusive")?;
				}

				Ok(())
			}
		}
	}
}
