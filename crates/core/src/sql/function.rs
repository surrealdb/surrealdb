use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::BindingPower;
use crate::sql::script::Script;
use crate::sql::value::SqlValue;
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;


pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Function {
	Normal(String, Vec<SqlValue>),
	Custom(String, Vec<SqlValue>),
	Script(Script, Vec<SqlValue>),
	#[revision(
		end = 2,
		convert_fn = "convert_anonymous_arg_computation",
		fields_name = "OldAnonymousFields"
	)]
	Anonymous(SqlValue, Vec<SqlValue>),
	/// Fields are: the function object itself, it's arguments and whether the arguments are calculated.
	#[revision(start = 2)]
	Anonymous(SqlValue, Vec<SqlValue>, bool),
	// Add new variants here
}

impl Function {
	fn convert_anonymous_arg_computation(
		old: OldAnonymousFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(Function::Anonymous(old.0, old.1, false))
	}
}

impl From<Function> for crate::expr::Function {
	fn from(v: Function) -> Self {
		match v {
			Function::Normal(s, e) => Self::Normal(s, e.into_iter().map(Into::into).collect()),
			Function::Custom(s, e) => Self::Custom(s, e.into_iter().map(Into::into).collect()),
			Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
		}
	}
}

impl From<crate::expr::Function> for Function {
	fn from(v: crate::expr::Function) -> Self {
		match v {
			crate::expr::Function::Normal(s, e) => {
				Self::Normal(s, e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Custom(s, e) => {
				Self::Custom(s, e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
		}
	}
}

pub(crate) enum OptimisedAggregate {
	None,
	Count,
	CountFunction,
	MathMax,
	MathMin,
	MathSum,
	MathMean,
	TimeMax,
	TimeMin,
}

impl PartialOrd for Function {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl Function {
	/// Get function name if applicable
	pub fn name(&self) -> Option<&str> {
		match self {
			Self::Normal(n, _) => Some(n.as_str()),
			Self::Custom(n, _) => Some(n.as_str()),
			_ => None,
		}
	}
	/// Get function arguments if applicable
	pub fn args(&self) -> &[SqlValue] {
		match self {
			Self::Normal(_, a) => a,
			Self::Custom(_, a) => a,
			_ => &[],
		}
	}
	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Self::Anonymous(_, _, _) => "function".to_string().into(),
			Self::Script(_, _) => "function".to_string().into(),
			Self::Normal(f, _) => f.to_owned().into(),
			Self::Custom(f, _) => format!("fn::{f}").into(),
		}
	}
	/// Convert this function to an aggregate
	pub fn aggregate(&self, val: SqlValue) -> Result<Self> {
		match self {
			Self::Normal(n, a) => {
				let mut a = a.to_owned();
				match a.len() {
					0 => a.insert(0, val),
					_ => {
						a.remove(0);
						a.insert(0, val);
					}
				}
				Ok(Self::Normal(n.to_owned(), a))
			}
			_ => fail!("Encountered a non-aggregate function: {self:?}"),
		}
	}
	/// Check if this function is a custom function
	pub fn is_custom(&self) -> bool {
		matches!(self, Self::Custom(_, _))
	}

	/// Check if this function is a scripting function
	pub fn is_script(&self) -> bool {
		matches!(self, Self::Script(_, _))
	}

	/// Check if all arguments are static values
	pub fn is_static(&self) -> bool {
		match self {
			Self::Normal(_, a) => a.iter().all(SqlValue::is_static),
			_ => false,
		}
	}

	/// Check if this function is a closure function
	pub fn is_inline(&self) -> bool {
		matches!(self, Self::Anonymous(_, _, _))
	}

	/// Check if this function is a rolling function
	pub fn is_rolling(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "count" => true,
			Self::Normal(f, _) if f == "math::max" => true,
			Self::Normal(f, _) if f == "math::mean" => true,
			Self::Normal(f, _) if f == "math::min" => true,
			Self::Normal(f, _) if f == "math::sum" => true,
			Self::Normal(f, _) if f == "time::max" => true,
			Self::Normal(f, _) if f == "time::min" => true,
			_ => false,
		}
	}
	/// Check if this function is a grouping function
	pub fn is_aggregate(&self) -> bool {
		match self {
			Self::Normal(f, _) if f == "array::distinct" => true,
			Self::Normal(f, _) if f == "array::first" => true,
			Self::Normal(f, _) if f == "array::flatten" => true,
			Self::Normal(f, _) if f == "array::group" => true,
			Self::Normal(f, _) if f == "array::last" => true,
			Self::Normal(f, _) if f == "count" => true,
			Self::Normal(f, _) if f == "math::bottom" => true,
			Self::Normal(f, _) if f == "math::interquartile" => true,
			Self::Normal(f, _) if f == "math::max" => true,
			Self::Normal(f, _) if f == "math::mean" => true,
			Self::Normal(f, _) if f == "math::median" => true,
			Self::Normal(f, _) if f == "math::midhinge" => true,
			Self::Normal(f, _) if f == "math::min" => true,
			Self::Normal(f, _) if f == "math::mode" => true,
			Self::Normal(f, _) if f == "math::nearestrank" => true,
			Self::Normal(f, _) if f == "math::percentile" => true,
			Self::Normal(f, _) if f == "math::sample" => true,
			Self::Normal(f, _) if f == "math::spread" => true,
			Self::Normal(f, _) if f == "math::stddev" => true,
			Self::Normal(f, _) if f == "math::sum" => true,
			Self::Normal(f, _) if f == "math::top" => true,
			Self::Normal(f, _) if f == "math::trimean" => true,
			Self::Normal(f, _) if f == "math::variance" => true,
			Self::Normal(f, _) if f == "time::max" => true,
			Self::Normal(f, _) if f == "time::min" => true,
			_ => false,
		}
	}
	pub(crate) fn get_optimised_aggregate(&self) -> OptimisedAggregate {
		match self {
			Self::Normal(f, v) if f == "count" => {
				if v.is_empty() {
					OptimisedAggregate::Count
				} else {
					OptimisedAggregate::CountFunction
				}
			}
			Self::Normal(f, _) if f == "math::max" => OptimisedAggregate::MathMax,
			Self::Normal(f, _) if f == "math::mean" => OptimisedAggregate::MathMean,
			Self::Normal(f, _) if f == "math::min" => OptimisedAggregate::MathMin,
			Self::Normal(f, _) if f == "math::sum" => OptimisedAggregate::MathSum,
			Self::Normal(f, _) if f == "time::max" => OptimisedAggregate::TimeMax,
			Self::Normal(f, _) if f == "time::min" => OptimisedAggregate::TimeMin,
			_ => OptimisedAggregate::None,
		}
	}

	pub(crate) fn is_count_all(&self) -> bool {
		matches!(self, Self::Normal(f, p) if f == "count" && p.is_empty() )
	}
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Normal(s, e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
			Self::Custom(s, e) => write!(f, "fn::{s}({})", Fmt::comma_separated(e)),
			Self::Script(s, e) => write!(f, "function({}) {{{s}}}", Fmt::comma_separated(e)),
			Self::Anonymous(p, e, _) => {
				if BindingPower::for_value(p) < BindingPower::Postfix {
					write!(f, "({p})")?;
				} else {
					write!(f, "{p}")?;
				}
				write!(f, "({})", Fmt::comma_separated(e))
			}
		}
	}
}
