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

use super::Ident;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Function {
	Normal(String, Vec<SqlValue>),
	#[revision(end = 3, convert_fn = "convert_custom_add_version", fields_name = "OldCustomFields")]
	Custom(String, Vec<SqlValue>),
	#[revision(start = 3)]
	Custom(CustomFunctionName, Vec<SqlValue>),
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
	#[revision(start = 3)]
	Silo {
		organisation: Ident,
		package: Ident,
		version: FunctionVersion,
		submodule: Option<Ident>,
		args: Vec<SqlValue>,
	},
	// Add new variants here
}

impl Function {
	fn convert_anonymous_arg_computation(
		old: OldAnonymousFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(Function::Anonymous(old.0, old.1, false))
	}

	fn convert_custom_add_version(
		old: OldCustomFields,
		_revision: u16,
	) -> Result<Self, revision::Error> {
		Ok(Function::Custom(
			CustomFunctionName {
				name: Ident(old.0),
				version: None,
				submodule: None,
			},
			old.1,
		))
	}
}

impl From<Function> for crate::expr::Function {
	fn from(v: Function) -> Self {
		match v {
			Function::Normal(s, e) => Self::Normal(s, e.into_iter().map(Into::into).collect()),
			Function::Custom(s, e) => {
				Self::Custom(s.into(), e.into_iter().map(Into::into).collect())
			}
			Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
			Function::Silo {
				organisation,
				package,
				version,
				submodule,
				args,
			} => Self::Silo {
				organisation: organisation.into(),
				package: package.into(),
				version: version.into(),
				submodule: submodule.map(Into::into),
				args: args.into_iter().map(Into::into).collect(),
			},
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
				Self::Custom(s.into(), e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Script(s, e) => {
				Self::Script(s.into(), e.into_iter().map(Into::into).collect())
			}
			crate::expr::Function::Anonymous(p, e, b) => {
				Self::Anonymous(p.into(), e.into_iter().map(Into::into).collect(), b)
			}
			crate::expr::Function::Silo {
				organisation,
				package,
				version,
				submodule,
				args,
			} => Self::Silo {
				organisation: organisation.into(),
				package: package.into(),
				version: version.into(),
				submodule: submodule.map(Into::into),
				args: args.into_iter().map(Into::into).collect(),
			},
		}
	}
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
			Self::Custom(n, _) => Some(n.name.as_str()),
			_ => None,
		}
	}
	/// Get function arguments if applicable
	pub fn args(&self) -> &[SqlValue] {
		match self {
			Self::Normal(_, a) => a,
			Self::Custom(_, a) => a,
			Self::Silo {
				args,
				..
			} => args,
			_ => &[],
		}
	}
	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			Self::Anonymous(_, _, _) => "function".to_string().into(),
			Self::Script(_, _) => "function".to_string().into(),
			Self::Normal(f, _) => f.to_owned().into(),
			Self::Custom(f, _) => f.to_string().into(),
			Self::Silo {
				organisation,
				package,
				version,
				submodule,
				..
			} => {
				if let Some(submodule) = submodule {
					format!("silo::{organisation}::{package}<{version}>::{submodule}").into()
				} else {
					format!("silo::{organisation}::{package}<{version}>").into()
				}
			}
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
}

impl fmt::Display for Function {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Normal(s, e) => write!(f, "{s}({})", Fmt::comma_separated(e)),
			Self::Custom(s, e) => write!(f, "fn::{s}({})", Fmt::comma_separated(e)),
			Self::Silo {
				organisation,
				package,
				version,
				submodule,
				args,
			} => {
				if let Some(submodule) = submodule {
					write!(
						f,
						"silo::{organisation}::{package}<{version}>::{submodule}({})",
						Fmt::comma_separated(args)
					)
				} else {
					write!(
						f,
						"silo::{organisation}::{package}<{version}>({})",
						Fmt::comma_separated(args)
					)
				}
			}
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
/// Does not store the function's prefix
pub struct CustomFunctionName {
	pub name: Ident,
	pub version: Option<FunctionVersion>,
	pub submodule: Option<Ident>,
}

impl CustomFunctionName {
	pub fn new(name: Ident, version: Option<FunctionVersion>, submodule: Option<Ident>) -> Self {
		CustomFunctionName {
			name,
			version,
			submodule,
		}
	}
}

impl From<CustomFunctionName> for crate::expr::CustomFunctionName {
	fn from(v: CustomFunctionName) -> Self {
		Self {
			name: v.name.into(),
			version: v.version.map(Into::into),
			submodule: v.submodule.map(Into::into),
		}
	}
}

impl From<crate::expr::CustomFunctionName> for CustomFunctionName {
	fn from(v: crate::expr::CustomFunctionName) -> Self {
		Self {
			name: v.name.into(),
			version: v.version.map(Into::into),
			submodule: v.submodule.map(Into::into),
		}
	}
}

impl fmt::Display for CustomFunctionName {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.name.0)?;
		match (&self.version, &self.submodule) {
			(Some(version), None) => write!(f, "<{version}>")?,
			(Some(version), Some(submodule)) => write!(f, "<{version}>::{}", submodule.0)?,
			(None, Some(submodule)) => write!(f, "<latest>::{}", submodule.0)?,
			_ => (),
		};

		Ok(())
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
/// Does not store the function's prefix
pub enum FunctionVersion {
	#[default]
	Latest,
	Major(u32),
	Minor(u32, u32),
	Patch(u32, u32, u32),
}

impl FunctionVersion {
	pub(crate) fn is_patch(&self) -> bool {
		matches!(self, Self::Patch(_, _, _))
	}

	pub(crate) fn kind(&self) -> &str {
		match self {
			Self::Latest => "latest",
			Self::Major(_) => "major",
			Self::Minor(_, _) => "minor",
			Self::Patch(_, _, _) => "patch",
		}
	}
}

impl From<FunctionVersion> for crate::expr::FunctionVersion {
	fn from(v: FunctionVersion) -> Self {
		match v {
			FunctionVersion::Latest => Self::Latest,
			FunctionVersion::Major(x) => Self::Major(x),
			FunctionVersion::Minor(x, y) => Self::Minor(x, y),
			FunctionVersion::Patch(x, y, z) => Self::Patch(x, y, z),
		}
	}
}

impl From<crate::expr::FunctionVersion> for FunctionVersion {
	fn from(v: crate::expr::FunctionVersion) -> Self {
		match v {
			crate::expr::FunctionVersion::Latest => Self::Latest,
			crate::expr::FunctionVersion::Major(x) => Self::Major(x),
			crate::expr::FunctionVersion::Minor(x, y) => Self::Minor(x, y),
			crate::expr::FunctionVersion::Patch(x, y, z) => Self::Patch(x, y, z),
		}
	}
}

impl fmt::Display for FunctionVersion {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Latest => write!(f, "latest"),
			Self::Major(a) => write!(f, "{a}"),
			Self::Minor(a, b) => write!(f, "{a}.{b}"),
			Self::Patch(a, b, c) => write!(f, "{a}.{b}.{c}"),
		}
	}
}
