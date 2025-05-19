use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc;
use crate::iam::Action;
use crate::sql::fmt::Fmt;
use crate::sql::idiom::Idiom;
use crate::sql::operator::BindingPower;
use crate::sql::script::Script;
use crate::sql::value::Value;
use crate::sql::Permission;
use futures::future::try_join_all;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

use super::statements::info::InfoStructure;
use super::{ControlFlow, FlowResult, Ident, Kind};

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Function";

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Function")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Function {
	Normal(String, Vec<Value>),
	#[revision(
		end = 3,
		convert_fn = "convert_custom_add_version",
		fields_name = "OldCustomFields"
	)]
	Custom(String, Vec<Value>),
	#[revision(start = 3)]
	Custom(CustomFunctionName, Vec<Value>),
	Script(Script, Vec<Value>),
	#[revision(
		end = 2,
		convert_fn = "convert_anonymous_arg_computation",
		fields_name = "OldAnonymousFields"
	)]
	Anonymous(Value, Vec<Value>),
	#[revision(start = 2)]
	Anonymous(Value, Vec<Value>, bool),
	#[revision(start = 3)]
	Silo {
		organisation: Ident,
		package: Ident,
		version: FunctionVersion,
		submodule: Option<Ident>,
		args: Vec<Value>,
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
			Self::Custom(n, _) => Some(n.name.as_str()),
			_ => None,
		}
	}
	/// Get function arguments if applicable
	pub fn args(&self) -> &[Value] {
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
	/// Checks if this function invocation is writable
	pub fn writeable(&self) -> bool {
		match self {
			Self::Custom(_, _) => true,
			Self::Silo {
				..
			} => true,
			Self::Script(_, _) => true,
			Self::Normal(f, _) if f == "api::invoke" => true,
			_ => self.args().iter().any(Value::writeable),
		}
	}
	/// Convert this function to an aggregate
	pub fn aggregate(&self, val: Value) -> Result<Self, Error> {
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
			_ => Err(fail!("Encountered a non-aggregate function: {self:?}")),
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
			Self::Normal(_, a) => a.iter().all(Value::is_static),
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

impl Function {
	/// Process this type returning a computed simple Value
	///
	/// Was marked recursive
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Ensure futures are run
		let opt = &opt.new_with_futures(true);
		// Process the function type
		match self {
			Self::Normal(s, x) => {
				// Check this function is allowed
				ctx.check_allowed_function(s)?;
				// Compute the function arguments
				let a = stk
					.scope(|scope| {
						try_join_all(
							x.iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
						)
					})
					.await?;
				// Run the normal function
				Ok(fnc::run(stk, ctx, opt, doc, s, a).await?)
			}
			Self::Anonymous(v, x, args_computed) => {
				let val = match v {
					c @ Value::Closure(_) => c.clone(),
					Value::Param(p) => ctx.value(p).cloned().unwrap_or(Value::None),
					Value::Block(_) | Value::Subquery(_) | Value::Idiom(_) | Value::Function(_) => {
						stk.run(|stk| v.compute(stk, ctx, opt, doc)).await?
					}
					_ => Value::None,
				};

				match val {
					Value::Closure(closure) => {
						// Compute the function arguments
						let a =
							match args_computed {
								true => x.clone(),
								false => {
									stk.scope(|scope| {
										try_join_all(x.iter().map(|v| {
											scope.run(|stk| v.compute(stk, ctx, opt, doc))
										}))
									})
									.await?
								}
							};

						Ok(stk.run(|stk| closure.compute(stk, ctx, opt, doc, a)).await?)
					}
					v => Err(ControlFlow::from(Error::InvalidFunction {
						name: "ANONYMOUS".to_string(),
						message: format!("'{}' is not a function", v.kindof()),
					})),
				}
			}
			fnc @ Self::Silo {
				..
			}
			| fnc @ Self::Custom(_, _) => {
				let (name, key, x, version, submodule) = match fnc {
					Self::Custom(s, x) => {
						let name = s.to_string();
						(name, s.name.0.clone(), x, s.version.as_ref(), s.submodule.as_ref())
					}
					Self::Silo {
						organisation,
						package,
						version,
						submodule,
						args,
					} => {
						let name = if let Some(submodule) = submodule {
							format!("silo::{organisation}::{package}::<{version}>::{submodule}")
						} else {
							format!("silo::{organisation}::{package}::<{version}>")
						};
						(
							name,
							format!("{organisation}::{package}"),
							args,
							Some(version),
							submodule.as_ref(),
						)
					}
					_ => {
						return Err(fail!(
							"Expected to find either custom or silo function as previously matched"
						)
						.into())
					}
				};
				// Check this function is allowed
				ctx.check_allowed_function(name.as_str())?;
				// Get the function definition
				let (ns, db) = opt.ns_db()?;
				let val = if matches!(fnc, Function::Silo { .. }) {
					match ctx.tx().get_silo_function(ns, db, &key).await {
						Err(Error::SiNotFound {
							name,
						}) => {
							let name = CustomFunctionName {
								name: Ident(name),
								version: version.cloned(),
								submodule: submodule.cloned(),
							};

							Err(Error::SiNotFound {
								name: name.to_string(),
							})
						}
						x => x,
					}?
				} else {
					match ctx.tx().get_db_function(ns, db, &key).await {
						Err(Error::FcNotFound {
							name,
						}) => {
							let name = CustomFunctionName {
								name: Ident(name),
								version: version.cloned(),
								submodule: submodule.cloned(),
							};

							Err(Error::FcNotFound {
								name: name.to_string(),
							})
						}
						x => x,
					}?
				};
				// Check permissions
				if opt.check_perms(Action::View)? {
					match &val.permissions {
						Permission::Full => (),
						Permission::None => {
							return Err(ControlFlow::from(Error::FunctionPermissions {
								name,
							}))
						}
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Process the PERMISSION clause
							if !stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?.is_truthy() {
								return Err(ControlFlow::from(Error::FunctionPermissions {
									name,
								}));
							}
						}
					}
				}
				let args = val.args().await?;
				// Get the number of function arguments
				let max_args_len = args.len();
				// Track the number of required arguments
				let mut min_args_len = 0;
				// Check for any final optional arguments
				args.iter().rev().for_each(|(_, kind)| match kind {
					Kind::Option(_) if min_args_len == 0 => {}
					Kind::Any if min_args_len == 0 => {}
					_ => min_args_len += 1,
				});
				// Check the necessary arguments are passed
				if x.len() < min_args_len || max_args_len < x.len() {
					return Err(ControlFlow::from(Error::InvalidArguments {
						name: format!("fn::{}", val.name),
						message: match (min_args_len, max_args_len) {
							(1, 1) => String::from("The function expects 1 argument."),
							(r, t) if r == t => format!("The function expects {r} arguments."),
							(r, t) => format!("The function expects {r} to {t} arguments."),
						},
					}));
				}
				// Compute the function arguments
				let a = stk
					.scope(|scope| {
						try_join_all(
							x.iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
						)
					})
					.await?;
				// Duplicate context
				let mut ctx = MutableContext::new_isolated(ctx);
				// Process the function arguments
				for (val, (name, kind)) in a.into_iter().zip(args) {
					ctx.add_value(
						name.to_raw(),
						val.coerce_to_kind(kind).map_err(Error::from)?.into(),
					);
				}
				let ctx = ctx.freeze();
				// Run the custom function
				let result =
					stk.run(|stk| val.execute(stk, &ctx, opt, doc, version, submodule)).await?;

				if let Some(ref returns) = val.returns().await? {
					result
						.coerce_to_kind(returns)
						.map_err(|e| Error::ReturnCoerce {
							name: val.name.to_string(),
							error: Box::new(e),
						})
						.map_err(ControlFlow::from)
				} else {
					Ok(result)
				}
			}
			#[cfg_attr(not(feature = "scripting"), expect(unused_variables))]
			Self::Script(s, x) => {
				#[cfg(feature = "scripting")]
				{
					// Check if scripting is allowed
					ctx.check_allowed_scripting()?;
					// Compute the function arguments
					let a = stk
						.scope(|scope| {
							try_join_all(
								x.iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
							)
						})
						.await?;
					// Run the script function
					Ok(fnc::script::run(ctx, opt, doc, s, a).await?)
				}
				#[cfg(not(feature = "scripting"))]
				{
					Err(ControlFlow::Err(Box::new(Error::InvalidScript {
						message: String::from("Embedded functions are not enabled."),
					})))
				}
			}
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

impl InfoStructure for CustomFunctionName {
	fn structure(self) -> Value {
		Value::from(self.to_string())
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
