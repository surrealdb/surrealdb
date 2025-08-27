use std::fmt;

use futures::future::try_join_all;
use reblessive::tree::Stk;

use super::{ControlFlow, FlowResult, FlowResultExt as _, Kind};
use crate::catalog::Permission;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::Fmt;
use crate::expr::{Expr, Ident, Idiom, Model, Script, Value};
use crate::fnc;
use crate::iam::Action;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
}

impl Function {
	/// Get function name if applicable
	pub fn name(&self) -> Option<&str> {
		match self {
			Self::Normal(n) => Some(n.as_str()),
			Self::Custom(n) => Some(n.as_str()),
			_ => None,
		}
	}

	/// Convert function call to a field name
	pub fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field(unsafe { Ident::new_unchecked("function".to_owned()) }),
			Self::Normal(f) => Idiom::field(unsafe { Ident::new_unchecked(f.to_owned()) }),
			Self::Custom(f) => Idiom::field(unsafe { Ident::new_unchecked(format!("fn::{f}")) }),
			Self::Model(m) => Idiom::field(unsafe { Ident::new_unchecked(m.to_string()) }),
		}
	}
	/// Checks if this function invocation is writable
	pub fn read_only(&self) -> bool {
		match self {
			Self::Custom(_) | Self::Script(_) => false,
			Self::Normal(f) => f != "api::invoke",
			Self::Model(_) => true,
		}
	}

	/// Check if this function is a grouping function
	pub fn is_aggregate(&self) -> bool {
		match self {
			Self::Normal(f) if f == "array::distinct" => true,
			Self::Normal(f) if f == "array::first" => true,
			Self::Normal(f) if f == "array::flatten" => true,
			Self::Normal(f) if f == "array::group" => true,
			Self::Normal(f) if f == "array::last" => true,
			Self::Normal(f) if f == "count" => true,
			Self::Normal(f) if f == "math::bottom" => true,
			Self::Normal(f) if f == "math::interquartile" => true,
			Self::Normal(f) if f == "math::max" => true,
			Self::Normal(f) if f == "math::mean" => true,
			Self::Normal(f) if f == "math::median" => true,
			Self::Normal(f) if f == "math::midhinge" => true,
			Self::Normal(f) if f == "math::min" => true,
			Self::Normal(f) if f == "math::mode" => true,
			Self::Normal(f) if f == "math::nearestrank" => true,
			Self::Normal(f) if f == "math::percentile" => true,
			Self::Normal(f) if f == "math::sample" => true,
			Self::Normal(f) if f == "math::spread" => true,
			Self::Normal(f) if f == "math::stddev" => true,
			Self::Normal(f) if f == "math::sum" => true,
			Self::Normal(f) if f == "math::top" => true,
			Self::Normal(f) if f == "math::trimean" => true,
			Self::Normal(f) if f == "math::variance" => true,
			Self::Normal(f) if f == "time::max" => true,
			Self::Normal(f) if f == "time::min" => true,
			_ => false,
		}
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
		args: Vec<Value>,
	) -> FlowResult<Value> {
		match self {
			Function::Normal(s) => {
				// Check this function is allowed
				ctx.check_allowed_function(s)?;
				// Run the normal function
				Ok(fnc::run(stk, ctx, opt, doc, s, args).await?)
			}
			Function::Custom(s) => {
				// Get the full name of this function
				let name = format!("fn::{s}");
				// Check this function is allowed
				ctx.check_allowed_function(name.as_str())?;
				// Get the function definition
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let val = ctx.tx().get_db_function(ns, db, s).await?;
				// Check permissions
				if opt.check_perms(Action::View)? {
					match &val.permissions {
						Permission::Full => (),
						Permission::None => {
							return Err(ControlFlow::from(anyhow::Error::new(
								Error::FunctionPermissions {
									name: s.to_owned(),
								},
							)));
						}
						Permission::Specific(e) => {
							// Disable permissions
							let opt = &opt.new_with_perms(false);
							// Process the PERMISSION clause
							if !stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?.is_truthy() {
								return Err(ControlFlow::from(anyhow::Error::new(
									Error::FunctionPermissions {
										name: s.to_owned(),
									},
								)));
							}
						}
					}
				}
				// Get the number of function arguments
				let max_args_len = val.args.len();
				// Track the number of required arguments
				// Check for any final optional arguments
				let min_args_len =
					val.args.iter().rev().map(|x| &x.1).fold(0, |acc, kind| match kind {
						Kind::Option(_) | Kind::Any => {
							if acc == 0 {
								0
							} else {
								acc + 1
							}
						}
						_ => acc + 1,
					});
				// Check the necessary arguments are passed
				//TODO(planner): Move this check out of the call.
				if !(min_args_len..=max_args_len).contains(&args.len()) {
					return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidArguments {
						name: format!("fn::{}", val.name.as_str()),
						message: match (min_args_len, max_args_len) {
							(1, 1) => String::from("The function expects 1 argument."),
							(r, t) if r == t => format!("The function expects {r} arguments."),
							(r, t) => format!("The function expects {r} to {t} arguments."),
						},
					})));
				}
				// Compute the function arguments
				// Duplicate context
				let mut ctx = MutableContext::new_isolated(ctx);
				// Process the function arguments
				for (val, (name, kind)) in args.into_iter().zip(&val.args) {
					ctx.add_value(
						name.clone(),
						val.coerce_to_kind(kind)
							.map_err(Error::from)
							.map_err(anyhow::Error::new)?
							.into(),
					);
				}
				let ctx = ctx.freeze();
				// Run the custom function
				let result =
					stk.run(|stk| val.block.compute(stk, &ctx, opt, doc)).await.catch_return()?;

				if let Some(ref returns) = val.returns {
					result
						.coerce_to_kind(returns)
						.map_err(|e| Error::ReturnCoerce {
							name: val.name.to_string(),
							error: Box::new(e),
						})
						.map_err(anyhow::Error::new)
						.map_err(ControlFlow::from)
				} else {
					Ok(result)
				}
			}
			#[cfg_attr(not(feature = "scripting"), expect(unused_variables))]
			Function::Script(s) => {
				#[cfg(feature = "scripting")]
				{
					// Check if scripting is allowed
					ctx.check_allowed_scripting()?;
					// Run the script function
					fnc::script::run(ctx, opt, doc, &s.0, args).await.map_err(ControlFlow::Err)
				}
				#[cfg(not(feature = "scripting"))]
				{
					Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidScript {
						message: String::from("Embedded functions are not enabled."),
					})))
				}
			}
			Function::Model(m) => m.compute(stk, ctx, opt, doc, args).await,
		}
	}
}

///TODO(3.0): Remove after proper first class function support?

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FunctionCall {
	pub receiver: Function,
	pub arguments: Vec<Expr>,
}

impl FunctionCall {
	/// Returns if this expression type object can do any writes.
	pub fn read_only(&self) -> bool {
		self.receiver.read_only() && self.arguments.iter().all(|x| x.read_only())
	}
}

impl fmt::Display for FunctionCall {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.receiver {
			Function::Normal(ref s) => write!(f, "{s}({})", Fmt::comma_separated(&self.arguments)),
			Function::Custom(ref s) => {
				write!(f, "fn::{s}({})", Fmt::comma_separated(&self.arguments))
			}
			Function::Script(ref s) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(&self.arguments))
			}
			Function::Model(ref m) => {
				write!(f, "{}({})", m, Fmt::comma_separated(&self.arguments))
			}
		}
	}
}

impl FunctionCall {
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
		// Compute the function arguments
		let args = stk
			.scope(|scope| {
				try_join_all(
					self.arguments.iter().map(|v| scope.run(|stk| v.compute(stk, ctx, opt, doc))),
				)
			})
			.await?;
		// Process the function type
		self.receiver.compute(stk, ctx, opt, doc, args).await
	}
}
