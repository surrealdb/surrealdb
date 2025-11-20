use std::fmt;

use futures::future::try_join_all;
use reblessive::tree::Stk;

use super::{ControlFlow, FlowResult, FlowResultExt as _};
use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, Idiom, Kind, Model, ModuleExecutable, Script, Value};
use crate::fmt::{EscapeKwFreeIdent, Fmt};
use crate::fnc;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) enum Function {
	Normal(String),
	Custom(String),
	Script(Script),
	Model(Model),
	Module(String, Option<String>),
	Silo {
		org: String,
		pkg: String,
		major: u32,
		minor: u32,
		patch: u32,
		sub: Option<String>,
	},
}

impl Function {
	/// Convert function call to a field name
	pub(crate) fn to_idiom(&self) -> Idiom {
		match self {
			// Safety: "function" does not contain null bytes"
			Self::Script(_) => Idiom::field("function".to_owned()),
			Self::Normal(f) => Idiom::field(f.to_owned()),
			Self::Custom(f) => Idiom::field(format!("fn::{f}")),
			Self::Model(m) => Idiom::field(m.to_string()),
			Self::Module(m, s) => match s {
				Some(s) => Idiom::field(format!("mod::{m}::{s}")),
				None => Idiom::field(format!("mod::{m}")),
			},
			Self::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => match sub {
				Some(s) => {
					Idiom::field(format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>::{s}"))
				}
				None => Idiom::field(format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>")),
			},
		}
	}

	/// Checks if this function invocation is writable
	pub fn read_only(&self) -> bool {
		match self {
			Self::Custom(_)
			| Self::Script(_)
			| Self::Module(_, _)
			| Self::Silo {
				..
			} => false,
			Self::Normal(f) => f != "api::invoke",
			Self::Model(_) => true,
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
			Function::Custom(s) => {
				// Get the full name of this function
				let name = format!("fn::{s}");
				// Check if this function is allowed
				ctx.check_allowed_function(name.as_str())?;
				// Get the function definition
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let val = ctx.tx().get_db_function(ns, db, s).await?;

				// Check permissions
				check_perms(stk, ctx, opt, doc, &name, &val.permissions).await?;
				// Validate the arguments
				validate_args(
					&name,
					&args,
					&val.args.iter().map(|(_, k)| k.clone()).collect::<Vec<Kind>>(),
				)?;
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
				// Validate the return value
				validate_return(name, val.returns.as_ref(), result)
			}
			Function::Module(module, sub) => {
				let mod_name = format!("mod::{module}");
				let fnc_name = match sub {
					Some(sub) => format!("{mod_name}::{sub}"),
					None => mod_name.clone(),
				};
				// Check if this module is allowed
				ctx.check_allowed_function(fnc_name.as_str())?;
				// Get the module definition
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let val = ctx.tx().get_db_module(ns, db, mod_name.as_str()).await?;

				// Check permissions
				check_perms(stk, ctx, opt, doc, &mod_name, &val.permissions).await?;

				// Get the executable & signature
				let executable: ModuleExecutable = val.executable.clone().into();
				let signature = executable.signature(ctx, &ns, &db, sub.as_deref()).await?;

				// Validate the arguments
				validate_args(&fnc_name, &args, &signature.args)?;

				// Run the module
				let result = executable.run(stk, ctx, opt, doc, args, sub.as_deref()).await?;

				// Validate the return value
				validate_return(fnc_name, signature.returns.as_ref(), result)
			}
			Function::Silo {
				org,
				pkg,
				major,
				minor,
				patch,
				sub,
			} => {
				let mod_name = format!("silo::{org}::{pkg}<{major}.{minor}.{patch}>");
				let fnc_name = match sub {
					Some(sub) => format!("{mod_name}::{sub}"),
					None => mod_name.clone(),
				};
				// Check if this module is allowed
				ctx.check_allowed_function(fnc_name.as_str())?;
				// Get the module definition
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let val = ctx.tx().get_db_module(ns, db, mod_name.as_str()).await?;

				// Check permissions
				check_perms(stk, ctx, opt, doc, &mod_name, &val.permissions).await?;

				// Get the executable & signature
				let executable: ModuleExecutable = val.executable.clone().into();
				let signature = executable.signature(ctx, &ns, &db, sub.as_deref()).await?;

				// Validate the arguments
				validate_args(&fnc_name, &args, &signature.args)?;

				// Run the module
				let result = executable.run(stk, ctx, opt, doc, args, sub.as_deref()).await?;

				// Validate the return value
				validate_return(fnc_name, signature.returns.as_ref(), result)
			}
		}
	}
}

///TODO(3.0): Remove after proper first class function support?

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct FunctionCall {
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
			Function::Normal(ref s) => {
				for (idx, s) in s.split("::").enumerate() {
					if idx != 0 {
						f.write_str("::")?;
					}
					s.fmt(f)?;
				}
				write!(f, "({})", Fmt::comma_separated(&self.arguments))
			}
			Function::Custom(ref s) => {
				f.write_str("fn")?;
				for s in s.split("::") {
					f.write_str("::")?;
					EscapeKwFreeIdent(s).fmt(f)?;
				}
				write!(f, "({})", Fmt::comma_separated(&self.arguments))
			}
			Function::Script(ref s) => {
				write!(f, "function({}) {{{s}}}", Fmt::comma_separated(&self.arguments))
			}
			Function::Model(ref m) => {
				write!(f, "{}({})", m, Fmt::comma_separated(&self.arguments))
			}
			Function::Module(ref m, ref s) => {
				f.write_str("mod")?;
				for s in m.split("::") {
					f.write_str("::")?;
					EscapeKwFreeIdent(s).fmt(f)?;
				}
				if let Some(s) = s {
					write!(f, "::{}", EscapeKwFreeIdent(s))?;
				}
				write!(f, "({})", Fmt::comma_separated(&self.arguments))
			}
			Function::Silo {
				ref org,
				ref pkg,
				ref major,
				ref minor,
				ref patch,
				ref sub,
			} => match sub {
				Some(s) => write!(
					f,
					"silo::{}::{}<{major}.{minor}.{patch}>::{}({})",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
					EscapeKwFreeIdent(s),
					Fmt::comma_separated(&self.arguments)
				),
				None => write!(
					f,
					"silo::{}::{}<{major}.{minor}.{patch}>({})",
					EscapeKwFreeIdent(org),
					EscapeKwFreeIdent(pkg),
					Fmt::comma_separated(&self.arguments)
				),
			},
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

async fn check_perms(
	stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	doc: Option<&CursorDoc>,
	name: &str,
	permissions: &Permission,
) -> FlowResult<()> {
	match permissions {
		Permission::Full => Ok(()),
		Permission::None => {
			Err(ControlFlow::from(anyhow::Error::new(Error::FunctionPermissions {
				name: name.to_string(),
			})))
		}
		Permission::Specific(e) => {
			// Disable permissions
			let opt = &opt.new_with_perms(false);
			// Process the PERMISSION clause
			if !stk.run(|stk| e.compute(stk, ctx, opt, doc)).await?.is_truthy() {
				Err(ControlFlow::from(anyhow::Error::new(Error::FunctionPermissions {
					name: name.to_string(),
				})))
			} else {
				Ok(())
			}
		}
	}
}

fn validate_args(name: &str, args: &[Value], sig: &[Kind]) -> FlowResult<()> {
	// Get the number of function arguments
	let max_args_len = sig.len();
	// Track the number of required arguments
	// Check for any final optional arguments
	let min_args_len = sig.iter().rev().fold(0, |acc, kind| {
		if kind.can_be_none() {
			if acc == 0 {
				0
			} else {
				acc + 1
			}
		} else {
			acc + 1
		}
	});
	// Check the necessary arguments are passed
	//TODO(planner): Move this check out of the call.
	if !(min_args_len..=max_args_len).contains(&args.len()) {
		return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidArguments {
			name: name.to_string(),
			message: match (min_args_len, max_args_len) {
				(1, 1) => String::from("The function expects 1 argument."),
				(r, t) if r == t => format!("The function expects {r} arguments."),
				(r, t) => format!("The function expects {r} to {t} arguments."),
			},
		})));
	}

	Ok(())
}

fn validate_return(name: String, return_kind: Option<&Kind>, result: Value) -> FlowResult<Value> {
	match return_kind {
		Some(kind) => result
			.coerce_to_kind(kind)
			.map_err(|e| Error::ReturnCoerce {
				name: name.to_string(),
				error: Box::new(e),
			})
			.map_err(anyhow::Error::new)
			.map_err(ControlFlow::from),
		None => Ok(result),
	}
}
