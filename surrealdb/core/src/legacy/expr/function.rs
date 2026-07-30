use futures::future::try_join_all;
use reblessive::tree::Stk;

use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt as _;
use crate::exec::Error as ExecError;
use crate::expr::function::{Function, FunctionCall};
use crate::expr::{ControlFlow, Error as ExprError, FlowResult, Kind, ModuleExecutable};
use crate::fnc;
use crate::iam::{Action, AuthLimit};
use crate::val::Value;

#[instrument(level = "trace", name = "Function::compute", skip_all)]
pub(crate) async fn function_compute(
	this: &Function,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
) -> FlowResult<Value> {
	match this {
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
				Err(ControlFlow::Err(anyhow::Error::new(ExecError::InvalidScript {
					message: String::from("Embedded functions are not enabled."),
				})))
			}
		}
		Function::Model(m) => crate::legacy::model_compute(m, stk, ctx, opt, doc, args).await,
		Function::Custom(s) => {
			// Get the full name of this function
			let name = format!("fn::{s}");
			// Check if this function is allowed
			ctx.check_allowed_function(name.as_str())?;
			// Get the function definition
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			let val = ctx.tx().get_db_function(ns, db, s, opt.version).await?;
			let opt = opt.limited_by(&AuthLimit::try_from(&val.auth_limit)?);

			// Check permissions
			if ctx.check_perms(&opt, Action::View)? {
				check_perms(stk, ctx, &opt, doc, &name, &val.permissions).await?;
			}
			// Validate the arguments
			validate_args(
				&name,
				&args,
				&val.args.iter().map(|(_, k)| k.clone()).collect::<Vec<Kind>>(),
			)?;
			// Compute the function arguments
			// Duplicate context
			let mut ctx = Context::new_isolated(ctx);
			// Process the function arguments
			for (val, (param_name, kind)) in args.into_iter().zip(&val.args) {
				ctx.add_value(
					param_name.clone(),
					val.coerce_to_kind(kind)
						.map_err(|e| ExprError::InvalidFunctionArguments {
							name: name.clone(),
							message: format!("Failed to coerce argument `${param_name}`: {e}"),
						})
						.map_err(anyhow::Error::new)?
						.into(),
				);
			}
			let ctx = ctx.freeze();
			// Run the custom function
			let result = stk
				.run(|stk| crate::legacy::block_compute(&val.block, stk, &ctx, &opt, doc))
				.await
				.catch_return()?;
			// Validate the return value
			validate_return(name.as_str(), val.returns.as_ref(), result)
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
			let val = ctx.tx().get_db_module(ns, db, mod_name.as_str(), opt.version).await?;

			// Check permissions
			if ctx.check_perms(opt, Action::View)? {
				check_perms(stk, ctx, opt, doc, &mod_name, &val.permissions).await?;
			}

			// Get the executable & signature
			let executable: ModuleExecutable = val.executable.clone().into();
			let signature = crate::legacy::module_executable_signature(
				&executable,
				ctx,
				&ns,
				&db,
				sub.as_deref(),
			)
			.await?;

			// Validate the arguments
			validate_args(&fnc_name, &args, &signature.args)?;

			// Run the module
			let result = crate::legacy::module_executable_run(
				&executable,
				stk,
				ctx,
				opt,
				doc,
				args,
				sub.as_deref(),
			)
			.await?;

			// Validate the return value
			validate_return(fnc_name.as_str(), signature.returns.as_ref(), result)
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
			let val = ctx.tx().get_db_module(ns, db, mod_name.as_str(), opt.version).await?;

			// Check permissions
			if ctx.check_perms(opt, Action::View)? {
				check_perms(stk, ctx, opt, doc, &mod_name, &val.permissions).await?;
			}

			// Get the executable & signature
			let executable: ModuleExecutable = val.executable.clone().into();
			let signature = crate::legacy::module_executable_signature(
				&executable,
				ctx,
				&ns,
				&db,
				sub.as_deref(),
			)
			.await?;

			// Validate the arguments
			validate_args(&fnc_name, &args, &signature.args)?;

			// Run the module
			let result = crate::legacy::module_executable_run(
				&executable,
				stk,
				ctx,
				opt,
				doc,
				args,
				sub.as_deref(),
			)
			.await?;

			// Validate the return value
			validate_return(fnc_name.as_str(), signature.returns.as_ref(), result)
		}
	}
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "FunctionCall::compute", skip_all)]
pub(crate) async fn function_call_compute(
	this: &FunctionCall,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<Value> {
	// Compute the function arguments
	let args = stk
		.scope(|scope| {
			try_join_all(
				this.arguments
					.iter()
					.map(|v| scope.run(|stk| crate::legacy::expr_compute(v, stk, ctx, opt, doc))),
			)
		})
		.await?;
	// Process the function type
	crate::legacy::function_compute(&this.receiver, stk, ctx, opt, doc, args).await
}

pub(crate) async fn check_perms(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	name: &str,
	permissions: &Permission,
) -> FlowResult<()> {
	match permissions {
		Permission::Full => Ok(()),
		Permission::None => {
			Err(ControlFlow::from(anyhow::Error::new(ExecError::FunctionPermissions {
				name: name.to_string(),
			})))
		}
		Permission::Specific(e) => {
			// Disable permission recursion and block side effects
			let opt = &opt.new_for_permission_predicate();
			// Process the PERMISSION clause
			if !stk.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, doc)).await?.is_truthy()
			{
				Err(ControlFlow::from(anyhow::Error::new(ExecError::FunctionPermissions {
					name: name.to_string(),
				})))
			} else {
				Ok(())
			}
		}
	}
}

pub(crate) fn validate_args(name: &str, args: &[Value], sig: &[Kind]) -> FlowResult<()> {
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
		return Err(ControlFlow::from(anyhow::Error::new(ExprError::InvalidFunctionArguments {
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

pub(crate) fn validate_return(
	name: &str,
	return_kind: Option<&Kind>,
	result: Value,
) -> FlowResult<Value> {
	match return_kind {
		Some(kind) => result
			.coerce_to_kind(kind)
			.map_err(|e| ExecError::ReturnCoerce {
				name: name.to_string(),
				error: Box::new(e),
			})
			.map_err(anyhow::Error::new)
			.map_err(ControlFlow::from),
		None => Ok(result),
	}
}
