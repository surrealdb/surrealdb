use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::Error as ExprError;
use crate::fnc::args::Any;
use crate::fnc::closure::NativeClosure;
use crate::val::Value;
use crate::val::closure::Closure;

pub(crate) async fn closure_invoke(
	this: &Closure,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
	args: Vec<Value>,
) -> Result<Value> {
	match this {
		Closure::Expr {
			args: arg_spec,
			returns,
			body,
			captures,
		} => {
			let mut ctx = Context::new_isolated(ctx);
			ctx.attach_variables(captures.clone())?;

			// check for missing arguments.
			if arg_spec.len() > args.len()
				&& let Some(x) = arg_spec[args.len()..].iter().find(|x| !x.1.can_be_none())
			{
				bail!(ExprError::InvalidFunctionArguments {
					name: "ANONYMOUS".to_string(),
					message: format!("Expected a value for {}", x.0.to_sql()),
				})
			}

			for ((name, kind), val) in arg_spec.iter().zip(args) {
				if let Ok(val) = val.coerce_to_kind(kind) {
					ctx.add_value(name.as_str(), val.into());
				} else {
					bail!(ExprError::InvalidFunctionArguments {
						name: "ANONYMOUS".to_string(),
						message: format!(
							"Expected a value of type '{}' for argument {}",
							kind.to_sql(),
							name.to_sql()
						),
					});
				}
			}

			let ctx = ctx.freeze();
			let result = stk
				.run(|stk| crate::legacy::expr_compute(body, stk, &ctx, opt, doc))
				.await
				.catch_return()?;
			if let Some(returns) = &returns {
				result
					.coerce_to_kind(returns)
					.map_err(|e| ExecError::ReturnCoerce {
						name: "ANONYMOUS".to_string(),
						error: Box::new(e),
					})
					.map_err(anyhow::Error::new)
			} else {
				Ok(result)
			}
		}
		Closure::Builtin(obj) => {
			let Some(NativeClosure(logic)) = obj.as_any().downcast_ref::<NativeClosure>() else {
				fail!("Closure::Builtin carried an unknown implementation type");
			};

			// Wrap args in Any - the builtin function will handle
			// argument validation and conversion using FromArgs::from_args
			let args = Any(args);

			// Call the builtin function
			logic(stk, ctx, opt, doc, args).await
		}
	}
}
