use js::Ctx;
use js::prelude::Rest;

use super::super::run;
use crate::val::Value;

pub struct Package;

impl js::module::ModuleDef for Package {
	fn declare(decls: &js::module::Declarations) -> js::Result<()> {
		decls.declare("default")?;
		decls.declare("v4")?;
		decls.declare("v7")?;
		Ok(())
	}
	fn evaluate<'js>(ctx: &js::Ctx<'js>, exports: &js::module::Exports<'js>) -> js::Result<()> {
		let default = js::Function::new(ctx.clone(), |ctx: Ctx<'js>, args: Rest<Value>| {
			run(ctx, "rand::uuid", args.0)
		})?
		.with_name("uuid")?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand::uuid", "v4", run,);
		exports.export("v4", value.clone())?;
		default.set("v4", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand::uuid", "v7", run,);
		exports.export("v7", value.clone())?;
		default.set("v7", value)?;
		exports.export("default", default)?;
		Ok(())
	}
}
