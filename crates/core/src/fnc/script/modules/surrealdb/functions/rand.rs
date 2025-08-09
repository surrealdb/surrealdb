use js::Ctx;
use js::prelude::Rest;

use super::run;
use crate::val::Value;

mod uuid;

pub struct Package;

impl js::module::ModuleDef for Package {
	fn declare(decls: &js::module::Declarations) -> js::Result<()> {
		decls.declare("default")?;
		decls.declare("bool")?;
		decls.declare("duration")?;
		decls.declare("enum")?;
		decls.declare("float")?;
		decls.declare("guid")?;
		decls.declare("int")?;
		decls.declare("string")?;
		decls.declare("time")?;
		decls.declare("ulid")?;
		decls.declare("uuid")?;
		Ok(())
	}
	fn evaluate<'js>(ctx: &js::Ctx<'js>, exports: &js::module::Exports<'js>) -> js::Result<()> {
		let default = js::Function::new(ctx.clone(), |ctx: Ctx<'js>, args: Rest<Value>| {
			run(ctx, "rand", args.0)
		})?
		.with_name("rand")?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "bool", run,);
		exports.export("bool", value.clone())?;
		default.set("bool", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "duration", run,);
		exports.export("duration", value.clone())?;
		default.set("duration", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "enum", run,);
		exports.export("enum", value.clone())?;
		default.set("enum", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "float", run,);
		exports.export("float", value.clone())?;
		default.set("float", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "guid", run,);
		exports.export("guid", value.clone())?;
		default.set("guid", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "int", run,);
		exports.export("int", value.clone())?;
		default.set("int", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "string", run,);
		exports.export("string", value.clone())?;
		default.set("string", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "time", run,);
		exports.export("time", value.clone())?;
		default.set("time", value)?;
		let value = crate::fnc::script::modules::impl_module_def!(ctx, "rand", "ulid", run,);
		exports.export("ulid", value.clone())?;
		default.set("ulid", value)?;
		let value =
			crate::fnc::script::modules::impl_module_def!(ctx, "rand", "uuid", (uuid::Package),);
		exports.export("uuid", value.clone())?;
		default.set("uuid", value)?;
		exports.export("default", default)?;
		Ok(())
	}
}
