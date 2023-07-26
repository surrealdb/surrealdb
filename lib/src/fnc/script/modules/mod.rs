pub mod os;
pub mod surrealdb;

use js::loader::{BuiltinResolver, ModuleLoader};

pub fn resolver() -> BuiltinResolver {
	BuiltinResolver::default().with_module("os").with_module("surrealdb")
}

pub fn loader() -> ModuleLoader {
	ModuleLoader::default()
		.with_module("os", os::Package)
		.with_module("surrealdb", surrealdb::Package)
}

macro_rules! impl_module_def {
	// Delegate to a sub-module.
	($ctx: expr, $path: literal, $name: literal, ($module: ident::$pkg: ident), $($wrapper: ident)?) => {
		{
			crate::fnc::script::modules::surrealdb::pkg::<$module::$pkg>($ctx, $name)?
		}
	};
	($ctx: expr, $path: literal, $name: literal, $call: ident, Async) => {
		{
			// It is currently impossible to create closures which capture Ctx in a returned future.
			// So instead we define a normal function for async.
            async fn f<'js>(ctx: js::Ctx<'js>, v: js::function::Rest<crate::sql::value::Value>) -> js::Result<crate::sql::value::Value>{
                $call(ctx,if $path == "" { $name } else { concat!($path, "::", $name) }, v.0).await
            }
			let func = Async(f);
			js::Function::new($ctx.clone(),func)?.with_name(stringify!($name))?
		}
	};
	// Call a (possibly-async) function.
	($ctx: expr, $path: literal, $name: literal, $call: ident, ) => {
		{
			let func = |ctx: js::Ctx<'_>, v: js::function::Rest<crate::sql::value::Value>| $call(ctx,if $path == "" { $name } else { concat!($path, "::", $name) }, v.0);
			js::Function::new($ctx.clone(),func)?.with_name(stringify!($name))?
		}
	};
	// Return the value of an expression that can be converted to JS.
	($ctx: expr, $path: literal, $name: literal, ($e: expr), $($wrapper: ident)?) => {
		{
			$e
		}
	};
	($pkg: ident, $path: literal, $($name: literal => $action: tt $($wrapper: ident)?),*) => {
		impl js::module::ModuleDef for Package {
			fn declare(decls: &mut js::module::Declarations) -> js::Result<()> {
				decls.declare("default")?;
				$(
					decls.declare($name)?;
				)*
				Ok(())
			}

			fn evaluate<'js>(ctx: &js::Ctx<'js>, exports: &mut js::module::Exports<'js>) -> js::Result<()> {
				let default = js::Object::new(ctx.clone())?;
				$(
					let value = crate::fnc::script::modules::impl_module_def!(ctx, $path, $name, $action, $($wrapper)?);
					exports.export($name, value.clone())?;
					default.set($name, value)?;
				)*
				exports.export("default", default)?;
				Ok(())
			}
		}
	}
}
pub(crate) use impl_module_def;
