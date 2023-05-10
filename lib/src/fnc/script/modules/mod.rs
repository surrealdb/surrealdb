pub mod os;
pub mod surrealdb;

use js::BuiltinResolver;
use js::ModuleLoader;

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
			crate::fnc::script::modules::surrealdb::pkg::<$module::$pkg>($ctx, $name)
		}
	};
	// Call a (possibly-async) function.
	($ctx: expr, $path: literal, $name: literal, $call: ident, $($wrapper: ident)?) => {
		{
			js::Func::from($($wrapper)? (|v: js::Rest<crate::sql::value::Value>| $call(if $path == "" { $name } else { concat!($path, "::", $name) }, v.0)))
		}
	};
	// Return the value of an expression that can be converted to JS.
	($ctx: expr, $path: literal, $name: literal, ($e: expr), $($wrapper: ident)?) => {
		{
			$e
		}
	};
	($pkg: ident, $path: literal, $($name: literal => $action: tt $($wrapper: ident)?),*) => {
		impl js::ModuleDef for Package {
			fn load<'js>(_ctx: js::Ctx<'js>, module: &js::Module<'js, js::Created>) -> js::Result<()> {
				module.add("default")?;
				$(
					module.add($name)?;
				)*
				Ok(())
			}

			fn eval<'js>(ctx: js::Ctx<'js>, module: &js::Module<'js, js::Loaded<js::Native>>) -> js::Result<()> {
				let default = js::Object::new(ctx)?;
				$(
					module.set($name, crate::fnc::script::modules::impl_module_def!(ctx, $path, $name, $action, $($wrapper)?))?;
					default.set($name, crate::fnc::script::modules::impl_module_def!(ctx, $path, $name, $action, $($wrapper)?))?;
				)*
				module.set("default", default)?;
				Ok(())
			}
		}
	}
}
pub(crate) use impl_module_def;
