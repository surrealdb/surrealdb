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
			pkg::<$module::$pkg>($ctx, $name)
		}
	};
	// Call a (possibly-async) function.
	($ctx: expr, $path: literal, $name: literal, $call: ident, $($wrapper: ident)?) => {
		{
			Func::from($($wrapper)? (|v: Any| $call(if $path == "" { $name } else { concat!($path, "::", $name) }, v.0)))
		}
	};
	// Return the value of an expression that can be converted to JS.
	($ctx: expr, $path: literal, $name: literal, ($e: expr), $($wrapper: ident)?) => {
		{
			$e
		}
	};
	($pkg: ident, $path: literal, $($name: literal => $action: tt $($wrapper: ident)?),*) => {
		impl ModuleDef for Package {
			fn load<'js>(_ctx: Ctx<'js>, module: &Module<'js, Created>) -> Result<()> {
				module.add("default")?;
				$(
					module.add($name)?;
				)*
				Ok(())
			}

			fn eval<'js>(ctx: Ctx<'js>, module: &Module<'js, Loaded<Native>>) -> Result<()> {
				let default = Object::new(ctx)?;
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
