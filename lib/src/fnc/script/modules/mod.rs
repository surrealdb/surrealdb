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
