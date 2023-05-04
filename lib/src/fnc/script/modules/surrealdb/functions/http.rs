use super::fut;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::Async;
use js::Created;
use js::Ctx;
use js::Func;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Rest;
use js::Result;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"http",
	"head" => fut Async,
	"get" => fut Async,
	"put" => fut Async,
	"post" => fut Async,
	"patch" => fut Async,
	"delete" => fut Async
);
