use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
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
	"type",
	"bool" => run,
	"datetime" => run,
	"decimal" => run,
	"duration" => run,
	"float" => run,
	"int" => run,
	"number" => run,
	"point" => run,
	"regex" => run,
	"string" => run,
	"table" => run,
	"thing" => run
);
