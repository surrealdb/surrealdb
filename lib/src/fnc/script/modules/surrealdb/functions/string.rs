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
	"string",
	"concat" => run,
	"contains" => run,
	"endsWith" => run,
	"join" => run,
	"len" => run,
	"lowercase" => run,
	"repeat" => run,
	"replace" => run,
	"reverse" => run,
	"slice" => run,
	"slug" => run,
	"split" => run,
	"startsWith" => run,
	"trim" => run,
	"uppercase" => run,
	"words" => run
);
