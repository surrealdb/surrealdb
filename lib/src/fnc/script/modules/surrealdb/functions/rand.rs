use super::super::pkg;
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

mod uuid;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"rand",
	"bool" => run,
	"enum" => run,
	"float" => run,
	"guid" => run,
	"int" => run,
	"string" => run,
	"time" => run,
	"uuid" => (uuid::Package)
);
