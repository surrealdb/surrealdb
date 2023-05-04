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
	"session",
	"db" => run,
	"id" => run,
	"ip" => run,
	"ns" => run,
	"origin" => run,
	"sc" => run,
	"sd" => run,
	"token" => run
);
