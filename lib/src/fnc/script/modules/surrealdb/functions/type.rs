use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

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
