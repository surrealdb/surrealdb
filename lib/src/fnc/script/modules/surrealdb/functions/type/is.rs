use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"type::is",
	"array" => run,
	"bool" => run,
	"datetime" => run,
	"decimal" => run,
	"duration" => run,
	"float" => run,
	"geometry" => run,
	"int" => run,
	"number" => run,
	"object" => run,
	"record" => run,
	"string" => run
);
