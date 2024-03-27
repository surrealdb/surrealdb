use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"type::is",
	"array" => run,
	"bool" => run,
	"bytes" => run,
	"collection" => run,
	"datetime" => run,
	"decimal" => run,
	"duration" => run,
	"float" => run,
	"geometry" => run,
	"int" => run,
	"line" => run,
	"none" => run,
	"null" => run,
	"multiline" => run,
	"multipoint" => run,
	"multipolygon" => run,
	"number" => run,
	"object" => run,
	"point" => run,
	"polygon" => run,
	"record" => run,
	"string" => run,
	"uuid" => run
);
