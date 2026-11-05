use super::run;
use crate::fnc::script::modules::impl_module_def;

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
	"f16" => run,
	"f32" => run,
	"float" => run,
	"geometry" => run,
	"i8" => run,
	"i16" => run,
	"i32" => run,
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
	"range" => run,
	"record" => run,
	"string" => run,
	"u8" => run,
	"u16" => run,
	"u32" => run,
	"u64" => run,
	"uuid" => run
);
