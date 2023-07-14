use super::run;
use crate::fnc::script::modules::impl_module_def;

mod uuid;

pub struct Package;

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
	"ulid" => run,
	"uuid" => (uuid::Package)
);
