use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"object",
	"entries" => run,
	"extend" => run,
	"from_entries" => run,
	"is_empty" => run,
	"keys" => run,
	"len" => run,
	"values" => run
);
