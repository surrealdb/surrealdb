use crate::fnc::script::modules::impl_module_def;

mod table;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"string",
	"table" => (table::Package)
);
