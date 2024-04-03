use super::super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"encoding::base64",
	"decode" => run,
	"encode" => run
);
