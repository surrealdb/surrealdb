use super::run;
use crate::fnc::script::modules::impl_module_def;

mod base64;

pub struct Package;

impl_module_def!(
	Package,
	"encoding::base64",
	"decode" => run,
	"encode" => run
);
