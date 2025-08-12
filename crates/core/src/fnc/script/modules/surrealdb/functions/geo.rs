use super::run;
use crate::fnc::script::modules::impl_module_def;

mod hash;
mod is;

pub struct Package;

impl_module_def!(
	Package,
	"geo",
	"area" => run,
	"bearing" => run,
	"centroid" => run,
	"distance" => run,
	"hash" => (hash::Package),
	"is" => (is::Package)
);
