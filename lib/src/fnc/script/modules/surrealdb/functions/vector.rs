use super::run;
use crate::fnc::script::modules::impl_module_def;

mod distance;
mod similarity;
pub struct Package;

impl_module_def!(
	Package,
	"vector",
	"distance" => (distance::Package),
	"dotproduct" => run,
	"magnitude" => run,
	"similarity" => (similarity::Package)
);
