use super::run;
use crate::fnc::script::modules::impl_module_def;

mod distance;
mod similarity;

pub struct Package;

impl_module_def!(
	Package,
	"vector",
	"distance" => (distance::Package),
	"similarity" => (similarity::Package),
	"add" => run,
	"angle" => run,
	"cross" => run,
	"divide" => run,
	"dot" => run,
	"magnitude" => run,
	"multiply" => run,
	"normalize" => run,
	"project" => run,
	"scale" => run,
	"subtract" => run
);
