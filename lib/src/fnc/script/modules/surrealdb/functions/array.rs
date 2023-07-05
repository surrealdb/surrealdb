use super::run;
use crate::fnc::script::modules::impl_module_def;

mod sort;
pub struct Package;

impl_module_def!(
	Package,
	"array",
	"add" => run,
	"all" => run,
	"any" => run,
	"append" => run,
	"combine" => run,
	"complement" => run,
	"concat" => run,
	"difference" => run,
	"distinct" => run,
	"flatten" => run,
	"group" => run,
	"insert" => run,
	"intersect" => run,
	"join" => run,
	"len" => run,
	"max" => run,
	"min" => run,
	"pop" => run,
	"push" => run,
	"prepend" => run,
	"remove" => run,
	"reverse" => run,
	"slice" => run,
	"sort" => (sort::Package),
	"union" => run
);
