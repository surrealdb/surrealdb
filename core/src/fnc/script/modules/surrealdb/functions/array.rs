use super::run;
use crate::fnc::script::modules::impl_module_def;

mod sort;
#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"array",
	"add" => run,
	"all" => run,
	"any" => run,
	"at" => run,
	"append" => run,
	"boolean_and" => run,
	"boolean_not" => run,
	"boolean_or" => run,
	"boolean_xor" => run,
	"clump" => run,
	"combine" => run,
	"complement" => run,
	"concat" => run,
	"difference" => run,
	"distinct" => run,
	"filter_index" => run,
	"find_index" => run,
	"first" => run,
	"flatten" => run,
	"group" => run,
	"insert" => run,
	"intersect" => run,
	"join" => run,
	"knn" => run,
	"last" => run,
	"len" => run,
	"logical_and" => run,
	"logical_or" => run,
	"logical_xor" => run,
	"matches" => run,
	"max" => run,
	"min" => run,
	"pop" => run,
	"push" => run,
	"prepend" => run,
	"remove" => run,
	"reverse" => run,
	"slice" => run,
	"sort" => (sort::Package),
	"transpose" => run,
	"union" => run
);
