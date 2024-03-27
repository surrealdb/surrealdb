use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"math",
	"abs" => run,
	"bottom" => run,
	"ceil" => run,
	"fixed" => run,
	"floor" => run,
	"interquartile" => run,
	"max" => run,
	"mean" => run,
	"median" => run,
	"midhinge" => run,
	"min" => run,
	"mode" => run,
	"nearestrank" => run,
	"percentile" => run,
	"pow" => run,
	"product" => run,
	"round" => run,
	"spread" => run,
	"sqrt" => run,
	"stddev" => run,
	"sum" => run,
	"top" => run,
	"trimean" => run,
	"variance" => run
);
