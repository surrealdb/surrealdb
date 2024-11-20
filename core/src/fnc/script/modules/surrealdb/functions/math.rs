use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"math",
	"abs" => run,
	"acos" => run,
	"acot" => run,
	"asin" => run,
	"atan" => run,
	"bottom" => run,
	"ceil" => run,
	"clamp" => run,
	"cos" => run,
	"cot" => run,
	"deg2rad" => run,
	"fixed" => run,
	"floor" => run,
	"interquartile" => run,
	"lerp" => run,
	"lerpangle" => run,
	"ln" => run,
	"log" => run,
	"log2" => run,
	"log10" => run,
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
	"rad2deg" => run,
	"round" => run,
	"sign" => run,
	"sin" => run,
	"spread" => run,
	"sqrt" => run,
	"stddev" => run,
	"sum" => run,
	"tan" => run,
	"top" => run,
	"trimean" => run,
	"variance" => run
);