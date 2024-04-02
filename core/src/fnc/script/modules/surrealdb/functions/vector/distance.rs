use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"vector::distance",
	"chebyshev" => run,
	"euclidean" => run,
	"hamming" => run,
	"mahalanobis" => run,
	"manhattan" => run,
	"minkowski" => run
);
