use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"string::distance",
	"damerau" => run,
	"damerau_nrm" => run,
	"hamming" => run,
	"levenshtein" => run,
	"levenshtein_nrm" => run,
	"levenshtein_osa" => run
);
