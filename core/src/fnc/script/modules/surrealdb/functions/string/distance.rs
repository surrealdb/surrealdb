use super::run;
use crate::fnc::script::modules::impl_module_def;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"string::distance",
	"damerau_levenshtein" => run,
	"damerau_levenshtein_normalized" => run,
	"hamming" => run,
	"levenshtein" => run,
	"levenshtein_normalized" => run,
	"levenshtein_osa" => run
);
