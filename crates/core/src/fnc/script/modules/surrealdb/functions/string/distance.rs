use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"string::distance",
	"damerau_levenshtein" => run,
	"hamming" => run,
	"levenshtein" => run,
	"normalized_damerau_levenshtein" => run,
	"normalized_levenshtein" => run,
	"osa_distance" => run
);
