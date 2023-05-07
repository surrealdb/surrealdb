use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"time::from",
	"micros" => run,
	"millis" => run,
	"secs" => run,
	"unix" => run
);
