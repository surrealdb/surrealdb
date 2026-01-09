use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"session",
	"db" => run,
	"id" => run,
	"ip" => run,
	"ns" => run,
	"origin" => run,
	"ac" => run,
	"rd" => run,
	"token" => run
);
