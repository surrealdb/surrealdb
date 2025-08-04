use super::run;
use crate::fnc::script::modules::impl_module_def;

pub struct Package;

impl_module_def!(
	Package,
	"string::is",
	"alphanum" => run,
	"alpha" => run,
	"ascii" => run,
	"datetime" => run,
	"domain" => run,
	"email" => run,
	"email" => run,
	"hexadecimal" => run,
	"ip" => run,
	"ipv4" => run,
	"ipv6" => run,
	"latitude" => run,
	"longitude" => run,
	"numeric" => run,
	"semver" => run,
	"url" => run,
	"ulid" => run,
	"uuid" => run,
	"record" => run
);
