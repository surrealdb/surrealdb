use super::run;
use crate::fnc::script::modules::impl_module_def;

mod distance;
mod html;
mod semver;
mod similarity;

pub struct Package;

impl_module_def!(
	Package,
	"string",
	"capitalize" => run,
	"concat" => run,
	"contains" => run,
	"distance" => (distance::Package),
	"ends_with" => run,
	"html" => (html::Package),
	"join" => run,
	"len" => run,
	"lowercase" => run,
	"matches" => run,
	"repeat" => run,
	"replace" => run,
	"reverse" => run,
	"similarity" => (similarity::Package),
	"slice" => run,
	"slug" => run,
	"split" => run,
	"starts_with" => run,
	"trim" => run,
	"uppercase" => run,
	"words" => run,
	"semver" => (semver::Package),
	"is_alphanum" => run,
	"is_alpha" => run,
	"is_ascii" => run,
	"is_datetime" => run,
	"is_domain" => run,
	"is_email" => run,
	"is_email" => run,
	"is_hexadecimal" => run,
	"is_ip" => run,
	"is_ipv4" => run,
	"is_ipv6" => run,
	"is_latitude" => run,
	"is_longitude" => run,
	"is_numeric" => run,
	"is_semver" => run,
	"is_url" => run,
	"is_ulid" => run,
	"is_uuid" => run,
	"is_record" => run
);
