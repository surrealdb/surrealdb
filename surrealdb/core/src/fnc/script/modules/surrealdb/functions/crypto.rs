use super::run;
use crate::fnc::script::modules::impl_module_def;

mod argon2;
mod bcrypt;
mod pbkdf2;
mod scrypt;

pub struct Package;

impl_module_def!(
	Package,
	"crypto",
	"blake3" => run,
	"joaat" => run,
	"md5" => run,
	"sha1" => run,
	"sha256" => run,
	"sha512" => run,
	"argon2" => (argon2::Package),
	"bcrypt" => (bcrypt::Package),
	"pbkdf2" => (pbkdf2::Package),
	"scrypt" => (scrypt::Package)
);
