use super::run;
use crate::fnc::script::modules::impl_module_def;

mod argon2;
mod bcrypt;
mod blake3;
mod pbkdf2;
mod scrypt;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"crypto",
	"md5" => run,
	"sha1" => run,
	"sha256" => run,
	"sha512" => run,
	"argon2" => (argon2::Package),
	"bcrypt" => (bcrypt::Package),
	"blake3" => (blake3::Package),
	"pbkdf2" => (pbkdf2::Package),
	"scrypt" => (scrypt::Package)
);
