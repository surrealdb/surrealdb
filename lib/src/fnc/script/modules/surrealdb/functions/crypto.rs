use super::super::pkg;
use super::run;
use crate::fnc::script::modules::impl_module_def;
use crate::sql::value::Value;
use js::Created;
use js::Ctx;
use js::Func;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Rest;
use js::Result;

mod argon2;
mod bcrypt;
mod pbkdf2;
mod scrypt;

pub struct Package;

type Any = Rest<Value>;

impl_module_def!(
	Package,
	"crypto",
	"md5" => run,
	"sha1" => run,
	"sha256" => run,
	"sha512" => run,
	"argon2" => (argon2::Package),
	"bcrypt" => (bcrypt::Package),
	"pbkdf2" => (pbkdf2::Package),
	"scrypt" => (scrypt::Package)
);
