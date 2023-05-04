use crate::fnc::script::modules::impl_module_def;

use super::super::pkg;
use js::Created;
use js::Ctx;
use js::Loaded;
use js::Module;
use js::ModuleDef;
use js::Native;
use js::Object;
use js::Result;

mod email;
mod url;

pub struct Package;

impl_module_def!(
	Package,
	"parse",
	"email" => (email::Package),
	"url" => (url::Package)
);
