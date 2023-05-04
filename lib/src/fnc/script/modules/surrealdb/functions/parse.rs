use super::super::pkg;
use crate::fnc::script::modules::impl_module_def;
use js::{Created, Ctx, Func, Loaded, Module, ModuleDef, Native, Object, Rest, Result};

mod email;
mod url;

pub struct Package;

impl_module_def!(
	Package,
	"parse",
	"email" => (email::Package),
	"url" => (url::Package)
);
