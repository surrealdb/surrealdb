use crate::fnc::script::modules::impl_module_def;

mod email;
mod url;

#[non_exhaustive]
pub struct Package;

impl_module_def!(
	Package,
	"parse",
	"email" => (email::Package),
	"url" => (url::Package)
);
