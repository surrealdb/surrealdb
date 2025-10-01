use crate::fnc::script::modules::impl_module_def;

mod base64;
mod cbor;

pub struct Package;

impl_module_def!(
	Package,
	"encoding",
	"base64" => (base64::Package),
	"cbor" => (cbor::Package)
);
