use revision::revisioned;

use crate::catalog::Permission;
use crate::expr::Expr;
use crate::val::Value;

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Method {
	Delete,
	Get,
	Patch,
	Post,
	Put,
	Trace,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ApiDefinition {
	pub methods: Vec<Method>,
	pub action: Expr,
	pub config: ApiConfig,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiConfig {
	pub middleware: Vec<MiddlewareStore>,
	pub permissions: Permission,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct MiddlewareStore {
	pub name: String,
	pub args: Vec<Value>,
}
