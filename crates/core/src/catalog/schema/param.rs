use crate::catalog::Permission;
use crate::kvs::impl_kv_value_revisioned;
use crate::val::Value;
use revision::revisioned;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ParamDefinition {
	pub name: String,
	pub value: Value,
	pub comment: Option<String>,
	pub permissions: Permission,
}
impl_kv_value_revisioned!(ParamDefinition);
