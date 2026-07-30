use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{Expr, ModuleExecutable};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub comment: Expr,
	pub permissions: Permission,
}
