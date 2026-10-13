use crate::expr::module::ModuleName;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
}
