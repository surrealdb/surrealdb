use crate::iam::ConfigKind;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RemoveConfigStatement {
	pub kind: ConfigKind,
	pub if_exists: bool,
}
