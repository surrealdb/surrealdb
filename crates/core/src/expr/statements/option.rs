#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct OptionStatement {
	pub name: String,
	pub what: bool,
}
