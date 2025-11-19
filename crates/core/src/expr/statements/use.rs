#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct UseStatement {
	pub ns: Option<String>,
	pub db: Option<String>,
}
