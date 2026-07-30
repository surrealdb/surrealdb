use crate::expr::Expr;
use crate::expr::fetch::Fetchs;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct OutputStatement {
	pub what: Expr,
	pub fetch: Option<Fetchs>,
}

impl OutputStatement {
	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		self.what.read_only()
	}
}
