#[derive(Debug, Clone)]
pub struct DialectCapabilities {
	pub supports_transactions: bool,
	pub supports_prepared_statements: bool,
	pub supports_cursors: bool,
}
