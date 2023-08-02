use std::fmt::{Debug, Display, Formatter};

#[derive(Debug)]
pub struct TestError {
	pub message: String,
}

impl Display for TestError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.message)
	}
}

impl std::error::Error for TestError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		None
	}
}
