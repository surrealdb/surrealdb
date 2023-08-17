/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type Key = Vec<u8>;

/// The value part of a key-value pair. An alias for [`Vec<u8>`].
pub type Val = Vec<u8>;

/// Used to determine the behaviour when a transaction is not handled correctly
#[derive(Default)]
pub(crate) enum Check {
	#[default]
	None,
	Warn,
	Panic,
}

/// This trait appends an element to a collection, and allows chaining
pub(super) trait Add<T> {
	fn add(self, v: T) -> Self;
}

impl Add<u8> for Vec<u8> {
	fn add(mut self, v: u8) -> Self {
		self.push(v);
		self
	}
}

/// This trait converts a collection of key-value pairs into the desired type
pub(super) trait Convert<T> {
	fn convert(self) -> T;
}

impl<T> Convert<Vec<T>> for Vec<(Key, Val)>
where
	T: From<Val>,
{
	fn convert(self) -> Vec<T> {
		self.into_iter().map(|(_, v)| v.into()).collect()
	}
}
