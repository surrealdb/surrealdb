use std::fmt::Display;
use std::rc::Rc;

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type KeyHeap = Vec<u8>;

/// The key part of a key-value pair. A mutable reference to heap allocated data.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct KeyStack<'data> {
	/// The key
	pub key: &'data [u8],
}

impl From<&[u8]> for KeyStack {
	fn from(value: &[u8]) -> Self {
		let backed = Rc::new(vec![0u8; value.len()]);
		backed[..value.len()].copy_from_slice(value);
		Self {
			key: &mut backed[..],
		}
	}
}

impl Into<KeyHeap> for KeyStack {
	fn into(self) -> KeyHeap {
		self.key.to_vec()
	}
}

impl Display for KeyStack {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for &byte in &self.key {
			write!(f, "{}", byte as char)?;
		}
		Ok(())
	}
}

impl Add<&KeyStack> for &KeyStack {
	fn add(self, v: &KeyStack) -> Self {
		let key = Rc::new(vec![0u8; self.key.len() + v.key.len()]);
		key[..self.key.len()].copy_from_slice(&self.key[..self.key.len()]);
		key[self.key.len()..].copy_from_slice(&v.key[..v.key.len()]);
		Self {
			key,
		}
	}
}

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

impl<T> Convert<Vec<T>> for Vec<(KeyStack, Val)>
where
	T: From<Val>,
{
	fn convert(self) -> Vec<T> {
		self.into_iter().map(|(_, v)| v.into()).collect()
	}
}
