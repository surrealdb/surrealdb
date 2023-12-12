use std::fmt::Display;

/// The key part of a key-value pair. An alias for [`Vec<u8>`].
pub type KeyHeap = Vec<u8>;

/// The key part of a key-value pair. Stack allocated.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct KeyStack<const S: usize> {
	/// The key
	pub key: [u8; S],
	/// Since the key size must be known at compile time, we need to track the size in case it is
	/// smaller
	pub size: usize,
}

impl<const S: usize> From<&[u8]> for KeyStack<S> {
	fn from(value: &[u8]) -> Self {
		if value.len() > S {
			panic!("Key too long");
		}
		let mut key = [0u8; S];
		key[..value.len()].copy_from_slice(value);
		Self {
			key,
			size: value.len(),
		}
	}
}

impl<const F: usize, const T: usize> From<KeyStack<F>> for KeyStack<T> {
	fn from(value: KeyStack<F>) -> Self {
		if value.size > T {
			panic!("Key too long");
		}
		let mut key = [0u8; T];
		key[..value.size].copy_from_slice(&value.key[..value.size]);
		Self {
			key,
			size: value.size,
		}
	}
}

impl<const S: usize> Into<KeyHeap> for KeyStack<S> {
	fn into(self) -> KeyHeap {
		// Fixed size vec
		let mut result = vec![0; S];
		result[..self.size].copy_from_slice(&self.key[..self.size]);
		result
	}
}

impl<const S: usize> Display for KeyStack<S> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for &byte in &self.key[..self.size] {
			write!(f, "{}", byte as char)?;
		}
		Ok(())
	}
}

impl<const S: usize> Add<&KeyStack<S>> for &KeyStack<S> {
	fn add(self, v: &KeyStack<S>) -> Self {
		if self.size + v.size > S {
			panic!("Key too long");
		}
		let mut key = [0u8; S];
		key[..self.size].copy_from_slice(&self.key[..self.size]);
		key[self.size..self.size + v.size].copy_from_slice(&v.key[..v.size]);
		Self {
			key,
			size: self.size + v.size,
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

impl<T> Convert<Vec<T>> for Vec<(KeyStack<128>, Val)>
where
	T: From<Val>,
{
	fn convert(self) -> Vec<T> {
		self.into_iter().map(|(_, v)| v.into()).collect()
	}
}
