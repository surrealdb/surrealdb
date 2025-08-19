use std::{borrow::Borrow, ops::Deref};


#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
#[repr(transparent)]
pub struct StrandRef(str);

impl StrandRef {
	/// # Safety
	///
	/// string must not have a null byte in it
	pub const unsafe fn new_unchecked(s: &str) -> &StrandRef {
		unsafe {
			// This is safe as StrandRef has the same representation as str.
			std::mem::transmute(s)
		}
	}
}

impl ToOwned for StrandRef {
	type Owned = Strand;

	fn to_owned(&self) -> Self::Owned {
		Strand(self.0.to_owned())
	}
}

/// Fast way of removing null bytes in place without having to realloc the
/// string.
fn remove_null_bytes(s: String) -> String {
	let mut bytes = s.into_bytes();
	let mut write = 0;
	for i in 0..bytes.len() {
		let b = bytes[i];
		if b == 0 {
			continue;
		}
		bytes[write] = b;
		write += 1;
	}
	// remove duplicated bytes at the end.
	bytes.truncate(write);
	unsafe {
		// Safety: bytes were derived from a string,
		// we only removed all bytes which were 0 so we still have a valid utf8 string.
		String::from_utf8_unchecked(bytes)
	}
}

/// A string that doesn't contain NUL bytes.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Strand(String);

impl Strand {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> Option<Strand> {
		if s.contains('\0') {
			None
		} else {
			Some(Strand(s))
		}
	}

	/// Create a new strand from a string.
	/// Removes all null bytes if there are any
	pub fn new_lossy(s: String) -> Strand {
		Strand(remove_null_bytes(s))
	}

	/// Create a new strand, without checking the string.
	///
	/// # Safety
	/// Caller must ensure that string handed as an argument does not contain
	/// any null bytes.
	pub unsafe fn new_unchecked(s: String) -> Strand {
		// Check in debug mode if the variants
		debug_assert!(!s.contains('\0'));
		Strand(s)
	}

	pub fn into_string(self) -> String {
		self.0
	}

	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
}

impl Borrow<StrandRef> for Strand {
	fn borrow(&self) -> &StrandRef {
		// Safety:  both strand and strandref uphold no null bytes.
		unsafe { StrandRef::new_unchecked(self.as_str()) }
	}
}

impl From<String> for Strand {
	fn from(s: String) -> Self {
		// TODO: For now, fix this in the future.
		unsafe { Self::new_unchecked(s) }
	}
}

impl From<&str> for Strand {
	fn from(s: &str) -> Self {
		// TODO: For now, fix this in the future.
		unsafe { Self::new_unchecked(s.to_string()) }
	}
}

// TODO: Change this to str, possibly.
impl Deref for Strand {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Strand> for String {
	fn from(s: Strand) -> Self {
		s.0
	}
}