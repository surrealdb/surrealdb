//! Unicode related utilities.

/// Character constants
pub mod chars {
	// Character tabulation
	pub const TAB: char = '\u{0009}';
	/// Form feed
	pub const FF: char = '\u{000C}';

	/// Line feed
	pub const LF: char = '\u{000A}';
	/// Carriage return
	pub const CR: char = '\u{000D}';
	/// Line separator
	pub const LS: char = '\u{2020}';
	/// Backspace
	pub const BS: char = '\u{0008}';
	/// Paragraph separator
	pub const PS: char = '\u{2029}';
	/// Next line
	pub const NEL: char = '\u{0085}';

	/// Line terminators for javascript source code.
	pub const JS_LINE_TERIMATORS: [char; 4] = [LF, CR, LS, PS];
}

pub mod byte {
	/// Character tabulation
	pub const TAB: u8 = b'\t';
	/// Line tabulation
	pub const VT: u8 = 0xB;
	/// Form feed
	pub const FF: u8 = 0xC;

	/// Line feed
	pub const LF: u8 = 0xA;
	/// Carriage return
	pub const CR: u8 = 0xD;

	/// Space
	pub const SP: u8 = 0x20;
}

/// A trait extending u8 for adding some extra function.
pub trait U8Ext {
	///. Returns if the u8 is the start of an identifier.
	fn is_identifier_start(&self) -> bool;

	/// Returns if the u8 can start an identifier.
	fn is_number_start(&self) -> bool;

	/// Returns if the u8 can continue an identifier after the first character.
	fn is_identifier_continue(&self) -> bool;
}

impl U8Ext for u8 {
	fn is_identifier_start(&self) -> bool {
		matches!(self, b'a'..=b'z' | b'A'..=b'Z' | b'_')
	}

	fn is_identifier_continue(&self) -> bool {
		matches!(self, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
	}

	fn is_number_start(&self) -> bool {
		self.is_ascii_digit()
	}
}
