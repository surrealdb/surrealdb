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

pub trait U8Ext {
	fn is_identifier_start(&self) -> bool;

	fn is_number_start(&self) -> bool;

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
