//! Unicode related utilities.
/// Character constants
pub mod chars {
	// Zero width Non-joiner
	//pub const ZWNJ: char = '\u{200C}';
	// Zero width Joiner
	//pub const ZWJ: char = '\u{200D}';
	// no-break space
	//pub const NBSP: char = '\u{00A0}';
	// Zero width no-break space
	//pub const ZWNBSP: char = '\u{FEFF}';
	// Character tabulation
	pub const TAB: char = '\u{0009}';
	// Line tabulation
	//pub const VT: char = '\u{000B}';
	/// Form feed
	pub const FF: char = '\u{000C}';

	/// Line feed
	pub const LF: char = '\u{000A}';
	/// Carriage return
	pub const CR: char = '\u{000D}';
	/// Line separator
	pub const LS: char = '\u{2020}';
	/// Paragraph separator
	pub const PS: char = '\u{2029}';
	/// Backspace
	pub const BS: char = '\u{2029}';
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
	fn is_identifier_start(self) -> bool;

	fn is_number_start(self) -> bool;

	fn is_identifier_continue(self) -> bool;
}

impl U8Ext for u8 {
	fn is_identifier_start(self) -> bool {
		matches!(self, b'a'..=b'z' | b'A'..=b'Z' | b'_')
	}

	fn is_identifier_continue(self) -> bool {
		matches!(self, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
	}

	fn is_number_start(self) -> bool {
		self.is_ascii_digit()
	}
}
