//! Unicode related utilities.

/// Character constants
pub mod chars {
	/// Zero width Non-joiner
	pub const ZWNJ: char = '\u{200C}';
	/// Zero width Joiner
	pub const ZWJ: char = '\u{200D}';
	/// no-break space
	pub const NBSP: char = '\u{00A0}';
	/// Zero width no-break space
	pub const ZWNBSP: char = '\u{FEFF}';
	/// Character tabulation
	pub const TAB: char = '\u{0009}';
	/// Line tabulation
	pub const VT: char = '\u{000B}';
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

	/// A const array of all characters which ecma considers whitespace.
	pub const WHITE_SPACE_CONST: [char; 20] = [
		'\u{0020}', TAB, FF, ZWNBSP, '\u{00A0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}',
		'\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}',
		'\u{200A}', '\u{202F}', '\u{205F}', '\u{3000}',
	];
	/// A static array of all characters which ecma considers whitespace.
	pub static WHITE_SPACE: [char; 20] = WHITE_SPACE_CONST;

	/// A const array of all characters which ecma considers line terminators.
	pub const LINE_TERMINATOR_CONST: [char; 4] = [LF, CR, LS, PS];
	/// A static array of all characters which ecma considers line terminators.
	pub static LINE_TERMINATOR: [char; 4] = LINE_TERMINATOR_CONST;
}

pub mod byte {
	/// Zero width Non-joiner
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
	/// No Break Space
	pub const NBSP: u8 = 0xA0;
}

pub fn byte_is_continue(v: u8) -> bool {
	// Bitmap containing with 1's for the bit representing a character which javascript
	// considers to be an ident continuing character.
	v < 128 && (1 << v) & 0x7fffffe87fffffe03ff001000000000u128 != 0
}

pub fn byte_is_start(v: u8) -> bool {
	(1 << v) & 0x7fffffe87fffffe0000001000000000u128 != 0
}
