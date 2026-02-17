use std::{
	fmt::{Arguments, Write},
	io,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Default, Debug)]
pub enum Color {
	#[default]
	Default,
	Red,
	Green,
	Blue,
	Yellow,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Default, Debug)]
pub enum Styling {
	#[default]
	Plain,
	Italic,
	Bold,
	BoldItalic,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Default, Debug)]
pub struct DisplayChar {
	pub color: Color,
	pub style: Styling,
	pub char: char,
}

/// A buffer of styled characters.
///
/// Can be written into and displayed, both styled and unstyled as required.
pub struct CharBuffer {
	lines: Vec<Vec<DisplayChar>>,
}

impl CharBuffer {
	/// Create a new char buffer.
	pub fn new() -> Self {
		CharBuffer {
			lines: vec![Vec::new()],
		}
	}

	/// Create a char buffer which just contains the contents of the given string, without styling.
	pub fn from_plain_string(str: &str) -> Self {
		let mut res = Self::new();
		res.push_str(str, Default::default(), Default::default());
		res
	}

	/// Add a string to the end of the buffer with the given styling.
	pub fn push_str(&mut self, s: &str, color: Color, style: Styling) {
		for c in s.chars() {
			self.push_char(c, color, style);
		}
	}

	/// Add a character to the end of the buffer with the given styling.
	pub fn push_char(&mut self, c: char, color: Color, style: Styling) {
		if c == '\n' {
			self.lines.push(Vec::new());
			return;
		}

		let last = self.lines.last_mut().unwrap();
		last.push(DisplayChar {
			color,
			style,
			char: c,
		});
	}

	/// Create a writer for pushing strings into the char buffer.
	pub fn writer<'a>(&'a mut self) -> CharBufferWriter<'a> {
		CharBufferWriter {
			indent: 0,
			color: Default::default(),
			style: Default::default(),
			buffer: self,
		}
	}

	/// Convert the char buffer to a plain string.
	pub fn to_string(&self) -> String {
		let mut res = String::new();
		for l in self.lines.iter() {
			for c in l.iter() {
				res.push(c.char);
			}
			res.push('\n');
		}
		res
	}

	/// Write the string to a writer, with all ansii styling.
	pub fn write_styled<W>(&self, out: &mut W) -> io::Result<()>
	where
		W: io::Write,
	{
		let mut color = Color::default();
		let mut style = Styling::default();
		let mut encode_buffer = [0u8; 4];
		for l in self.lines.iter() {
			for c in l.iter() {
				if c.char.is_whitespace() {
					out.write_all(c.char.encode_utf8(&mut encode_buffer).as_bytes())?;
					continue;
				}

				if c.color != color {
					match c.color {
						Color::Default => {
							out.write_all(&[b'\x1b', b'[', b'm'])?;
							style = Styling::Plain;
						}
						Color::Red => {
							out.write_all(&[b'\x1b', b'[', b'3', b'1', b'm'])?;
						}
						Color::Green => {
							out.write_all(&[b'\x1b', b'[', b'3', b'2', b'm'])?;
						}
						Color::Yellow => {
							out.write_all(&[b'\x1b', b'[', b'3', b'3', b'm'])?;
						}
						Color::Blue => {
							out.write_all(&[b'\x1b', b'[', b'3', b'4', b'm'])?;
						}
					}
					color = c.color;
				}

				match (style, c.style) {
					(Styling::Plain, Styling::Bold) => {
						out.write_all(&[b'\x1b', b'[', b'1', b'm'])?;
					}
					(Styling::Plain, Styling::BoldItalic) => {
						out.write_all(&[b'\x1b', b'[', b'1', b'm'])?;
						out.write_all(&[b'\x1b', b'[', b'3', b'm'])?;
					}
					(Styling::Plain, Styling::Italic) => {
						out.write_all(&[b'\x1b', b'[', b'3', b'm'])?;
					}
					(Styling::Italic, Styling::Plain) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'3', b'm'])?;
					}
					(Styling::Italic, Styling::Bold) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'3', b'm'])?;
						out.write_all(&[b'\x1b', b'[', b'1', b'm'])?;
					}
					(Styling::Italic, Styling::BoldItalic) => {
						out.write_all(&[b'\x1b', b'[', b'1', b'm'])?;
					}
					(Styling::Bold, Styling::Plain) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'2', b'm'])?;
					}
					(Styling::Bold, Styling::Italic) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'2', b'm'])?;
						out.write_all(&[b'\x1b', b'[', b'3', b'm'])?;
					}
					(Styling::Bold, Styling::BoldItalic) => {
						out.write_all(&[b'\x1b', b'[', b'3', b'm'])?;
					}
					(Styling::BoldItalic, Styling::Plain) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'2', b'm'])?;
						out.write_all(&[b'\x1b', b'[', b'2', b'3', b'm'])?;
					}
					(Styling::BoldItalic, Styling::Italic) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'2', b'm'])?;
					}
					(Styling::BoldItalic, Styling::Bold) => {
						out.write_all(&[b'\x1b', b'[', b'2', b'3', b'm'])?;
					}
					_ => {}
				}
				style = c.style;

				out.write_all(c.char.encode_utf8(&mut encode_buffer).as_bytes())?;
			}
			out.write_all(&[b'\n'])?;
		}
		out.write_all(&[
			b'\x1b', b'[', b'm', b'\x1b', b'[', b'2', b'2', b'm', b'\x1b', b'[', b'2', b'3', b'm',
		])?;
		Ok(())
	}
}

pub struct CharBufferWriter<'a> {
	color: Color,
	style: Styling,
	indent: usize,
	buffer: &'a mut CharBuffer,
}

impl<'a> CharBufferWriter<'a> {
	/// Set the color of text tfor the writer.
	pub fn color(&mut self, c: Color) -> &mut Self {
		self.color = c;
		self
	}

	/// Set the style of text tfor the writer.
	pub fn style(&mut self, style: Styling) -> &mut Self {
		self.style = style;
		self
	}

	/// Set the indentation for the text for the writer.
	pub fn indent(&mut self, indent: usize) -> &mut Self {
		self.indent = indent;
		self
	}

	/// Push a formatting args into the writer, unlike `write_fmt` this doesn't return an `Result`.
	pub fn push_fmt(&mut self, args: Arguments) -> &mut Self {
		self.write_fmt(args).unwrap();
		self
	}

	/// Push a string into the writer, unlike `write_str` this doesn't return an `Result`.
	pub fn push_str(&mut self, s: &str) -> &mut Self {
		for (idx, s) in s.split("\n").enumerate() {
			if idx != 0 {
				self.buffer.push_char('\n', self.color, self.style);
			}
			if s.is_empty() {
				continue;
			}
			if self.buffer.lines.last().unwrap().is_empty() {
				for _ in 0..self.indent {
					self.buffer.push_char(' ', self.color, self.style);
				}
			}
			self.buffer.push_str(s, self.color, self.style);
		}
		self
	}
}

impl Write for CharBufferWriter<'_> {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		self.push_str(s);
		Ok(())
	}
}
