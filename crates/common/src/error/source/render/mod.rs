use core::fmt;
use std::fmt::{Arguments, Display, Write};

use crate::{error::source::SourceDiagnostic, source_error::Group};

use super::Level;

pub enum Decor {
	Ascii,
	Unicode,
}

pub enum Styling {
	Plain,
	Colored,
}

pub struct Renderer {
	line_width: usize,
	decor: Decor,
	styling: Styling,
}

impl Renderer {
	pub const fn plain() -> Self {
		Renderer {
			line_width: 100,
			styling: Styling::Plain,
			decor: Decor::Unicode,
		}
	}

	pub const fn styled() -> Self {
		Renderer {
			line_width: 100,
			styling: Styling::Colored,
			decor: Decor::Unicode,
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Color {
	None,
	Blue,
	Green,
	Red,
	Yellow,
}

struct CharStyle {
	bold: bool,
	color: Color,
}

struct RenderChar {
	c: char,
	style: Color,
}

pub struct RenderBuffer {
	lines: Vec<Vec<RenderChar>>,
}

impl RenderBuffer {
	pub fn new() -> RenderBuffer {
		RenderBuffer {
			lines: vec![Vec::new()],
		}
	}

	fn push_char(&mut self, c: RenderChar) {
		self.lines.last_mut().unwrap().push(c);
	}

	pub fn display_ansi(&'_ self) -> AnsiBuffer<'_> {
		AnsiBuffer(self)
	}
}

impl Display for RenderBuffer {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		for l in &self.lines {
			for c in l {
				f.write_char(c.c)?;
			}
			f.write_char('\n')?;
		}
		Ok(())
	}
}

pub struct AnsiBuffer<'a>(&'a RenderBuffer);

impl Display for AnsiBuffer<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut last_color = Color::None;
		for l in &self.0.lines {
			for c in l {
				if c.style != last_color {
					let escape = match c.style {
						Color::None => "\x1b[0m",
						Color::Blue => "\x1b[34m",
						Color::Green => "\x1b[32m",
						Color::Red => "\x1b[31m",
						Color::Yellow => "\x1b[33m",
					};
					f.write_str(escape)?;
					last_color = c.style;
				}
				f.write_char(c.c)?;
			}
			f.write_char('\n')?;
		}
		f.write_str("\x1b[0m")?;

		Ok(())
	}
}

impl Renderer {
	pub fn render(&self, diagnostic: &SourceDiagnostic) -> RenderBuffer {
		let mut buffer = RenderBuffer::new();
		for g in &diagnostic.groups {
			render_group(&mut buffer, g);
		}
		buffer
	}
}

fn render_group<R: Eq>(buffer: &mut RenderBuffer, group: &Group<R>) {
	match group.level {
		Level::Error => fmt_color(buffer, Color::Red, "ERROR:"),
		Level::Warning => fmt_color(buffer, Color::Yellow, "WARNING:"),
	}

	fmt_color(buffer, Color::None, " ");

	fmt_color(buffer, Color::None, &group.title);
}

fn fmt_color<F: Display>(buffer: &mut RenderBuffer, color: Color, args: F) {
	struct Fmt<'a>(&'a mut RenderBuffer, Color);

	impl fmt::Write for Fmt<'_> {
		fn write_str(&mut self, s: &str) -> std::fmt::Result {
			for c in s.chars() {
				match c {
					'\n' => {
						self.0.lines.push(Vec::new());
					}
					c => self.0.push_char(RenderChar {
						c,
						style: self.1,
					}),
				}
			}
			Ok(())
		}
	}

	Fmt(buffer, color)
		.write_fmt(format_args!("{args}"))
		.expect("Writing into a render buffer cannot fail");
}
