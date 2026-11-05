//! Well-formedness of XML content, the check PostgreSQL makes of an `xml` value under its default
//! `xmloption` of `content`.
//!
//! Content is zero or more elements, text, comments, CDATA sections, and processing
//! instructions, with an optional XML declaration at the start. Entity references are the five
//! predefined ones and character references; a DOCTYPE, which would declare entities, is refused.
//! Names are checked for their first and later characters but not against every Unicode range,
//! so a name with an exotic letter may be accepted that XML would refuse.

/// Why a text is not well-formed content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct XmlError {
	pub reason: String,
}

/// Check that `text` is well-formed XML content. The text is not changed.
pub fn check_content(text: &str) -> Result<(), XmlError> {
	// A character XML does not allow is refused wherever it is: in text, a tag, an attribute
	// value, a comment, a CDATA section, a processing instruction or the declaration.
	if let Some((at, _)) = text.char_indices().find(|(_, c)| !is_xml_char(*c)) {
		return Err(XmlError {
			reason: format!("a character XML does not allow is at byte {at}"),
		});
	}
	Checker {
		bytes: text.as_bytes(),
		at: 0,
		open: Vec::new(),
	}
	.run()
}

/// Whether XML allows `c` in a document: the `Char` production, less the surrogates a `char`
/// cannot hold.
fn is_xml_char(c: char) -> bool {
	matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..)
}

struct Checker<'a> {
	bytes: &'a [u8],
	at: usize,
	open: Vec<&'a [u8]>,
}

impl<'a> Checker<'a> {
	fn run(mut self) -> Result<(), XmlError> {
		if self.starts_with(b"<?xml")
			&& self.bytes.get(5).is_some_and(|b| b.is_ascii_whitespace() || *b == b'?')
		{
			self.declaration()?;
		}
		while let Some(byte) = self.peek() {
			match byte {
				b'<' => self.markup()?,
				b'&' => self.reference()?,
				b']' if self.starts_with(b"]]>") => {
					return Err(self.error("\"]]>\" is not allowed in content"));
				}
				_ => self.at += 1,
			}
		}
		if let Some(name) = self.open.last() {
			return Err(XmlError {
				reason: format!("element \"{}\" is not closed", String::from_utf8_lossy(name)),
			});
		}
		Ok(())
	}

	fn error(&self, what: &str) -> XmlError {
		XmlError {
			reason: format!("{what} at byte {}", self.at),
		}
	}

	fn peek(&self) -> Option<u8> {
		self.bytes.get(self.at).copied()
	}

	fn starts_with(&self, prefix: &[u8]) -> bool {
		self.bytes[self.at..].starts_with(prefix)
	}

	fn skip_whitespace(&mut self) {
		while self.peek().is_some_and(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n')) {
			self.at += 1;
		}
	}

	/// Consume up to and including `end`, returning the index where `end` starts.
	fn until(&mut self, end: &[u8], what: &str) -> Result<usize, XmlError> {
		let start = self.at;
		match self.bytes[start..].windows(end.len()).position(|window| window == end) {
			Some(offset) => {
				self.at = start + offset + end.len();
				Ok(start + offset)
			}
			None => Err(self.error(&format!("{what} is not closed"))),
		}
	}

	/// The XML declaration at the start, as XML 1.0 spells it: `<?xml`, a `version` of
	/// `1.` and digits, then optionally an `encoding` name and a `standalone` of `yes` or
	/// `no`, in that order, each preceded by whitespace, then `?>`.
	fn declaration(&mut self) -> Result<(), XmlError> {
		self.at += 5;
		let mut seen = 0;
		loop {
			let spaced = self.peek().is_some_and(|b| b.is_ascii_whitespace());
			self.skip_whitespace();
			if self.starts_with(b"?>") {
				if seen == 0 {
					return Err(self.error("the XML declaration has no version"));
				}
				self.at += 2;
				return Ok(());
			}
			if !spaced {
				return Err(self.error("the XML declaration is malformed"));
			}
			let (rank, name): (usize, &[u8]) = if self.starts_with(b"version") {
				(1, b"version")
			} else if self.starts_with(b"encoding") {
				(2, b"encoding")
			} else if self.starts_with(b"standalone") {
				(3, b"standalone")
			} else {
				return Err(self.error("the XML declaration has an unknown pseudo-attribute"));
			};
			if rank <= seen || (seen == 0 && rank != 1) {
				return Err(self.error("the XML declaration's pseudo-attributes are out of order"));
			}
			seen = rank;
			self.at += name.len();
			self.skip_whitespace();
			if self.peek() != Some(b'=') {
				return Err(self.error("the XML declaration is malformed"));
			}
			self.at += 1;
			self.skip_whitespace();
			let quote = match self.peek() {
				Some(q @ (b'"' | b'\'')) => q,
				_ => return Err(self.error("the XML declaration is malformed")),
			};
			self.at += 1;
			let start = self.at;
			while self.peek().is_some_and(|b| b != quote) {
				self.at += 1;
			}
			if self.peek().is_none() {
				return Err(self.error("the XML declaration is not closed"));
			}
			let value = &self.bytes[start..self.at];
			self.at += 1;
			let valid = match rank {
				1 => value
					.strip_prefix(b"1.")
					.is_some_and(|rest| !rest.is_empty() && rest.iter().all(u8::is_ascii_digit)),
				2 => {
					value.first().is_some_and(u8::is_ascii_alphabetic)
						&& value
							.iter()
							.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
				}
				_ => value == b"yes" || value == b"no",
			};
			if !valid {
				return Err(self.error("the XML declaration has an invalid pseudo-attribute value"));
			}
		}
	}

	fn markup(&mut self) -> Result<(), XmlError> {
		if self.starts_with(b"<!--") {
			self.at += 4;
			let body_start = self.at;
			let end = self.until(b"-->", "a comment")?;
			if self.bytes[body_start..end].windows(2).any(|pair| pair == b"--") {
				return Err(self.error("\"--\" is not allowed in a comment"));
			}
			Ok(())
		} else if self.starts_with(b"<![CDATA[") {
			self.at += 9;
			self.until(b"]]>", "a CDATA section")?;
			Ok(())
		} else if self.starts_with(b"<?") {
			self.at += 2;
			let target = self.name()?;
			if target.eq_ignore_ascii_case(b"xml") {
				return Err(self.error("the processing instruction target \"xml\" is reserved"));
			}
			self.until(b"?>", "a processing instruction")?;
			Ok(())
		} else if self.starts_with(b"<!") {
			Err(self.error("a DOCTYPE or a declaration is not supported"))
		} else if self.starts_with(b"</") {
			self.at += 2;
			let name = self.name()?;
			self.skip_whitespace();
			if self.peek() != Some(b'>') {
				return Err(self.error("an end tag is not closed"));
			}
			self.at += 1;
			match self.open.pop() {
				Some(open) if open == name => Ok(()),
				Some(open) => Err(XmlError {
					reason: format!(
						"element \"{}\" is closed by \"{}\"",
						String::from_utf8_lossy(open),
						String::from_utf8_lossy(name)
					),
				}),
				None => Err(XmlError {
					reason: format!(
						"end tag \"{}\" has no start tag",
						String::from_utf8_lossy(name)
					),
				}),
			}
		} else {
			self.at += 1;
			self.start_tag()
		}
	}

	/// An element start tag after its `<`, through `>` or `/>`.
	fn start_tag(&mut self) -> Result<(), XmlError> {
		let name = self.name()?;
		let mut attributes: Vec<&[u8]> = Vec::new();
		loop {
			let before = self.at;
			self.skip_whitespace();
			match self.peek() {
				Some(b'>') => {
					self.at += 1;
					self.open.push(name);
					return Ok(());
				}
				Some(b'/') => {
					self.at += 1;
					if self.peek() != Some(b'>') {
						return Err(self.error("\"/\" must be followed by \">\""));
					}
					self.at += 1;
					return Ok(());
				}
				Some(_) if self.at > before => {
					let attribute = self.name()?;
					if attributes.contains(&attribute) {
						return Err(XmlError {
							reason: format!(
								"attribute \"{}\" is repeated",
								String::from_utf8_lossy(attribute)
							),
						});
					}
					attributes.push(attribute);
					self.skip_whitespace();
					if self.peek() != Some(b'=') {
						return Err(self.error("an attribute has no value"));
					}
					self.at += 1;
					self.skip_whitespace();
					self.attribute_value()?;
				}
				Some(_) => {
					return Err(
						self.error("a tag is not closed or has no space before an attribute")
					);
				}
				None => return Err(self.error("a tag is not closed")),
			}
		}
	}

	fn attribute_value(&mut self) -> Result<(), XmlError> {
		let quote = match self.peek() {
			Some(q @ (b'"' | b'\'')) => q,
			_ => return Err(self.error("an attribute value is not quoted")),
		};
		self.at += 1;
		loop {
			match self.peek() {
				None => return Err(self.error("an attribute value is not closed")),
				Some(b) if b == quote => {
					self.at += 1;
					return Ok(());
				}
				Some(b'<') => return Err(self.error("\"<\" is not allowed in an attribute value")),
				Some(b'&') => self.reference()?,
				Some(_) => self.at += 1,
			}
		}
	}

	/// An entity or character reference at `&`.
	fn reference(&mut self) -> Result<(), XmlError> {
		let start = self.at;
		self.at += 1;
		let end = self.bytes[self.at..]
			.iter()
			.position(|b| *b == b';')
			.map(|offset| self.at + offset)
			.ok_or_else(|| self.error("an \"&\" is not a reference"))?;
		let body = &self.bytes[self.at..end];
		let valid = match body {
			b"amp" | b"lt" | b"gt" | b"quot" | b"apos" => true,
			[b'#', b'x', hex @ ..] => {
				!hex.is_empty()
					&& hex.iter().all(u8::is_ascii_hexdigit)
					&& code_point_is_xml_char(hex, 16)
			}
			[b'#', decimal @ ..] => {
				!decimal.is_empty()
					&& decimal.iter().all(u8::is_ascii_digit)
					&& code_point_is_xml_char(decimal, 10)
			}
			_ => false,
		};
		if !valid {
			return Err(XmlError {
				reason: format!(
					"the reference \"{}\" is not a predefined entity or a character reference",
					String::from_utf8_lossy(&self.bytes[start..=end.min(self.bytes.len() - 1)])
				),
			});
		}
		self.at = end + 1;
		Ok(())
	}

	/// A name: a letter, `_` or `:`, then letters, digits, `.`, `-`, `_`, `:`, or non-ASCII.
	fn name(&mut self) -> Result<&'a [u8], XmlError> {
		let start = self.at;
		let first_ok = self
			.peek()
			.is_some_and(|b| b.is_ascii_alphabetic() || matches!(b, b'_' | b':') || b >= 0x80);
		if !first_ok {
			return Err(self.error("a name is expected"));
		}
		self.at += 1;
		while self.peek().is_some_and(|b| {
			b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':') || b >= 0x80
		}) {
			self.at += 1;
		}
		Ok(&self.bytes[start..self.at])
	}
}

/// Whether a character reference's digits name a character XML allows.
fn code_point_is_xml_char(digits: &[u8], radix: u32) -> bool {
	let Ok(text) = std::str::from_utf8(digits) else {
		return false;
	};
	let Ok(value) = u32::from_str_radix(text, radix) else {
		return false;
	};
	matches!(
		value,
		0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ok(text: &str) {
		if let Err(err) = check_content(text) {
			panic!("{text:?} should be well-formed: {err:?}");
		}
	}

	fn refused(text: &str) {
		assert!(check_content(text).is_err(), "{text:?} should be refused");
	}

	#[test]
	fn the_xml_declaration_follows_its_grammar() {
		ok("<?xml version=\"1.0\"?><a/>");
		ok("<?xml version='1.1' encoding=\"UTF-8\" standalone='yes' ?><a/>");
		ok("<?xml version = \"1.0\"\n encoding='iso-8859-1'?><a/>");
		for bad in [
			"<?xml bogus?><a/>",
			"<?xml?><a/>",
			"<?xml encoding=\"UTF-8\"?><a/>",
			"<?xml version=\"2.0\"?><a/>",
			"<?xml version=\"1.\"?><a/>",
			"<?xml version=\"1.0\" standalone=\"yes\" encoding=\"UTF-8\"?><a/>",
			"<?xml version=\"1.0\" standalone=\"maybe\"?><a/>",
			"<?xml version=\"1.0\"encoding=\"UTF-8\"?><a/>",
			"<?xml version=\"1.0\" encoding=\"8bit\"?><a/>",
			"<?xml version=1.0?><a/>",
			"<?xml version=\"1.0\" version=\"1.0\"?><a/>",
			"<?xml version=\"1.0\"",
		] {
			refused(bad);
		}
	}

	#[test]
	fn content_accepts_documents_fragments_and_text() {
		ok("");
		ok("<a/>");
		ok("<a>text</a>");
		ok("<a><b>1</b><b>2</b></a>");
		ok("<a>x</a><b>y</b>");
		ok("plain text");
		ok("text <a/> more");
		ok("<a x=\"1\" y='two'>&amp; &lt; &#65; &#x42;</a>");
		ok("<?xml version=\"1.0\"?><root/>");
		ok("<!-- note --><root><?pi data?></root>");
		ok("<?xml-stylesheet href=\"x\"?><a/>");
		ok("<root><![CDATA[ <not> & markup ]]></root>");
		ok("<ns:a xmlns:ns=\"u\"><ns:b/></ns:a>");
		ok("<a  b = \"c\"\n d=\"e\"/>");
		ok("<a>é</a>");
	}

	#[test]
	fn content_refuses_unbalanced_and_misnamed_elements() {
		refused("<a>");
		refused("</a>");
		refused("<a></b>");
		refused("<a><b></a></b>");
		refused("<1a/>");
		refused("<a b/ >");
		refused("<a b=\"1\" b=\"2\"/>");
		refused("<a b=1/>");
		refused("<a b=\"1\"c=\"2\"/>");
		refused("<a");
		refused("<a><");
	}

	#[test]
	fn content_refuses_bad_text_and_references() {
		refused("a & b");
		refused("&unknown;");
		refused("&#0;");
		refused("&#xD800;");
		refused("&amp");
		refused("<a>x < y</a>");
		refused("a ]]> b");
		refused("<a b=\"x<y\"/>");
		refused("<a b=\"&bad;\"/>");
		refused("\u{1}");
	}

	#[test]
	fn a_character_xml_does_not_allow_is_refused_inside_markup_too() {
		refused("<a x=\"\u{1}\"/>");
		refused("<!-- \u{1} -->");
		refused("<![CDATA[\u{1}]]>");
		refused("<?pi \u{1}?>");
		refused("<?xml version=\"1.0\u{1}\"?><a/>");
		refused("<a>\u{FFFE}</a>");
		ok("<a x=\"\t\">\r\n\u{10000}</a>");
	}

	#[test]
	fn content_refuses_comments_processing_instructions_and_doctype() {
		refused("<!-- a -- b -->");
		refused("<!-- open");
		refused("<?xml version=\"1.0\"?><a/><?xml version=\"1.0\"?>");
		refused("<!DOCTYPE a><a/>");
		refused("<![CDATA[ open");
		refused("<?pi open");
	}

	#[test]
	fn reserved_target_is_checked_case_insensitively() {
		refused("<?XML data?>");
	}
}
