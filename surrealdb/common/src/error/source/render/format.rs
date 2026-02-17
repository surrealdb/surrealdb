use super::char_buffer::{CharBuffer, Color, Styling};
use crate::error::source::{AnnotationKind, Diagnostic, Level, Snippet};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Loc {
	pub line: usize,
	pub column: usize,
}

impl Loc {
	pub fn from_offset_in_str(s: &str, offset: usize) -> Self {
		let mut line = 0;
		let mut start_line = 0;
		for (idx, _) in s.match_indices("\n") {
			line += 1;
			if offset <= idx {
				line -= 1;
				break;
			}
			start_line = idx + 1;
		}

		let mut col = 0;
		for (i, _) in s[start_line..].char_indices() {
			col += 1;
			if offset - start_line <= i {
				col -= 1;
				break;
			}
		}

		Loc {
			line,
			column: col,
		}
	}
}

pub fn render_string(g: &Diagnostic<'_>) -> String {
	render_char_buffer(g).to_string()
}

pub fn render_char_buffer(g: &Diagnostic<'_>) -> CharBuffer {
	let primary_group = g.groups.first().expect("Diagnostic must atleast have a single group");

	let mut buffer = CharBuffer::new();

	let color = match primary_group.level {
		Level::Error => Color::Red,
		Level::Warning => Color::Yellow,
	};

	buffer
		.writer()
		.color(color)
		.style(Styling::Bold)
		.push_str("Error")
		.color(Color::Default)
		.style(Styling::Bold)
		.push_str(": ")
		.push_str(primary_group.title.as_ref())
		.push_str("\n");

	for e in primary_group.elements.iter() {
		render_element(&mut buffer, e, color);
	}

	buffer
}

fn render_element(buf: &mut CharBuffer, elem: &Snippet, line_color: Color) {
	let Some(source) = elem.source.as_ref() else {
		return;
	};

	let largest_offset =
		elem.annotations.iter().map(|x| x.span.end as usize).max().unwrap_or_default();

	let largest_loc = Loc::from_offset_in_str(source, largest_offset);
	let line_number_char_n = (largest_loc.line + 1).ilog10() + 1;
	let line_n_indent = line_number_char_n as usize + 1;

	let Some(prime) = elem.annotations.iter().find(|e| e.kind == AnnotationKind::Primary) else {
		return;
	};

	let prime_loc = Loc::from_offset_in_str(source, prime.span.start as usize);

	let mut anns = elem
		.annotations
		.iter()
		.map(|x| {
			(
				x,
				Loc::from_offset_in_str(source, x.span.start as usize),
				Loc::from_offset_in_str(source, x.span.end as usize),
			)
		})
		.collect::<Vec<_>>();

	anns.sort_unstable_by(|a, b| a.1.line.cmp(&b.1.line).then_with(|| a.0.kind.cmp(&b.0.kind)));

	buf.writer()
		.indent(line_n_indent)
		.color(line_color)
		.push_str("|>")
		.color(Color::Default)
		.push_fmt(format_args!(
			" {}:{}:{}\n",
			elem.origin.as_ref().unwrap_or("???"),
			prime_loc.line + 1,
			prime_loc.column + 1
		))
		.color(line_color)
		.push_str("|\n");

	let mut last_line = None;
	for (ann, start, end) in anns {
		if let Some(last_line) = last_line {
			if last_line != start.line && last_line + 1 != start.line {
				buf.writer()
					.indent(line_n_indent)
					.color(line_color)
					.push_str("| ")
					.color(Color::Default)
					.push_str("...\n");
			}
		}

		let line_n = start.line + 1;
		let line_char_n = line_n.ilog10() + 1;
		for _ in 0..(line_n_indent as u32 - line_char_n - 1) {
			buf.writer().push_str(" ");
		}

		let line = source.lines().nth(start.line).unwrap_or("INVALID LINE");

		buf.writer()
			.push_fmt(format_args!("{line_n} "))
			.color(line_color)
			.push_str("| ")
			.color(Color::Default)
			.push_str(line)
			.indent(line_n_indent)
			.color(line_color)
			.push_str("\n| ");

		for _ in 0..start.column {
			buf.writer().push_str(" ");
		}

		let underline_color = if AnnotationKind::Primary == ann.kind {
			line_color
		} else {
			Color::Blue
		};

		if end.line == start.line {
			for _ in 0..end.column.saturating_sub(start.column).max(1) {
				buf.writer().color(underline_color).push_str("^");
			}
			buf.writer().color(underline_color).push_str(" ");
		} else {
			buf.writer().color(underline_color).push_str("^... ");
		}
		if let Some(x) = ann.label.as_ref() {
			buf.writer().push_str(x);
		}
		buf.writer().push_str("\n");

		last_line = Some(start.line)
	}
}
