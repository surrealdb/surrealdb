use std::fmt::Display;

use crate::syn::token::Span;

mod location;
mod mac;
mod render;
pub use location::Location;
pub(crate) use mac::{bail, syntax_error};
pub use render::{RenderedError, Snippet};

#[derive(Debug, Clone, Copy)]
pub enum MessageKind {
	Suggestion,
	Error,
}

#[derive(Debug)]
enum DiagnosticKind {
	Cause(String),
	Span {
		kind: MessageKind,
		span: Span,
		label: Option<String>,
	},
}

#[derive(Debug)]
pub struct Diagnostic {
	kind: DiagnosticKind,
	next: Option<Box<Diagnostic>>,
}

/// A parsing error.
#[derive(Debug)]
pub struct SyntaxError {
	diagnostic: Box<Diagnostic>,
}

impl SyntaxError {
	/// Create a new parse error.
	#[cold]
	pub fn new<T>(message: T) -> Self
	where
		T: Display,
	{
		//println!("{}", std::backtrace::Backtrace::force_capture());

		let diagnostic = Diagnostic {
			kind: DiagnosticKind::Cause(message.to_string()),
			next: None,
		};

		Self {
			diagnostic: Box::new(diagnostic),
		}
	}

	#[cold]
	pub fn with_span(mut self, span: Span, kind: MessageKind) -> Self {
		self.diagnostic = Box::new(Diagnostic {
			kind: DiagnosticKind::Span {
				kind,
				span,
				label: None,
			},
			next: Some(self.diagnostic),
		});
		self
	}

	#[cold]
	pub fn with_labeled_span<T: Display>(
		mut self,
		span: Span,
		kind: MessageKind,
		label: T,
	) -> Self {
		self.diagnostic = Box::new(Diagnostic {
			kind: DiagnosticKind::Span {
				kind,
				span,
				label: Some(label.to_string()),
			},
			next: Some(self.diagnostic),
		});
		self
	}

	#[cold]
	pub fn with_cause<T: Display>(mut self, t: T) -> Self {
		self.diagnostic = Box::new(Diagnostic {
			kind: DiagnosticKind::Cause(t.to_string()),
			next: Some(self.diagnostic),
		});
		self
	}

	pub fn render_on(&self, source: &str) -> RenderedError {
		let mut res = RenderedError {
			errors: Vec::new(),
			snippets: Vec::new(),
		};
		Self::render_on_inner(&self.diagnostic, source, &mut res);
		res
	}

	pub fn render_on_bytes(&self, source: &[u8]) -> RenderedError {
		let source = String::from_utf8_lossy(source);
		self.render_on(&source)
	}

	/// Traverses all diagnostics and hands a mutable reference to the diagnostic span to the given
	/// callback.
	pub fn update_spans<F: FnMut(&mut Span)>(mut self, mut cb: F) -> Self {
		let mut cur = &mut *self.diagnostic;

		loop {
			match cur.kind {
				DiagnosticKind::Cause(_) => {}
				DiagnosticKind::Span {
					ref mut span,
					..
				} => cb(span),
			}
			let Some(next) = cur.next.as_mut() else {
				break;
			};
			cur = &mut *next;
		}

		self
	}

	fn render_on_inner(diagnostic: &Diagnostic, source: &str, res: &mut RenderedError) {
		if let Some(ref x) = diagnostic.next {
			Self::render_on_inner(x, source, res);
		}

		match diagnostic.kind {
			DiagnosticKind::Cause(ref x) => res.errors.push(x.clone()),
			DiagnosticKind::Span {
				ref span,
				ref label,
				ref kind,
			} => {
				let locations = Location::range_of_span(source, *span);
				let snippet = Snippet::from_source_location_range(
					source,
					locations,
					label.as_ref().map(|x| x.as_str()),
					*kind,
				);
				res.snippets.push(snippet)
			}
		}
	}
}
