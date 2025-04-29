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
	data_pending: bool,
}

impl SyntaxError {
	/// Create a new parse error.
	pub fn new<T>(message: T) -> Self
	where
		T: Display,
	{
		let diagnostic = Diagnostic {
			kind: DiagnosticKind::Cause(message.to_string()),
			next: None,
		};

		Self {
			diagnostic: Box::new(diagnostic),
			data_pending: false,
		}
	}

	/// Returns whether this error is possibly the result of missing data.
	pub fn is_data_pending(&self) -> bool {
		self.data_pending
	}

	/// Indicate that this error might be the result of missing data and could
	/// be resolved with more data.
	pub fn with_data_pending(mut self) -> Self {
		self.data_pending = true;
		self
	}

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
