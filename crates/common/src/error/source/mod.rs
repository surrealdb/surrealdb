//! Module implementing functionality for errors which are related to some source code.

mod render;
pub use render::Renderer;

use std::{
	borrow::Cow,
	error::Error,
	fmt::{self},
};

use crate::error::TypedError;
use crate::span::Span;

#[derive(Debug)]
pub enum OptionCow<'a> {
	Owned(String),
	Borrowed(&'a str),
	None,
}

impl<'a> From<&'a str> for OptionCow<'a> {
	fn from(value: &'a str) -> Self {
		OptionCow::Borrowed(value)
	}
}
impl<'a> From<String> for OptionCow<'a> {
	fn from(value: String) -> Self {
		OptionCow::Owned(value)
	}
}

impl<'a, T> From<Option<T>> for OptionCow<'a>
where
	T: Into<Cow<'a, str>>,
{
	fn from(value: Option<T>) -> Self {
		if let Some(x) = value {
			match x.into() {
				Cow::Borrowed(x) => OptionCow::Borrowed(x),
				Cow::Owned(x) => OptionCow::Owned(x),
			}
		} else {
			OptionCow::None
		}
	}
}

#[derive(Debug)]
pub enum Level {
	Error,
	Warning,
}

impl Level {
	pub fn title(self, t: impl Into<String>) -> Group {
		Group {
			level: self,
			title: t.into(),
			elements: Vec::new(),
			cause: None,
		}
	}
}

#[derive(Debug)]
pub struct Group<R: Eq = ()> {
	level: Level,
	title: String,
	elements: Vec<Snippet<R>>,
	cause: Option<Box<Group>>,
}

impl Group {
	pub fn element(mut self, annotation: Snippet) -> Self {
		let element = annotation;
		self.elements.push(element);
		self
	}
}

#[derive(Debug)]
pub enum AnnotationKind {
	Primary,
	Context,
}

impl AnnotationKind {
	pub fn span(self, span: Span) -> Annotation {
		Annotation {
			kind: self,
			span,
			label: OptionCow::None,
		}
	}
}

#[derive(Debug)]
pub struct Snippet<R: Eq = ()> {
	pub(crate) origin: OptionCow<'static>,
	pub(crate) source: SnippetSource<R>,
	pub(crate) annotations: Vec<Annotation>,
}

impl Snippet<()> {
	/// Returns a snippet which references the base source, for when there is only one kind of
	/// source which is relevent to the error.
	pub fn base() -> Self {
		Self::references(())
	}
}

impl<R: Eq> Snippet<R> {
	pub fn references(r: R) -> Self {
		Snippet {
			origin: OptionCow::None,
			source: SnippetSource::Reference(r),
			annotations: Vec::new(),
		}
	}

	pub fn source(r: R) -> Self {
		Snippet {
			origin: OptionCow::None,
			source: SnippetSource::Reference(r),
			annotations: Vec::new(),
		}
	}

	pub fn origin<T>(mut self, s: T) -> Self
	where
		T: Into<OptionCow<'static>>,
	{
		self.origin = s.into();
		self
	}

	pub fn annotate(mut self, annotation: Annotation) -> Self {
		self.annotations.push(annotation);
		self
	}
}

#[derive(Debug)]
pub enum SnippetSource<R: Eq = ()> {
	/// The full source code is provided inline
	Inline(String),
	/// The diagnostic is later given the actual source on which the diagnostic is defined.
	Reference(R),
}

#[derive(Debug)]
pub struct Annotation {
	kind: AnnotationKind,
	span: Span,
	label: OptionCow<'static>,
}

impl Annotation {
	pub fn label<T>(mut self, label: T) -> Annotation
	where
		T: Into<OptionCow<'static>>,
	{
		self.label = label.into();
		self
	}
}

/// An error type which is associated with snippet of code.
#[derive(Debug)]
pub struct SourceDiagnostic<R: Eq = ()> {
	groups: Vec<Group<R>>,
}

impl<R> SourceDiagnostic<R>
where
	R: Eq,
{
	// Provides a source text to the error.
	pub fn provide_source(&mut self, id: R, source: &str) {
		for g in self.groups.iter_mut() {
			for e in g.elements.iter_mut() {
				match &e.source {
					SnippetSource::Reference(r) if *r == id => {
						e.source = SnippetSource::Inline(source.to_owned());
					}
					_ => {}
				}
			}
		}
	}
}

impl Error for SourceDiagnostic {}

impl fmt::Display for SourceDiagnostic {
	fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
		Ok(())
	}
}

impl From<Group> for TypedError<SourceDiagnostic> {
	fn from(value: Group) -> Self {
		TypedError::new(SourceDiagnostic {
			groups: vec![value],
		})
	}
}
