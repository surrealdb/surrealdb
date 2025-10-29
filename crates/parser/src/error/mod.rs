use std::borrow::Cow;

use ast::Span;

pub enum Level {
	Error,
	Warning,
}

pub struct Group<'a> {
	level: Level,
	title: Cow<'a, str>,
	elements: Vec<Element<'a>>,
}

pub enum Element<'a> {
	Snippet(Snippet<'a>),
}

pub struct Snippet<'a> {
	pub(crate) origin: Option<Cow<'a, str>>,
	pub(crate) source: Cow<'a, str>,
	pub(crate) annotations: Vec<Annotation<'a>>,
}

pub enum AnnotationKind {
	Primary,
	Context,
}

pub struct Annotation<'a> {
	kind: AnnotationKind,
	span: Span,
	label: Option<Cow<'a, str>>,
}

pub struct ParseError {
	pub span: Span,
	pub message: String,
}
