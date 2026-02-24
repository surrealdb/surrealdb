mod render;

use std::borrow::Cow;
use std::fmt;

use crate::span::Span;

#[derive(Debug)]
pub enum OptionCow<'a> {
	None,
	Owned(String),
	Borrow(&'a str),
}

impl<'a> From<String> for OptionCow<'a> {
	fn from(value: String) -> Self {
		OptionCow::Owned(value)
	}
}
impl<'a> From<&'a str> for OptionCow<'a> {
	fn from(value: &'a str) -> Self {
		OptionCow::Borrow(value)
	}
}

impl<'a, T> From<Option<T>> for OptionCow<'a>
where
	OptionCow<'a>: From<T>,
{
	fn from(v: Option<T>) -> Self {
		match v {
			Some(x) => Self::from(x),
			None => OptionCow::None,
		}
	}
}

impl<'a> OptionCow<'a> {
	pub fn to_owned(self) -> OptionCow<'static> {
		match self {
			OptionCow::None => OptionCow::None,
			OptionCow::Owned(x) => OptionCow::Owned(x),
			OptionCow::Borrow(x) => OptionCow::Owned(x.to_owned()),
		}
	}

	pub fn as_ref(&self) -> Option<&str> {
		match self {
			OptionCow::None => None,
			OptionCow::Owned(x) => Some(x.as_ref()),
			OptionCow::Borrow(x) => Some(x.as_ref()),
		}
	}
}

#[derive(Debug)]
pub struct Diagnostic<'a> {
	groups: Vec<Group<'a>>,
}

impl<'a> Diagnostic<'a> {
	pub fn to_owned(self) -> Diagnostic<'static> {
		Diagnostic {
			groups: self.groups.into_iter().map(|x| x.to_owned()).collect(),
		}
	}

	pub fn map_source<Fs, Fp>(&mut self, mut map_source: Fs, mut map_span: Fp)
	where
		Fs: FnMut(&mut OptionCow<'a>),
		Fp: FnMut(&mut Span),
	{
		for g in self.groups.iter_mut() {
			for s in g.elements.iter_mut() {
				map_source(&mut s.source);
				for a in s.annotations.iter_mut() {
					map_span(&mut a.span);
				}
			}
		}
	}
}

impl fmt::Display for Diagnostic<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.render_string())
	}
}
impl crate::error::ErrorTrait for Diagnostic<'static> {}

#[derive(Debug)]
pub enum Level {
	Error,
	Warning,
}

impl Level {
	pub fn title<'a, T>(self, title: T) -> Group<'a>
	where
		Cow<'a, str>: From<T>,
	{
		Group {
			level: self,
			title: title.into(),
			elements: Vec::new(),
		}
	}
}

#[derive(Debug)]
pub struct Group<'a> {
	level: Level,
	title: Cow<'a, str>,
	elements: Vec<Snippet<'a>>,
}

impl<'a> Group<'a> {
	pub fn to_owned(self) -> Group<'static> {
		Group {
			level: self.level,
			title: Cow::Owned(self.title.into_owned()),
			elements: self.elements.into_iter().map(|x| x.to_owned()).collect(),
		}
	}

	pub fn snippet(mut self, s: Snippet<'a>) -> Group<'a> {
		self.elements.push(s);
		self
	}

	pub fn to_diagnostic(self) -> Diagnostic<'a> {
		Diagnostic {
			groups: vec![self],
		}
	}
}

#[derive(Debug)]
pub struct Snippet<'a> {
	origin: OptionCow<'a>,
	source: OptionCow<'a>,
	annotations: Vec<Annotation<'a>>,
}

impl<'a> Snippet<'a> {
	pub fn source<T>(s: T) -> Self
	where
		OptionCow<'a>: From<T>,
	{
		Snippet {
			origin: OptionCow::None,
			source: s.into(),
			annotations: Vec::new(),
		}
	}

	pub fn origin<T>(mut self, s: T) -> Self
	where
		OptionCow<'a>: From<T>,
	{
		self.origin = s.into();
		self
	}

	pub fn annotate(mut self, annotation: Annotation<'a>) -> Self {
		self.annotations.push(annotation);
		self
	}

	pub fn to_owned(self) -> Snippet<'static> {
		Snippet {
			origin: self.origin.to_owned(),
			source: self.source.to_owned(),
			annotations: self.annotations.into_iter().map(|x| x.to_owned()).collect(),
		}
	}
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, PartialOrd, Ord)]
pub enum AnnotationKind {
	Primary,
	Context,
}

impl AnnotationKind {
	pub fn span<'a>(self, span: Span) -> Annotation<'a> {
		Annotation {
			kind: self,
			span,
			label: OptionCow::None,
		}
	}
}

#[derive(Debug)]
pub struct Annotation<'a> {
	kind: AnnotationKind,
	span: Span,
	label: OptionCow<'a>,
}

impl<'a> Annotation<'a> {
	pub fn label<T>(mut self, label: T) -> Self
	where
		OptionCow<'a>: From<T>,
	{
		self.label = label.into();
		self
	}

	pub fn to_owned(self) -> Annotation<'static> {
		Annotation {
			kind: self.kind,
			span: self.span,
			label: self.label.to_owned(),
		}
	}
}
