//! Settings and semantic rejection spans for runtime expression construction.

#[derive(Clone, Copy, Debug)]
pub struct Options {
	/// A plain string that reads as a uuid, datetime or record id is that
	/// value (`legacy_strands`).
	pub legacy_strands: bool,
	/// File literals and buckets are allowed (`files_enabled`).
	pub files: bool,
	/// Surrealism extensions are allowed (`surrealism_enabled`).
	pub surrealism: bool,
}

impl Options {
	/// Every optional grammar enabled and strings read as written.
	pub const ALL: Options = Options {
		legacy_strands: false,
		files: true,
		surrealism: true,
	};
}

/// A lowering failure: a semantic check made at parse time, in the
/// established wording, with the span it points at when it has one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reject {
	pub msg: std::borrow::Cow<'static, str>,
	pub span: Option<(u32, u32)>,
	pub label: Option<std::borrow::Cow<'static, str>>,
	/// A second span with its label.
	pub extra: Option<(u32, u32, std::borrow::Cow<'static, str>)>,
}

impl Reject {
	pub fn new(msg: impl Into<std::borrow::Cow<'static, str>>) -> Self {
		Reject {
			msg: msg.into(),
			span: None,
			label: None,
			extra: None,
		}
	}

	pub fn at(mut self, start: u32, end: u32) -> Self {
		self.span = Some((start, end));
		self
	}

	pub fn with_label(mut self, label: impl Into<std::borrow::Cow<'static, str>>) -> Self {
		self.label = Some(label.into());
		self
	}
}
