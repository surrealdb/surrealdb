/// A struct containing some type of code, along with it's origin.
///

pub enum SourceKind {
	Whole,
	Snippet {
		offset_line: usize,
		offset_column: usize,
	},
}

pub struct Source<'a> {
	origin: Cow<'a, str>,
	kind: SourceKind,
}
