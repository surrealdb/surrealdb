use ast::Span;

pub struct ParseError {
	pub span: Span,
	pub message: String,
}
