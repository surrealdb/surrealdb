//! Request statements compiled from flat syntax, with lazy observer rendering.

use std::ops::Range;
use std::sync::Arc;

use surrealdb_parser_next::Tag;
use surrealdb_types::{SqlFormat, ToSql};

use super::{ParserSettings, TopLevelExpr};
use crate::expr::logical::LogicalOp;

/// The syntactic category of a statement compiled directly from its flat tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind {
	Select,
	Create,
	Update,
	Upsert,
	Delete,
	Relate,
	Insert,
	Info,
	Foreach,
	IfElse,
	Sleep,
	Explain,
	Let,
	Return,
	Break,
	Continue,
	Throw,
	Block,
	Other,
}

impl StatementKind {
	pub(super) fn from_tag(tag: Tag) -> Self {
		match tag {
			Tag::Select => Self::Select,
			Tag::Create => Self::Create,
			Tag::Update => Self::Update,
			Tag::Upsert => Self::Upsert,
			Tag::Delete => Self::Delete,
			Tag::Relate => Self::Relate,
			Tag::Insert => Self::Insert,
			Tag::Info => Self::Info,
			Tag::For => Self::Foreach,
			Tag::If => Self::IfElse,
			Tag::Sleep => Self::Sleep,
			Tag::Explain => Self::Explain,
			Tag::Let => Self::Let,
			Tag::Return => Self::Return,
			Tag::Break => Self::Break,
			Tag::Continue => Self::Continue,
			Tag::Throw => Self::Throw,
			Tag::Block => Self::Block,
			_ => Self::Other,
		}
	}
}

/// Validated source retained only for canonical, redacted observer text.
/// Rendering uses the settings which admitted the statement and never prepares
/// credentials. All statements of one request share its source allocation.
#[derive(Clone)]
pub struct StatementText {
	pub(super) source: Arc<str>,
	pub(super) span: Range<usize>,
	pub(super) settings: ParserSettings,
}

impl ToSql for StatementText {
	fn fmt_sql(&self, output: &mut String, fmt: SqlFormat) {
		let query = super::entry::query(&self.source[self.span.clone()], &self.settings.next())
			.expect("validated statement remains valid under its admission settings");
		debug_assert_eq!(query.expressions.len(), 1);
		query.expressions[0].fmt_redacted_sql(output, fmt);
	}
}

/// A statement and the metadata required by the request executor.
pub struct LogicalStatement {
	pub plan: LogicalOp,
	pub kind: StatementKind,
	pub text: StatementText,
}

/// Driver directives retain their dedicated payload. Engine statements carry
/// the logical form compiled from the flat parser arena.
pub enum ExecutionStatement {
	Surface(TopLevelExpr),
	Logical(Box<LogicalStatement>),
}
