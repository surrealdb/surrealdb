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
		if tag == Tag::Select {
			Self::Select
		} else if tag == Tag::Create {
			Self::Create
		} else if tag == Tag::Update {
			Self::Update
		} else if tag == Tag::Upsert {
			Self::Upsert
		} else if tag == Tag::Delete {
			Self::Delete
		} else if tag == Tag::Relate {
			Self::Relate
		} else if tag == Tag::Insert {
			Self::Insert
		} else if tag == Tag::Info {
			Self::Info
		} else if tag == Tag::For {
			Self::Foreach
		} else if tag == Tag::If {
			Self::IfElse
		} else if tag == Tag::Sleep {
			Self::Sleep
		} else if tag == Tag::Explain {
			Self::Explain
		} else if tag == Tag::Let {
			Self::Let
		} else if tag == Tag::Return {
			Self::Return
		} else if tag == Tag::Break {
			Self::Break
		} else if tag == Tag::Continue {
			Self::Continue
		} else if tag == Tag::Throw {
			Self::Throw
		} else if tag == Tag::Block {
			Self::Block
		} else {
			Self::Other
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
