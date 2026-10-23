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

impl LogicalStatement {
	/// What it takes to compile this statement again: its text and the settings that admitted
	/// it. Cheap to take, as the text is shared with every statement of the request.
	pub fn source(&self) -> StatementSource {
		StatementSource {
			kind: self.kind,
			text: self.text.clone(),
		}
	}
}

/// The text of a [`LogicalStatement`], kept so that a statement whose plan a run consumed can be
/// compiled again for another run ([`Self::compile`]).
#[derive(Clone)]
pub struct StatementSource {
	kind: StatementKind,
	text: StatementText,
}

impl StatementSource {
	/// Compile the statement again, under the settings that admitted it the first time.
	///
	/// The text compiled once, so it compiles again to the same plan; `None` when it does not,
	/// which a caller treats as a statement it cannot run again.
	pub fn compile(&self) -> Option<LogicalStatement> {
		let src = self.text.source.get(self.text.span.clone())?;
		let mut statements = super::entry::execution_query(src, self.text.settings).ok()?;
		if statements.len() != 1 {
			return None;
		}
		match statements.pop()? {
			ExecutionStatement::Logical(statement) => Some(LogicalStatement {
				plan: statement.plan,
				kind: self.kind,
				text: self.text.clone(),
			}),
			ExecutionStatement::Surface(_) => None,
		}
	}
}

/// Driver directives retain their dedicated payload. Engine statements carry
/// the logical form compiled from the flat parser arena.
pub enum ExecutionStatement {
	Surface(TopLevelExpr),
	Logical(Box<LogicalStatement>),
}
