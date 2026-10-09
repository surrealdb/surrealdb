//! Statements, one module per statement family.

use crate::parse::*;

mod alter;
mod define;
pub(crate) use define::is_distance;
mod misc;
mod remove;
mod select;
mod write;

/// Whether `t` starts a statement that is also an expression: the keywords
/// `syn` parses as primes.
pub(crate) fn is_statement_keyword(p: &Parser<'_, '_>, t: Tok) -> bool {
	t.kind == T::Ident
		&& match t.kw {
			Kw::Select
			| Kw::Create
			| Kw::Update
			| Kw::Upsert
			| Kw::Delete
			| Kw::Relate
			| Kw::Insert
			| Kw::Define
			| Kw::Remove
			| Kw::Rebuild
			| Kw::Alter
			| Kw::Info
			| Kw::If
			| Kw::Explain
			| Kw::For
			| Kw::Let
			| Kw::Return
			| Kw::Throw
			| Kw::Break
			| Kw::Continue => true,
			Kw::Sleep => p.peek1().kind != T::LParen,
			_ => false,
		}
}

impl<'a, 's> Parser<'a, 's> {
	/// A statement that may only appear at the top level, at its keyword
	/// `t`; `None` when `t` does not start one.
	pub(crate) fn top_level_statement(&mut self, t: Tok) -> R<Option<NodeId>> {
		let tag = match t.kw {
			Kw::Begin => Tag::Begin,
			Kw::Cancel => Tag::Cancel,
			Kw::Commit => Tag::Commit,
			Kw::Use | Kw::Option | Kw::Live | Kw::Kill | Kw::Show => Tag::Use,
			Kw::Access => Tag::AccessStmt,
			_ => return Ok(None),
		};
		self.bump();
		let s = t.start;
		match t.kw {
			Kw::Use => self.use_stmt(s),
			Kw::Option => self.option_stmt(s),
			Kw::Live => self.live_stmt(s),
			Kw::Kill => self.kill_stmt(s),
			Kw::Show => self.show_stmt(s),
			Kw::Access => self.access_stmt(s),
			_ => self.transaction(tag, t),
		}
		.map(Some)
	}

	/// The statement whose keyword `t` was just consumed.
	pub(crate) fn statement_body(&mut self, t: Tok) -> R<NodeId> {
		let start = t.start;
		match t.kw {
			Kw::Select => self.select(start),
			Kw::Create => self.write(Tag::Create, start),
			Kw::Update => self.write(Tag::Update, start),
			Kw::Upsert => self.write(Tag::Upsert, start),
			Kw::Delete => {
				self.eat_kw(Kw::From);
				self.write(Tag::Delete, start)
			}
			Kw::Return => self.return_stmt(start),
			Kw::Let => self.let_stmt(start),
			Kw::Sleep => self.sleep_stmt(start),
			Kw::Throw => self.throw_stmt(start),
			Kw::Break => Ok(self.leaf(Tag::Break, 0, t)),
			Kw::Continue => Ok(self.leaf(Tag::Continue, 0, t)),
			Kw::Info => self.info_stmt(start),
			Kw::Rebuild => self.rebuild_stmt(start),
			Kw::Define => self.define(start),
			Kw::Explain => self.explain_stmt(start),
			Kw::Relate => self.relate(start),
			Kw::If => self.if_stmt(start),
			Kw::Remove => self.remove(start),
			Kw::Alter => self.alter(start),
			Kw::For => self.for_stmt(start),
			Kw::Insert => self.insert(start),
			// Only called for `is_statement_keyword` tokens.
			_ => Err(self.error(t.start, t.end, "expected a statement")),
		}
	}
}
