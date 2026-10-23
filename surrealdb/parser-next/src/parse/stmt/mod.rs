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
/// parsed as primes.
pub(crate) fn is_statement_keyword(p: &Parser<'_, '_>, t: Tok) -> bool {
	if t.kind != T::Ident {
		return false;
	}
	if t.kw == Kw::Select
		|| t.kw == Kw::Create
		|| t.kw == Kw::Update
		|| t.kw == Kw::Upsert
		|| t.kw == Kw::Delete
		|| t.kw == Kw::Relate
		|| t.kw == Kw::Insert
		|| t.kw == Kw::Define
		|| t.kw == Kw::Remove
		|| t.kw == Kw::Rebuild
		|| t.kw == Kw::Alter
		|| t.kw == Kw::Info
		|| t.kw == Kw::If
		|| t.kw == Kw::Explain
		|| t.kw == Kw::For
		|| t.kw == Kw::Let
		|| t.kw == Kw::Return
		|| t.kw == Kw::Throw
		|| t.kw == Kw::Break
		|| t.kw == Kw::Continue
	{
		true
	} else if t.kw == Kw::Sleep {
		p.peek1().kind != T::LParen
	} else {
		false
	}
}

impl<'a, 's> Parser<'a, 's> {
	/// A statement that may only appear at the top level, at its keyword
	/// `t`; `None` when `t` does not start one.
	pub(crate) fn top_level_statement(&mut self, t: Tok) -> R<Option<NodeId>> {
		let tag = if t.kw == Kw::Begin {
			Tag::Begin
		} else if t.kw == Kw::Cancel {
			Tag::Cancel
		} else if t.kw == Kw::Commit {
			Tag::Commit
		} else if t.kw == Kw::Use
			|| t.kw == Kw::Option
			|| t.kw == Kw::Live
			|| t.kw == Kw::Kill
			|| t.kw == Kw::Show
		{
			Tag::Use
		} else if t.kw == Kw::Access {
			Tag::AccessStmt
		} else {
			return Ok(None);
		};
		self.bump();
		let s = t.start;
		let res = if t.kw == Kw::Use {
			self.use_stmt(s)
		} else if t.kw == Kw::Option {
			self.option_stmt(s)
		} else if t.kw == Kw::Live {
			self.live_stmt(s)
		} else if t.kw == Kw::Kill {
			self.kill_stmt(s)
		} else if t.kw == Kw::Show {
			self.show_stmt(s)
		} else if t.kw == Kw::Access {
			self.access_stmt(s)
		} else {
			self.transaction(tag, t)
		};
		res.map(Some)
	}

	/// The statement whose keyword `t` was just consumed.
	pub(crate) fn statement_body(&mut self, t: Tok) -> R<NodeId> {
		let start = t.start;
		if t.kw == Kw::Select {
			self.select(start)
		} else if t.kw == Kw::Create {
			self.write(Tag::Create, start)
		} else if t.kw == Kw::Update {
			self.write(Tag::Update, start)
		} else if t.kw == Kw::Upsert {
			self.write(Tag::Upsert, start)
		} else if t.kw == Kw::Delete {
			self.eat_kw(Kw::From);
			self.write(Tag::Delete, start)
		} else if t.kw == Kw::Return {
			self.return_stmt(start)
		} else if t.kw == Kw::Let {
			self.let_stmt(start)
		} else if t.kw == Kw::Sleep {
			self.sleep_stmt(start)
		} else if t.kw == Kw::Throw {
			self.throw_stmt(start)
		} else if t.kw == Kw::Break {
			Ok(self.leaf(Tag::Break, 0, t))
		} else if t.kw == Kw::Continue {
			Ok(self.leaf(Tag::Continue, 0, t))
		} else if t.kw == Kw::Info {
			self.info_stmt(start)
		} else if t.kw == Kw::Rebuild {
			self.rebuild_stmt(start)
		} else if t.kw == Kw::Define {
			self.define(start)
		} else if t.kw == Kw::Explain {
			self.explain_stmt(start)
		} else if t.kw == Kw::Relate {
			self.relate(start)
		} else if t.kw == Kw::If {
			self.if_stmt(start)
		} else if t.kw == Kw::Remove {
			self.remove(start)
		} else if t.kw == Kw::Alter {
			self.alter(start)
		} else if t.kw == Kw::For {
			self.for_stmt(start)
		} else if t.kw == Kw::Insert {
			self.insert(start)
		} else {
			// Only called for `is_statement_keyword` tokens.
			Err(self.error(t.start, t.end, "expected a statement"))
		}
	}
}
