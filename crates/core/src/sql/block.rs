use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::fmt::{is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::statements::{
	AlterStatement, BreakStatement, ContinueStatement, CreateStatement, DefineStatement,
	DeleteStatement, ForeachStatement, IfelseStatement, InsertStatement, OutputStatement,
	RelateStatement, RemoveStatement, SelectStatement, SetStatement, ThrowStatement,
	UpdateStatement, UpsertStatement,
};
use crate::sql::value::Value;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

use super::statements::InfoStatement;
use super::FlowResult;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Block";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Block")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Block(pub Vec<Entry>);

impl Deref for Block {
	type Target = Vec<Entry>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Value> for Block {
	fn from(v: Value) -> Self {
		Block(vec![Entry::Value(v)])
	}
}

impl From<Block> for crate::expr::Block {
	fn from(v: Block) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Block> for Block {
	fn from(v: crate::expr::Block) -> Self {
		Self(v.0.into_iter().map(Into::into).collect())
	}
}

impl Block {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.iter().any(Entry::writeable)
	}
}

crate::sql::impl_display_from_sql!(Block);

impl crate::sql::DisplaySql for Block {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match (self.len(), self.first()) {
			(0, _) => f.write_str("{}"),
			(1, Some(Entry::Value(v))) => {
				write!(f, "{{ {v} }}")
			}
			(l, _) => {
				f.write_char('{')?;
				if l > 1 {
					f.write_char('\n')?;
				} else if !is_pretty() {
					f.write_char(' ')?;
				}
				let indent = pretty_indent();
				if is_pretty() {
					write!(
						f,
						"{}",
						&Fmt::two_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f| write!(f, "{};", v))),
						)
					)?;
				} else {
					write!(
						f,
						"{}",
						&Fmt::one_line_separated(
							self.0.iter().map(|args| Fmt::new(args, |v, f| write!(f, "{};", v))),
						)
					)?;
				}
				drop(indent);
				if l > 1 {
					f.write_char('\n')?;
				} else if !is_pretty() {
					f.write_char(' ')?;
				}
				f.write_char('}')
			}
		}
	}
}

impl InfoStructure for Block {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Entry {
	Value(Value),
	Set(SetStatement),
	Ifelse(IfelseStatement),
	Select(SelectStatement),
	Create(CreateStatement),
	Update(UpdateStatement),
	Delete(DeleteStatement),
	Relate(RelateStatement),
	Insert(InsertStatement),
	Output(OutputStatement),
	Define(DefineStatement),
	Remove(RemoveStatement),
	Throw(ThrowStatement),
	Break(BreakStatement),
	Continue(ContinueStatement),
	Foreach(ForeachStatement),
	#[revision(start = 2)]
	Rebuild(RebuildStatement),
	#[revision(start = 3)]
	Upsert(UpsertStatement),
	#[revision(start = 4)]
	Alter(AlterStatement),
	#[revision(start = 5)]
	Info(InfoStatement),
}

impl PartialOrd for Entry {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl From<Entry> for crate::expr::Entry {
	fn from(v: Entry) -> Self {
		match v {
			Entry::Value(v) => Self::Value(v.into()),
			Entry::Set(v) => Self::Set(v.into()),
			Entry::Ifelse(v) => Self::Ifelse(v.into()),
			Entry::Select(v) => Self::Select(v.into()),
			Entry::Create(v) => Self::Create(v.into()),
			Entry::Update(v) => Self::Update(v.into()),
			Entry::Delete(v) => Self::Delete(v.into()),
			Entry::Relate(v) => Self::Relate(v.into()),
			Entry::Insert(v) => Self::Insert(v.into()),
			Entry::Output(v) => Self::Output(v.into()),
			Entry::Define(v) => Self::Define(v.into()),
			Entry::Remove(v) => Self::Remove(v.into()),
			Entry::Throw(v) => Self::Throw(v.into()),
			Entry::Break(v) => Self::Break(v.into()),
			Entry::Continue(v) => Self::Continue(v.into()),
			Entry::Foreach(v) => Self::Foreach(v.into()),
			Entry::Rebuild(v) => Self::Rebuild(v.into()),
			Entry::Upsert(v) => Self::Upsert(v.into()),
			Entry::Alter(v) => Self::Alter(v.into()),
			Entry::Info(v) => Self::Info(v.into()),
		}
	}
}

impl From<crate::expr::Entry> for Entry {
	fn from(v: crate::expr::Entry) -> Self {
		match v {
			crate::expr::Entry::Value(v) => Self::Value(v.into()),
			crate::expr::Entry::Set(v) => Self::Set(v.into()),
			crate::expr::Entry::Ifelse(v) => Self::Ifelse(v.into()),
			crate::expr::Entry::Select(v) => Self::Select(v.into()),
			crate::expr::Entry::Create(v) => Self::Create(v.into()),
			crate::expr::Entry::Update(v) => Self::Update(v.into()),
			crate::expr::Entry::Delete(v) => Self::Delete(v.into()),
			crate::expr::Entry::Relate(v) => Self::Relate(v.into()),
			crate::expr::Entry::Insert(v) => Self::Insert(v.into()),
			crate::expr::Entry::Output(v) => Self::Output(v.into()),
			crate::expr::Entry::Define(v) => Self::Define(v.into()),
			crate::expr::Entry::Remove(v) => Self::Remove(v.into()),
			crate::expr::Entry::Throw(v) => Self::Throw(v.into()),
			crate::expr::Entry::Break(v) => Self::Break(v.into()),
			crate::expr::Entry::Continue(v) => Self::Continue(v.into()),
			crate::expr::Entry::Foreach(v) => Self::Foreach(v.into()),
			crate::expr::Entry::Rebuild(v) => Self::Rebuild(v.into()),
			crate::expr::Entry::Upsert(v) => Self::Upsert(v.into()),
			crate::expr::Entry::Alter(v) => Self::Alter(v.into()),
			crate::expr::Entry::Info(v) => Self::Info(v.into()),
		}
	}
}

impl Entry {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		match self {
			Self::Set(v) => v.writeable(),
			Self::Value(v) => v.writeable(),
			Self::Ifelse(v) => v.writeable(),
			Self::Select(v) => v.writeable(),
			Self::Create(v) => v.writeable(),
			Self::Upsert(v) => v.writeable(),
			Self::Update(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
			Self::Output(v) => v.writeable(),
			Self::Define(v) => v.writeable(),
			Self::Rebuild(v) => v.writeable(),
			Self::Remove(v) => v.writeable(),
			Self::Throw(v) => v.writeable(),
			Self::Break(v) => v.writeable(),
			Self::Continue(v) => v.writeable(),
			Self::Foreach(v) => v.writeable(),
			Self::Alter(v) => v.writeable(),
			Self::Info(v) => v.writeable(),
		}
	}
}

crate::sql::impl_display_from_sql!(Entry);

impl crate::sql::DisplaySql for Entry {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Set(v) => write!(f, "{v}"),
			Self::Value(v) => Display::fmt(v, f),
			Self::Ifelse(v) => write!(f, "{v}"),
			Self::Select(v) => write!(f, "{v}"),
			Self::Create(v) => write!(f, "{v}"),
			Self::Upsert(v) => write!(f, "{v}"),
			Self::Update(v) => write!(f, "{v}"),
			Self::Delete(v) => write!(f, "{v}"),
			Self::Relate(v) => write!(f, "{v}"),
			Self::Insert(v) => write!(f, "{v}"),
			Self::Output(v) => write!(f, "{v}"),
			Self::Define(v) => write!(f, "{v}"),
			Self::Rebuild(v) => write!(f, "{v}"),
			Self::Remove(v) => write!(f, "{v}"),
			Self::Throw(v) => write!(f, "{v}"),
			Self::Break(v) => write!(f, "{v}"),
			Self::Continue(v) => write!(f, "{v}"),
			Self::Foreach(v) => write!(f, "{v}"),
			Self::Alter(v) => write!(f, "{v}"),
			Self::Info(v) => write!(f, "{v}"),
		}
	}
}
