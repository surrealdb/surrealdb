use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fmt::{is_pretty, pretty_indent, Fmt, Pretty};
use crate::sql::statements::{
	BreakStatement, ContinueStatement, CreateStatement, DefineStatement, DeleteStatement,
	ForeachStatement, IfelseStatement, InsertStatement, OutputStatement, RelateStatement,
	RemoveStatement, SelectStatement, SetStatement, ThrowStatement, UpdateStatement,
};
use crate::sql::value::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::Deref;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Block";

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Block")]
#[revisioned(revision = 1)]
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

impl Block {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.iter().any(Entry::writeable)
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Duplicate context
		let mut ctx = Context::new(ctx);
		// Loop over the statements
		for (i, v) in self.iter().enumerate() {
			match v {
				Entry::Set(v) => {
					let val = v.compute(&ctx, opt, txn, doc).await?;
					ctx.add_value(v.name.to_owned(), val);
				}
				Entry::Throw(v) => {
					// Always errors immediately
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Break(v) => {
					// Always errors immediately
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Continue(v) => {
					// Always errors immediately
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Foreach(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Ifelse(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Select(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Create(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Update(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Delete(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Relate(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Insert(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Define(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Remove(v) => {
					v.compute(&ctx, opt, txn, doc).await?;
				}
				Entry::Output(v) => {
					// Return the RETURN value
					return v.compute(&ctx, opt, txn, doc).await;
				}
				Entry::Value(v) => {
					if i == self.len() - 1 {
						// If the last entry then return the value
						return v.compute(&ctx, opt, txn, doc).await;
					} else {
						// Otherwise just process the value
						v.compute(&ctx, opt, txn, doc).await?;
					}
				}
			}
		}
		// Return nothing
		Ok(Value::None)
	}
}

impl Display for Block {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[revisioned(revision = 1)]
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
}

impl PartialOrd for Entry {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
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
			Self::Update(v) => v.writeable(),
			Self::Delete(v) => v.writeable(),
			Self::Relate(v) => v.writeable(),
			Self::Insert(v) => v.writeable(),
			Self::Output(v) => v.writeable(),
			Self::Define(v) => v.writeable(),
			Self::Remove(v) => v.writeable(),
			Self::Throw(v) => v.writeable(),
			Self::Break(v) => v.writeable(),
			Self::Continue(v) => v.writeable(),
			Self::Foreach(v) => v.writeable(),
		}
	}
}

impl Display for Entry {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Set(v) => write!(f, "{v}"),
			Self::Value(v) => Display::fmt(v, f),
			Self::Ifelse(v) => write!(f, "{v}"),
			Self::Select(v) => write!(f, "{v}"),
			Self::Create(v) => write!(f, "{v}"),
			Self::Update(v) => write!(f, "{v}"),
			Self::Delete(v) => write!(f, "{v}"),
			Self::Relate(v) => write!(f, "{v}"),
			Self::Insert(v) => write!(f, "{v}"),
			Self::Output(v) => write!(f, "{v}"),
			Self::Define(v) => write!(f, "{v}"),
			Self::Remove(v) => write!(f, "{v}"),
			Self::Throw(v) => write!(f, "{v}"),
			Self::Break(v) => write!(f, "{v}"),
			Self::Continue(v) => write!(f, "{v}"),
			Self::Foreach(v) => write!(f, "{v}"),
		}
	}
}
