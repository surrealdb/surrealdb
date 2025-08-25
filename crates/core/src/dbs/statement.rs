use std::borrow::Cow;
use std::fmt;

use anyhow::Result;

use crate::catalog::{Permission, TableDefinition};
use crate::ctx::{Context, MutableContext};
use crate::err::Error;
use crate::expr::cond::Cond;
use crate::expr::data::Data;
use crate::expr::fetch::Fetchs;
use crate::expr::field::Fields;
use crate::expr::group::Groups;
use crate::expr::idiom::Idioms;
use crate::expr::limit::Limit;
use crate::expr::order::Ordering;
use crate::expr::output::Output;
use crate::expr::split::Splits;
use crate::expr::start::Start;
use crate::expr::statements::access::AccessStatement;
use crate::expr::statements::create::CreateStatement;
use crate::expr::statements::delete::DeleteStatement;
use crate::expr::statements::insert::InsertStatement;
use crate::expr::statements::live::LiveStatement;
use crate::expr::statements::relate::RelateStatement;
use crate::expr::statements::select::SelectStatement;
use crate::expr::statements::show::ShowStatement;
use crate::expr::statements::update::UpdateStatement;
use crate::expr::statements::upsert::UpsertStatement;
use crate::expr::{Explain, Timeout, With};
use crate::idx::planner::QueryPlanner;

#[derive(Clone, Debug)]
pub(crate) enum Statement<'a> {
	Live(&'a LiveStatement),
	Show(&'a ShowStatement),
	Select(&'a SelectStatement),
	Create(&'a CreateStatement),
	Upsert(&'a UpsertStatement),
	Update(&'a UpdateStatement),
	Relate(&'a RelateStatement),
	Delete(&'a DeleteStatement),
	Insert(&'a InsertStatement),
	Access(&'a AccessStatement),
}

impl<'a> From<&'a LiveStatement> for Statement<'a> {
	fn from(v: &'a LiveStatement) -> Self {
		Statement::Live(v)
	}
}

impl<'a> From<&'a ShowStatement> for Statement<'a> {
	fn from(v: &'a ShowStatement) -> Self {
		Statement::Show(v)
	}
}

impl<'a> From<&'a SelectStatement> for Statement<'a> {
	fn from(v: &'a SelectStatement) -> Self {
		Statement::Select(v)
	}
}

impl<'a> From<&'a CreateStatement> for Statement<'a> {
	fn from(v: &'a CreateStatement) -> Self {
		Statement::Create(v)
	}
}

impl<'a> From<&'a UpsertStatement> for Statement<'a> {
	fn from(v: &'a UpsertStatement) -> Self {
		Statement::Upsert(v)
	}
}

impl<'a> From<&'a UpdateStatement> for Statement<'a> {
	fn from(v: &'a UpdateStatement) -> Self {
		Statement::Update(v)
	}
}

impl<'a> From<&'a RelateStatement> for Statement<'a> {
	fn from(v: &'a RelateStatement) -> Self {
		Statement::Relate(v)
	}
}

impl<'a> From<&'a DeleteStatement> for Statement<'a> {
	fn from(v: &'a DeleteStatement) -> Self {
		Statement::Delete(v)
	}
}

impl<'a> From<&'a InsertStatement> for Statement<'a> {
	fn from(v: &'a InsertStatement) -> Self {
		Statement::Insert(v)
	}
}

impl<'a> From<&'a AccessStatement> for Statement<'a> {
	fn from(v: &'a AccessStatement) -> Self {
		Statement::Access(v)
	}
}

impl fmt::Display for Statement<'_> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Statement::Live(v) => write!(f, "{v}"),
			Statement::Show(v) => write!(f, "{v}"),
			Statement::Select(v) => write!(f, "{v}"),
			Statement::Create(v) => write!(f, "{v}"),
			Statement::Upsert(v) => write!(f, "{v}"),
			Statement::Update(v) => write!(f, "{v}"),
			Statement::Relate(v) => write!(f, "{v}"),
			Statement::Delete(v) => write!(f, "{v}"),
			Statement::Insert(v) => write!(f, "{v}"),
			Statement::Access(v) => write!(f, "{v}"),
		}
	}
}

impl Statement<'_> {
	/// Check if this is a SELECT statement
	pub(crate) fn is_select(&self) -> bool {
		matches!(self, Statement::Select(_))
	}

	/// Check if this is a CREATE statement
	pub(crate) fn is_create(&self) -> bool {
		matches!(self, Statement::Create(_))
	}

	/// Check if this is a DELETE statement
	pub(crate) fn is_delete(&self) -> bool {
		matches!(self, Statement::Delete(_))
	}

	/// Returns whether the document retrieval for
	/// this statement can be deferred. This is used
	/// in the following instances:
	///
	/// CREATE some;
	/// CREATE some:thing;
	/// CREATE |some:1000|;
	/// CREATE |some:1..1000|;
	/// CREATE { id: some:thing };
	/// UPSERT some;
	/// UPSERT some:thing;
	/// UPSERT |some:1000|;
	/// UPSERT |some:1..1000|;
	/// UPSERT { id: some:thing };
	///
	/// Importantly, when a WHERE clause condition is
	/// specified on an UPSERT clause, then we do
	/// first retrieve the document from storage, and
	/// this function will return false in the
	/// following instances:
	///
	/// UPSERT some WHERE test = true;
	/// UPSERT some:thing WHERE test = true;
	/// UPSERT |some:1000| WHERE test = true;
	/// UPSERT |some:1..1000| WHERE test = true;
	/// UPSERT { id: some:thing } WHERE test = true;
	pub(crate) fn is_deferable(&self) -> bool {
		match self {
			Statement::Upsert(v) if v.cond.is_none() => true,
			Statement::Create(_) => true,
			_ => false,
		}
	}

	/// Returns whether the document retrieval for
	/// this statement potentially depends on the
	/// initial value for this document, and can
	/// therefore be retried as an update. This will
	/// be true in the following instances:
	///
	/// UPSERT some UNSET test;
	/// UPSERT some SET test = true;
	/// UPSERT some MERGE { test: true };
	/// UPSERT some PATCH [{ op: 'replace', path: '/', value: { test: true } }];
	/// UPSERT some:thing UNSET test;
	/// UPSERT some:thing SET test = true;
	/// UPSERT some:thing MERGE { test: true };
	/// UPSERT some:thing PATCH [{ op: 'replace', path: '/', value: { test: true
	/// } }]; UPSERT |some:1000| UNSET test;
	/// UPSERT |some:1000| SET test = true;
	/// UPSERT |some:1000| MERGE { test: true };
	/// UPSERT |some:1000| PATCH [{ op: 'replace', path: '/', value: { test:
	/// true } }]; UPSERT |some:1..1000| UNSET test;
	/// UPSERT |some:1..1000| SET test = true;
	/// UPSERT |some:1..1000| MERGE { test: true };
	/// UPSERT |some:1..1000| PATCH [{ op: 'replace', path: '/', value: { test:
	/// true } }];
	///
	/// Importantly, when a WHERE clause condition is
	/// specified on an UPSERT clause, then we do
	/// first retrieve the document from storage, and
	/// this function will return false in the
	/// following instances:
	///
	/// UPSERT some WHERE test = true;
	/// UPSERT some:thing WHERE test = true;
	/// UPSERT |some:1000| WHERE test = true;
	/// UPSERT |some:1..1000| WHERE test = true;
	/// UPSERT { id: some:thing } WHERE test = true;
	pub(crate) fn is_repeatable(&self) -> bool {
		match self {
			Statement::Upsert(v) if v.cond.is_none() => match v.data {
				// We are setting the entire record content
				// so there is no need to fetch the value
				// from the storage engine, if it exists.
				Some(Data::ContentExpression(_)) => false,
				// We are setting the entire record content
				// so there is no need to fetch the value
				// from the storage engine, if it exists.
				Some(Data::ReplaceExpression(_)) => false,
				// We likely have a MERGE or SET clause on
				// this UPSERT statement, and so we might
				// potentially need to access fields from
				// the initial value already existing in
				// the database. Therefore we need to fetch
				// the initial value from storage.
				Some(_) => true,
				// We have no data clause, so we don't need
				// to check if the record exists initially.
				None => false,
			},
			_ => false,
		}
	}

	/// Returns whether the document retrieval for
	/// this statement should attempt to loop over
	/// existing document to update, or is guaranteed
	/// to create a record, if none exists. This is
	/// used in the following instances when the WHERE
	/// clause does not find any matching documents in
	/// the storage engine, and therefore a new record
	/// must be upserted:
	///
	/// UPSERT some WHERE test = true;
	pub(crate) fn is_guaranteed(&self) -> bool {
		matches!(self, Statement::Upsert(v) if v.cond.is_some())
	}

	/// Returns any query fields if specified
	pub(crate) fn expr(&self) -> Option<&Fields> {
		match self {
			Statement::Select(v) => Some(&v.expr),
			Statement::Live(v) => Some(&v.fields),
			_ => None,
		}
	}

	/// Returns any OMIT clause if specified
	pub(crate) fn omit(&self) -> Option<&Idioms> {
		match self {
			Statement::Select(v) => v.omit.as_ref(),
			_ => None,
		}
	}

	/// Returns any SET, CONTENT, or MERGE clause if specified
	pub(crate) fn data(&self) -> Option<&Data> {
		match self {
			Statement::Create(v) => v.data.as_ref(),
			Statement::Upsert(v) => v.data.as_ref(),
			Statement::Update(v) => v.data.as_ref(),
			Statement::Relate(v) => v.data.as_ref(),
			Statement::Insert(v) => v.update.as_ref(),
			_ => None,
		}
	}

	/// Returns any WHERE clause if specified
	pub(crate) fn cond(&self) -> Option<&Cond> {
		match self {
			Statement::Live(v) => v.cond.as_ref(),
			Statement::Select(v) => v.cond.as_ref(),
			Statement::Upsert(v) => v.cond.as_ref(),
			Statement::Update(v) => v.cond.as_ref(),
			Statement::Delete(v) => v.cond.as_ref(),
			_ => None,
		}
	}

	/// Returns any SPLIT clause if specified
	pub(crate) fn split(&self) -> Option<&Splits> {
		match self {
			Statement::Select(v) => v.split.as_ref(),
			_ => None,
		}
	}

	/// Returns any GROUP clause if specified
	pub(crate) fn group(&self) -> Option<&Groups> {
		match self {
			Statement::Select(v) => v.group.as_ref(),
			_ => None,
		}
	}

	/// Returns any ORDER clause if specified
	pub(crate) fn order(&self) -> Option<&Ordering> {
		match self {
			Statement::Select(v) => v.order.as_ref(),
			_ => None,
		}
	}

	/// Returns any WITH clause if specified
	pub(crate) fn with(&self) -> Option<&With> {
		match self {
			Statement::Select(s) => s.with.as_ref(),
			Statement::Update(s) => s.with.as_ref(),
			Statement::Upsert(s) => s.with.as_ref(),
			Statement::Delete(s) => s.with.as_ref(),
			_ => None,
		}
	}

	/// Returns any FETCH clause if specified
	pub(crate) fn fetch(&self) -> Option<&Fetchs> {
		match self {
			Statement::Select(v) => v.fetch.as_ref(),
			_ => None,
		}
	}

	/// Returns any START clause if specified
	pub(crate) fn start(&self) -> Option<&Start> {
		match self {
			Statement::Select(v) => v.start.as_ref(),
			_ => None,
		}
	}

	/// Returns any LIMIT clause if specified
	pub(crate) fn limit(&self) -> Option<&Limit> {
		match self {
			Statement::Select(v) => v.limit.as_ref(),
			_ => None,
		}
	}

	pub(crate) fn is_only(&self) -> bool {
		match self {
			Statement::Create(v) => v.only,
			Statement::Delete(v) => v.only,
			Statement::Relate(v) => v.only,
			Statement::Select(v) => v.only,
			Statement::Upsert(v) => v.only,
			Statement::Update(v) => v.only,
			_ => false,
		}
	}

	/// Returns any RETURN clause if specified
	pub(crate) fn output(&self) -> Option<&Output> {
		match self {
			Statement::Create(v) => v.output.as_ref(),
			Statement::Upsert(v) => v.output.as_ref(),
			Statement::Update(v) => v.output.as_ref(),
			Statement::Relate(v) => v.output.as_ref(),
			Statement::Delete(v) => v.output.as_ref(),
			Statement::Insert(v) => v.output.as_ref(),
			_ => None,
		}
	}

	/// Returns any TEMPFILES clause if specified
	#[cfg(storage)]
	pub(crate) fn tempfiles(&self) -> bool {
		match self {
			Statement::Select(v) => v.tempfiles,
			_ => false,
		}
	}

	/// Returns any EXPLAIN clause if specified
	pub(crate) fn explain(&self) -> Option<&Explain> {
		match self {
			Statement::Select(s) => s.explain.as_ref(),
			Statement::Update(s) => s.explain.as_ref(),
			Statement::Upsert(s) => s.explain.as_ref(),
			Statement::Delete(s) => s.explain.as_ref(),
			_ => None,
		}
	}

	/// Returns a reference to the appropriate `Permission` field within the
	/// `TableDefinition` structure based on the type of the statement.
	pub(crate) fn permissions<'b>(
		&self,
		table: &'b TableDefinition,
		doc_is_new: bool,
	) -> &'b Permission {
		if self.is_delete() {
			&table.permissions.delete
		} else if self.is_select() {
			&table.permissions.select
		} else if doc_is_new {
			&table.permissions.create
		} else {
			&table.permissions.update
		}
	}

	pub(crate) fn timeout(&self) -> Option<&Timeout> {
		match self {
			Statement::Create(s) => s.timeout.as_ref(),
			Statement::Delete(s) => s.timeout.as_ref(),
			Statement::Insert(s) => s.timeout.as_ref(),
			Statement::Select(s) => s.timeout.as_ref(),
			Statement::Update(s) => s.timeout.as_ref(),
			Statement::Upsert(s) => s.timeout.as_ref(),
			_ => None,
		}
	}
	pub(crate) fn setup_timeout<'a>(&self, ctx: &'a Context) -> Result<Cow<'a, Context>, Error> {
		if let Some(t) = self.timeout() {
			let mut ctx = MutableContext::new(ctx);
			ctx.add_timeout(*t.0)?;
			Ok(Cow::Owned(ctx.freeze()))
		} else {
			Ok(Cow::Borrowed(ctx))
		}
	}

	pub(crate) fn setup_query_planner<'a>(
		&self,
		planner: QueryPlanner,
		ctx: Cow<'a, Context>,
	) -> Cow<'a, Context> {
		// Add query executors if any
		if planner.has_executors() {
			// Create a new context
			let mut ctx = MutableContext::new(&ctx);
			ctx.set_query_planner(planner);
			Cow::Owned(ctx.freeze())
		} else {
			ctx
		}
	}
}
