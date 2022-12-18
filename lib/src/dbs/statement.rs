use crate::sql::cond::Cond;
use crate::sql::data::Data;
use crate::sql::fetch::Fetchs;
use crate::sql::field::Fields;
use crate::sql::group::Groups;
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
use crate::sql::output::Output;
use crate::sql::split::Splits;
use crate::sql::start::Start;
use crate::sql::statements::create::CreateStatement;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::insert::InsertStatement;
use crate::sql::statements::relate::RelateStatement;
use crate::sql::statements::select::SelectStatement;
use crate::sql::statements::update::UpdateStatement;
use crate::sql::version::Version;
use std::fmt;

#[derive(Clone, Debug)]
pub enum Statement<'a> {
	Select(&'a SelectStatement),
	Create(&'a CreateStatement),
	Update(&'a UpdateStatement),
	Relate(&'a RelateStatement),
	Delete(&'a DeleteStatement),
	Insert(&'a InsertStatement),
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

impl<'a> fmt::Display for Statement<'a> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Statement::Select(v) => write!(f, "{}", v),
			Statement::Create(v) => write!(f, "{}", v),
			Statement::Update(v) => write!(f, "{}", v),
			Statement::Relate(v) => write!(f, "{}", v),
			Statement::Delete(v) => write!(f, "{}", v),
			Statement::Insert(v) => write!(f, "{}", v),
		}
	}
}

impl<'a> Statement<'a> {
	/// Check the type of statement
	#[inline]
	pub fn is_select(&self) -> bool {
		matches!(self, Statement::Select(_))
	}
	/// Check the type of statement
	#[inline]
	pub fn is_delete(&self) -> bool {
		matches!(self, Statement::Delete(_))
	}
	/// Returns any query fields if specified
	#[inline]
	pub fn expr(&self) -> Option<&Fields> {
		match self {
			Statement::Select(v) => Some(&v.expr),
			_ => None,
		}
	}
	/// Returns any SET clause if specified
	#[inline]
	pub fn data(&self) -> Option<&Data> {
		match self {
			Statement::Create(v) => v.data.as_ref(),
			Statement::Update(v) => v.data.as_ref(),
			Statement::Relate(v) => v.data.as_ref(),
			Statement::Insert(v) => v.update.as_ref(),
			_ => None,
		}
	}
	/// Returns any WHERE clause if specified
	#[inline]
	pub fn conds(&self) -> Option<&Cond> {
		match self {
			Statement::Select(v) => v.cond.as_ref(),
			Statement::Update(v) => v.cond.as_ref(),
			Statement::Delete(v) => v.cond.as_ref(),
			_ => None,
		}
	}
	/// Returns any SPLIT clause if specified
	#[inline]
	pub fn split(&self) -> Option<&Splits> {
		match self {
			Statement::Select(v) => v.split.as_ref(),
			_ => None,
		}
	}
	/// Returns any GROUP clause if specified
	#[inline]
	pub fn group(&self) -> Option<&Groups> {
		match self {
			Statement::Select(v) => v.group.as_ref(),
			_ => None,
		}
	}
	/// Returns any ORDER clause if specified
	#[inline]
	pub fn order(&self) -> Option<&Orders> {
		match self {
			Statement::Select(v) => v.order.as_ref(),
			_ => None,
		}
	}
	/// Returns any FETCH clause if specified
	#[inline]
	pub fn fetch(&self) -> Option<&Fetchs> {
		match self {
			Statement::Select(v) => v.fetch.as_ref(),
			_ => None,
		}
	}
	/// Returns any START clause if specified
	#[inline]
	pub fn start(&self) -> Option<&Start> {
		match self {
			Statement::Select(v) => v.start.as_ref(),
			_ => None,
		}
	}
	/// Returns any LIMIT clause if specified
	#[inline]
	pub fn limit(&self) -> Option<&Limit> {
		match self {
			Statement::Select(v) => v.limit.as_ref(),
			_ => None,
		}
	}
	/// Returns any VERSION clause if specified
	#[inline]
	pub fn version(&self) -> Option<&Version> {
		match self {
			Statement::Select(v) => v.version.as_ref(),
			_ => None,
		}
	}
	/// Returns any RETURN clause if specified
	#[inline]
	pub fn output(&self) -> Option<&Output> {
		match self {
			Statement::Create(v) => v.output.as_ref(),
			Statement::Update(v) => v.output.as_ref(),
			Statement::Relate(v) => v.output.as_ref(),
			Statement::Delete(v) => v.output.as_ref(),
			Statement::Insert(v) => v.output.as_ref(),
			_ => None,
		}
	}
	/// Returns any RETURN clause if specified
	#[inline]
	pub fn parallel(&self) -> bool {
		match self {
			Statement::Select(v) => v.parallel,
			Statement::Create(v) => v.parallel,
			Statement::Update(v) => v.parallel,
			Statement::Relate(v) => v.parallel,
			Statement::Delete(v) => v.parallel,
			Statement::Insert(v) => v.parallel,
		}
	}
}
