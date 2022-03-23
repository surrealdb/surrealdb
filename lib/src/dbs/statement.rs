use crate::sql::cond::Cond;
use crate::sql::fetch::Fetchs;
use crate::sql::group::Groups;
use crate::sql::limit::Limit;
use crate::sql::order::Orders;
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
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum Statement {
	None,
	Select(Arc<SelectStatement>),
	Create(Arc<CreateStatement>),
	Update(Arc<UpdateStatement>),
	Relate(Arc<RelateStatement>),
	Delete(Arc<DeleteStatement>),
	Insert(Arc<InsertStatement>),
}

impl Default for Statement {
	fn default() -> Self {
		Statement::None
	}
}

impl From<Arc<SelectStatement>> for Statement {
	fn from(v: Arc<SelectStatement>) -> Self {
		Statement::Select(v)
	}
}

impl From<Arc<CreateStatement>> for Statement {
	fn from(v: Arc<CreateStatement>) -> Self {
		Statement::Create(v)
	}
}

impl From<Arc<UpdateStatement>> for Statement {
	fn from(v: Arc<UpdateStatement>) -> Self {
		Statement::Update(v)
	}
}

impl From<Arc<RelateStatement>> for Statement {
	fn from(v: Arc<RelateStatement>) -> Self {
		Statement::Relate(v)
	}
}

impl From<Arc<DeleteStatement>> for Statement {
	fn from(v: Arc<DeleteStatement>) -> Self {
		Statement::Delete(v)
	}
}

impl From<Arc<InsertStatement>> for Statement {
	fn from(v: Arc<InsertStatement>) -> Self {
		Statement::Insert(v)
	}
}

impl fmt::Display for Statement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Statement::Select(v) => write!(f, "{}", v),
			Statement::Create(v) => write!(f, "{}", v),
			Statement::Update(v) => write!(f, "{}", v),
			Statement::Relate(v) => write!(f, "{}", v),
			Statement::Delete(v) => write!(f, "{}", v),
			Statement::Insert(v) => write!(f, "{}", v),
			_ => unreachable!(),
		}
	}
}

impl Statement {
	// Returns any SPLIT clause if specified
	pub fn conds(self: &Statement) -> Option<&Cond> {
		match self {
			Statement::Select(v) => v.cond.as_ref(),
			Statement::Update(v) => v.cond.as_ref(),
			Statement::Delete(v) => v.cond.as_ref(),
			_ => None,
		}
	}
	// Returns any SPLIT clause if specified
	pub fn split(self: &Statement) -> Option<&Splits> {
		match self {
			Statement::Select(v) => v.split.as_ref(),
			_ => None,
		}
	}
	// Returns any GROUP clause if specified
	pub fn group(self: &Statement) -> Option<&Groups> {
		match self {
			Statement::Select(v) => v.group.as_ref(),
			_ => None,
		}
	}
	// Returns any ORDER clause if specified
	pub fn order(self: &Statement) -> Option<&Orders> {
		match self {
			Statement::Select(v) => v.order.as_ref(),
			_ => None,
		}
	}
	// Returns any FETCH clause if specified
	pub fn fetch(self: &Statement) -> Option<&Fetchs> {
		match self {
			Statement::Select(v) => v.fetch.as_ref(),
			_ => None,
		}
	}
	// Returns any START clause if specified
	pub fn start(self: &Statement) -> Option<&Start> {
		match self {
			Statement::Select(v) => v.start.as_ref(),
			_ => None,
		}
	}
	// Returns any LIMIT clause if specified
	pub fn limit(self: &Statement) -> Option<&Limit> {
		match self {
			Statement::Select(v) => v.limit.as_ref(),
			_ => None,
		}
	}
	// Returns any VERSION clause if specified
	pub fn version(self: &Statement) -> Option<&Version> {
		match self {
			Statement::Select(v) => v.version.as_ref(),
			_ => None,
		}
	}
}
