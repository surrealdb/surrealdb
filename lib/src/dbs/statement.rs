use crate::sql::statements::create::CreateStatement;
use crate::sql::statements::delete::DeleteStatement;
use crate::sql::statements::insert::InsertStatement;
use crate::sql::statements::relate::RelateStatement;
use crate::sql::statements::select::SelectStatement;
use crate::sql::statements::update::UpdateStatement;
use std::fmt;

#[derive(Debug)]
pub enum Statement<'a> {
	None,
	Select(&'a SelectStatement),
	Create(&'a CreateStatement),
	Update(&'a UpdateStatement),
	Relate(&'a RelateStatement),
	Delete(&'a DeleteStatement),
	Insert(&'a InsertStatement),
}

impl Default for Statement<'_> {
	fn default() -> Self {
		Statement::None
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
			_ => unreachable!(),
		}
	}
}
