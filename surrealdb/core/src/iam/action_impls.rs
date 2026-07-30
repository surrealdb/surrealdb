//! Conversions from engine statements to IAM actions.
//!
//! `Statement` is engine-local while `Action` belongs to the iam leaf; the
//! local type in the parameters keeps this impl coherent here.

use crate::dbs::Statement;
use crate::iam::Action;

impl From<&Statement<'_>> for Action {
	fn from(stmt: &Statement) -> Self {
		match stmt {
			Statement::Live(_) => Action::View,
			Statement::Select {
				..
			} => Action::View,
			Statement::Show(_) => Action::View,
			Statement::Create(_) => Action::Edit,
			Statement::Upsert(_) => Action::Edit,
			Statement::Update(_) => Action::Edit,
			Statement::Relate(_) => Action::Edit,
			Statement::Delete(_) => Action::Edit,
			Statement::Insert(_) => Action::Edit,
			Statement::Access(_) => Action::Edit,
		}
	}
}
