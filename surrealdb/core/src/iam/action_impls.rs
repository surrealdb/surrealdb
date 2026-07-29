use crate::catalog::PermissionKind;
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

impl std::convert::From<PermissionKind> for Action {
	fn from(kind: PermissionKind) -> Self {
		match kind {
			PermissionKind::Select => Action::View,
			PermissionKind::Create => Action::Edit,
			PermissionKind::Update => Action::Edit,
			PermissionKind::Delete => Action::Edit,
		}
	}
}
