use crate::catalog::PermissionKind;
use crate::dbs::Statement;

// TODO(sgirones): For now keep it simple. In the future, we will allow for
// custom roles and policies using a more exhaustive list of actions and
// resources.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Action {
	View,
	Edit,
}

impl std::fmt::Display for Action {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Action::View => write!(f, "View"),
			Action::Edit => write!(f, "Edit"),
		}
	}
}

impl Action {
	pub fn id(&self) -> String {
		self.to_string()
	}
}

impl From<&Statement<'_>> for Action {
	fn from(stmt: &Statement) -> Self {
		match stmt {
			Statement::Live(_) => Action::View,
			Statement::Select(_) => Action::View,
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
