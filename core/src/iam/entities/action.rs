use std::str::FromStr;

use cedar_policy::{Entity, EntityId, EntityTypeName, EntityUid};

use crate::dbs::Statement;

// TODO(sgirones): For now keep it simple. In the future, we will allow for custom roles and policies using a more exhaustive list of actions and resources.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[non_exhaustive]
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

impl std::convert::From<&Action> for EntityUid {
	fn from(action: &Action) -> Self {
		EntityUid::from_type_name_and_id(
			EntityTypeName::from_str("Action").unwrap(),
			EntityId::from_str(&action.id()).unwrap(),
		)
	}
}

impl std::convert::From<&Action> for Entity {
	fn from(action: &Action) -> Self {
		Entity::new(action.into(), Default::default(), Default::default())
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
