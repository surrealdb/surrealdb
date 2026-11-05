// TODO(sgirones): For now keep it simple. In the future, we will allow for
// custom roles and policies using a more exhaustive list of actions and
// resources.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd)]
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
