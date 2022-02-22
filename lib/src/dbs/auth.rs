#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Level {
	No,
	Kv,
	Ns,
	Db,
	Sc,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Auth {
	No,
	Kv,
	Ns(String),
	Db(String, String),
	Sc(String, String, String),
}

impl Default for Auth {
	fn default() -> Self {
		Auth::No
	}
}

impl Auth {
	pub fn check(&self, level: Level) -> bool {
		match self {
			Auth::No => match level {
				Level::No => true,
				_ => false,
			},
			Auth::Kv => match level {
				Level::No => true,
				Level::Kv => true,
				_ => false,
			},
			Auth::Ns(_) => match level {
				Level::No => true,
				Level::Kv => true,
				Level::Ns => true,
				_ => false,
			},
			Auth::Db(_, _) => match level {
				Level::No => true,
				Level::Kv => true,
				Level::Ns => true,
				Level::Db => true,
				_ => false,
			},
			Auth::Sc(_, _, _) => match level {
				Level::No => true,
				Level::Kv => true,
				Level::Ns => true,
				Level::Db => true,
				Level::Sc => true,
			},
		}
	}
}
