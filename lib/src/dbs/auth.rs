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
	pub fn perms(&self) -> bool {
		match self {
			Auth::No => true,
			Auth::Sc(_, _, _) => true,
			Auth::Db(_, _) => false,
			Auth::Ns(_) => false,
			Auth::Kv => false,
		}
	}
	pub fn check(&self, level: Level) -> bool {
		match self {
			Auth::No => matches!(level, Level::No),
			Auth::Sc(_, _, _) => matches!(level, Level::No | Level::Sc),
			Auth::Db(_, _) => matches!(level, Level::No | Level::Sc | Level::Db),
			Auth::Ns(_) => matches!(level, Level::No | Level::Sc | Level::Db | Level::Ns),
			Auth::Kv => true,
		}
	}
}
