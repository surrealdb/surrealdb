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
			Auth::No => matches!(level, Level::No),
			Auth::Kv => matches!(level, Level::No | Level::Kv),
			Auth::Ns(_) => matches!(level, Level::No | Level::Kv | Level::Ns),
			Auth::Db(_, _) => matches!(level, Level::No | Level::Kv | Level::Ns | Level::Db),
			Auth::Sc(_, _, _) => true,
		}
	}
}
