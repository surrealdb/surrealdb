#[non_exhaustive]
pub enum Method {
	Unknown,
	Ping,
	Info,
	Use,
	Signup,
	Signin,
	Invalidate,
	Authenticate,
	Kill,
	Live,
	Set,
	Unset,
	Select,
	Insert,
	Create,
	Update,
	Merge,
	Patch,
	Delete,
	Version,
	Query,
	Relate,
	Run,
}

impl Method {
	pub fn parse<S>(s: S) -> Self
	where
		S: AsRef<str>,
	{
		match s.as_ref().to_lowercase().as_str() {
			"ping" => Self::Ping,
			"info" => Self::Info,
			"use" => Self::Use,
			"signup" => Self::Signup,
			"signin" => Self::Signin,
			"invalidate" => Self::Invalidate,
			"authenticate" => Self::Authenticate,
			"kill" => Self::Kill,
			"live" => Self::Live,
			"let" | "set" => Self::Set,
			"unset" => Self::Unset,
			"select" => Self::Select,
			"insert" => Self::Insert,
			"create" => Self::Create,
			"update" => Self::Update,
			"merge" => Self::Merge,
			"patch" => Self::Patch,
			"delete" => Self::Delete,
			"version" => Self::Version,
			"query" => Self::Query,
			"relate" => Self::Relate,
			"run" => Self::Run,
			_ => Self::Unknown,
		}
	}
}

impl Method {
	pub fn to_str(&self) -> &str {
		match self {
			Self::Unknown => "unknown",
			Self::Ping => "ping",
			Self::Info => "info",
			Self::Use => "use",
			Self::Signup => "signup",
			Self::Signin => "signin",
			Self::Invalidate => "invalidate",
			Self::Authenticate => "authenticate",
			Self::Kill => "kill",
			Self::Live => "live",
			Self::Set => "set",
			Self::Unset => "unset",
			Self::Select => "select",
			Self::Insert => "insert",
			Self::Create => "create",
			Self::Update => "update",
			Self::Merge => "merge",
			Self::Patch => "patch",
			Self::Delete => "delete",
			Self::Version => "version",
			Self::Query => "query",
			Self::Relate => "relate",
			Self::Run => "run",
		}
	}
}

impl Method {
	pub fn is_valid(&self) -> bool {
		!matches!(self, Self::Unknown)
	}

	pub fn needs_mut(&self) -> bool {
		!self.can_be_immut()
	}

	// should be the same as execute_immut
	pub fn can_be_immut(&self) -> bool {
		matches!(
			self,
			Method::Ping
				| Method::Info | Method::Select
				| Method::Insert | Method::Create
				| Method::Update | Method::Merge
				| Method::Patch | Method::Delete
				| Method::Version
				| Method::Query | Method::Relate
				| Method::Run | Method::Unknown
		)
	}
}
