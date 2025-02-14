#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum Method {
	Unknown,
	Ping,
	Info,
	Use,
	Signup,
	Signin,
	Authenticate,
	Invalidate,
	Reset,
	Kill,
	Live,
	Set,
	Unset,
	Select,
	Insert,
	Create,
	Upsert,
	Update,
	Merge,
	Patch,
	Delete,
	Version,
	Query,
	Relate,
	Run,
	GraphQL,
	InsertRelation,
}

impl Method {
	/// Parse a [Method] from a [str] with any case
	pub fn parse_case_insensitive<S>(s: S) -> Self
	where
		S: AsRef<str>,
	{
		Self::parse(s.as_ref().to_ascii_lowercase().as_str())
	}

	/// Parse a [Method] from a [str] in lower case
	pub fn parse_case_sensitive<S>(s: S) -> Self
	where
		S: AsRef<str>,
	{
		Self::parse(s.as_ref())
	}

	/// Parse a [Method] from a [str]
	fn parse<S>(s: S) -> Self
	where
		S: AsRef<str>,
	{
		match s.as_ref() {
			"ping" => Self::Ping,
			"info" => Self::Info,
			"use" => Self::Use,
			"signup" => Self::Signup,
			"signin" => Self::Signin,
			"authenticate" => Self::Authenticate,
			"invalidate" => Self::Invalidate,
			"reset" => Self::Reset,
			"kill" => Self::Kill,
			"live" => Self::Live,
			"set" | "let" => Self::Set,
			"unset" => Self::Unset,
			"select" => Self::Select,
			"insert" => Self::Insert,
			"create" => Self::Create,
			"upsert" => Self::Upsert,
			"update" => Self::Update,
			"merge" => Self::Merge,
			"patch" => Self::Patch,
			"delete" => Self::Delete,
			"version" => Self::Version,
			"query" => Self::Query,
			"relate" => Self::Relate,
			"run" => Self::Run,
			"graphql" => Self::GraphQL,
			"insert_relation" => Self::InsertRelation,
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
			Self::Authenticate => "authenticate",
			Self::Invalidate => "invalidate",
			Self::Reset => "reset",
			Self::Kill => "kill",
			Self::Live => "live",
			Self::Set => "set",
			Self::Unset => "unset",
			Self::Select => "select",
			Self::Insert => "insert",
			Self::Create => "create",
			Self::Upsert => "upsert",
			Self::Update => "update",
			Self::Merge => "merge",
			Self::Patch => "patch",
			Self::Delete => "delete",
			Self::Version => "version",
			Self::Query => "query",
			Self::Relate => "relate",
			Self::Run => "run",
			Self::GraphQL => "graphql",
			Self::InsertRelation => "insert_relation",
		}
	}
}

impl std::fmt::Display for Method {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_str())
	}
}

impl Method {
	/// Checks if the provided method is a valid and supported RPC method
	pub fn is_valid(&self) -> bool {
		!matches!(self, Self::Unknown)
	}
}
