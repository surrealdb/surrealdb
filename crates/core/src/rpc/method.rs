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
			"authenticate" => Self::Authenticate,
			"invalidate" => Self::Invalidate,
			"reset" => Self::Reset,
			"kill" => Self::Kill,
			"live" => Self::Live,
			"let" | "set" => Self::Set,
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

impl Method {
	/// Checks if the provided method is a valid and supported RPC method
	pub fn is_valid(&self) -> bool {
		!matches!(self, Self::Unknown)
	}
	/// Checks if this method needs mutable access to the RPC session
	pub fn needs_mutability(&self) -> bool {
		!matches!(
			self,
			Method::Ping
				| Method::Info
				| Method::Select
				| Method::Insert
				| Method::Create
				| Method::Upsert
				| Method::Update
				| Method::Merge
				| Method::Patch
				| Method::Delete
				| Method::Version
				| Method::Query
				| Method::Relate
				| Method::Run
				| Method::GraphQL
				| Method::InsertRelation
				| Method::Unknown
		)
	}
}
