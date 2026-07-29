#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum Method {
	Unknown,
	Ping,
	Info,
	Use,
	Signup,
	Signin,
	Authenticate,
	Refresh,
	Invalidate,
	Revoke,
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
	Gql,
	Graphql,
	Relate,
	Run,
	InsertRelation,
	Attach,
	Sessions,
	Detach,
	Begin,
	Commit,
	Cancel,
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
			"refresh" => Self::Refresh,
			"invalidate" => Self::Invalidate,
			"revoke" => Self::Revoke,
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
			"gql" => Self::Gql,
			"graphql" => Self::Graphql,
			"relate" => Self::Relate,
			"run" => Self::Run,
			"insert_relation" => Self::InsertRelation,
			"attach" => Self::Attach,
			"sessions" => Self::Sessions,
			"detach" => Self::Detach,
			"begin" => Self::Begin,
			"commit" => Self::Commit,
			"cancel" => Self::Cancel,
			_ => Self::Unknown,
		}
	}
}

impl Method {
	/// Stable lower-case label for this method. Always a `'static`
	/// literal so it is safe to use as a Prometheus label value or to
	/// stash on a `&'static str` field.
	pub fn to_str(&self) -> &'static str {
		match self {
			Self::Unknown => "unknown",
			Self::Ping => "ping",
			Self::Info => "info",
			Self::Use => "use",
			Self::Signup => "signup",
			Self::Signin => "signin",
			Self::Authenticate => "authenticate",
			Self::Refresh => "refresh",
			Self::Invalidate => "invalidate",
			Self::Revoke => "revoke",
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
			Self::Gql => "gql",
			Self::Graphql => "graphql",
			Self::Relate => "relate",
			Self::Run => "run",
			Self::InsertRelation => "insert_relation",
			Self::Attach => "attach",
			Self::Sessions => "sessions",
			Self::Detach => "detach",
			Self::Begin => "begin",
			Self::Commit => "commit",
			Self::Cancel => "cancel",
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

	/// Whether this method controls an explicit transaction lifecycle
	/// (`begin` / `commit` / `cancel`).
	///
	/// Transaction control is the *only* exemption from the per-call wall-clock
	/// query timeout, primarily for safety: `commit` removes the transaction
	/// from the connection *before* awaiting the durable commit, so a wall-clock
	/// timeout dropping that future mid-flight could leave a transaction marked
	/// done but not committed. `begin` / `cancel` are exempted alongside it.
	///
	/// The exemption does not leave real work unbounded: the individual
	/// statement methods executed inside the transaction (`query`, `create`,
	/// ...) are each still guarded by the wall-clock query timeout. Note,
	/// however, that `--transaction-timeout` only caps executor-run
	/// transactions (auto-transactions and SurrealQL `BEGIN ... COMMIT`
	/// blocks); a transaction held open across RPC calls (WebSocket
	/// `begin`/`commit`) is bounded only by its per-statement timeouts and the
	/// connection lifetime, not by `--transaction-timeout`.
	///
	/// Auth methods (`signup` / `signin` / `authenticate`) are deliberately
	/// *not* exempt: for record access they execute user-defined SurrealQL
	/// (the access method's `SIGNIN` / `SIGNUP` / `AUTHENTICATE` clauses),
	/// which is exactly the kind of potentially unbounded work the guard exists
	/// to cap. Dropping an auth future on timeout is safe (no partial state is
	/// committed), so bounding them closes a DoS vector rather than opening one.
	pub fn is_transaction_control(&self) -> bool {
		matches!(self, Self::Begin | Self::Commit | Self::Cancel)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn gql_method_round_trips() {
		assert_eq!(Method::parse_case_sensitive("gql"), Method::Gql);
		assert_eq!(Method::parse_case_insensitive("GQL"), Method::Gql);
		assert_eq!(Method::Gql.to_str(), "gql");
		assert!(Method::Gql.is_valid());
	}

	#[test]
	fn transaction_control_methods_are_flagged() {
		// Only the explicit-transaction lifecycle methods are exempt from the
		// wall-clock query timeout.
		assert!(Method::Begin.is_transaction_control());
		assert!(Method::Commit.is_transaction_control());
		assert!(Method::Cancel.is_transaction_control());
		// Statement / query methods are guarded.
		assert!(!Method::Query.is_transaction_control());
		assert!(!Method::Graphql.is_transaction_control());
		assert!(!Method::Gql.is_transaction_control());
		assert!(!Method::Create.is_transaction_control());
		assert!(!Method::Select.is_transaction_control());
		// Auth methods are deliberately guarded too: record-access
		// signin/signup/authenticate run user-defined SurrealQL, so the
		// wall-clock timeout must be able to bound them.
		assert!(!Method::Signin.is_transaction_control());
		assert!(!Method::Signup.is_transaction_control());
		assert!(!Method::Authenticate.is_transaction_control());
	}

	#[test]
	fn graphql_method_round_trips() {
		assert_eq!(Method::parse_case_sensitive("graphql"), Method::Graphql);
		assert_eq!(Method::parse_case_insensitive("GraphQL"), Method::Graphql);
		assert_eq!(Method::Graphql.to_str(), "graphql");
		assert!(Method::Graphql.is_valid());
		// `gql` (GQL) and `graphql` are distinct methods.
		assert_ne!(Method::Gql, Method::Graphql);
	}
}
