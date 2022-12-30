//! Methods to use when interacting with a SurrealDB instance

pub(crate) mod query;

mod authenticate;
mod begin;
mod cancel;
mod commit;
mod content;
mod create;
mod delete;
mod export;
mod health;
mod import;
mod invalidate;
mod kill;
mod live;
mod merge;
mod patch;
mod select;
mod set;
mod signin;
mod signup;
mod unset;
mod update;
mod use_ns;
mod version;

#[cfg(test)]
mod tests;

pub use authenticate::Authenticate;
#[doc(hidden)] // Not supported yet
pub use begin::Begin;
#[doc(hidden)] // Not supported yet
pub use begin::Transaction;
#[doc(hidden)] // Not supported yet
pub use cancel::Cancel;
#[doc(hidden)] // Not supported yet
pub use commit::Commit;
pub use content::Content;
pub use create::Create;
pub use delete::Delete;
pub use export::Export;
pub use health::Health;
pub use import::Import;
pub use invalidate::Invalidate;
#[doc(hidden)] // Not supported yet
pub use kill::Kill;
#[doc(hidden)] // Not supported yet
pub use live::Live;
pub use merge::Merge;
pub use patch::Patch;
pub use query::Query;
pub use select::Select;
pub use set::Set;
pub use signin::Signin;
pub use signup::Signup;
pub use unset::Unset;
pub use update::Update;
pub use use_ns::UseNs;
pub use use_ns::UseNsDb;
pub use version::Version;

use crate::api::conn::Method;
use crate::api::opt;
use crate::api::opt::auth;
use crate::api::opt::auth::Credentials;
use crate::api::opt::auth::Jwt;
use crate::api::opt::from_json;
use crate::api::opt::IntoEndpoint;
use crate::api::Connect;
use crate::api::Connection;
use crate::api::ExtractRouter;
use crate::api::Surreal;
use crate::sql::Uuid;
use once_cell::sync::OnceCell;
use serde::Serialize;
use serde_json::json;
use std::marker::PhantomData;
use std::path::Path;

impl Method {
	#[allow(dead_code)] // used by `ws` and `http`
	pub(crate) fn as_str(&self) -> &str {
		match self {
			Method::Authenticate => "authenticate",
			Method::Create => "create",
			Method::Delete => "delete",
			Method::Export => "export",
			Method::Health => "health",
			Method::Import => "import",
			Method::Invalidate => "invalidate",
			Method::Kill => "kill",
			Method::Live => "live",
			Method::Merge => "merge",
			Method::Patch => "patch",
			Method::Query => "query",
			Method::Select => "select",
			Method::Set => "set",
			Method::Signin => "signin",
			Method::Signup => "signup",
			Method::Unset => "unset",
			Method::Update => "update",
			Method::Use => "use",
			Method::Version => "version",
		}
	}
}

impl<C> Surreal<C>
where
	C: Connection,
{
	/// Creates a new static instance of the client
	///
	/// The static singleton ensures that a single database instance is available across very large
	/// or complicated applications. With the singleton, only one connection to the database is
	/// instantiated, and the database connection does not have to be shared across components
	/// or controllers.
	///
	/// # Examples
	///
	/// Using a static, compile-time scheme
	///
	/// ```no_run
	/// use serde::{Serialize, Deserialize};
	/// use std::borrow::Cow;
	/// use surrealdb::Surreal;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::engines::remote::ws::Ws;
	/// use surrealdb::engines::remote::ws::Client;
	///
	/// // Creates a new static instance of the client
	/// static DB: Surreal<Client> = Surreal::init();
	///
	/// #[derive(Serialize, Deserialize)]
	/// struct Person {
	///     name: Cow<'static, str>,
	/// }
	///
	/// #[tokio::main]
	/// async fn main() -> surrealdb::Result<()> {
	///     // Connect to the database
	///     DB.connect::<Ws>("localhost:8000").await?;
	///
	///     // Log into the database
	///     DB.signin(Root {
	///         username: "root",
	///         password: "root",
	///     }).await?;
	///
	///     // Select a namespace/database
	///     DB.use_ns("namespace").use_db("database").await?;
	///
	///     // Create or update a specific record
	///     let tobie: Option<Person> = DB.update(("person", "tobie"))
	///         .content(Person {
	///             name: "Tobie".into(),
	///         }).await?;
	///
	///     Ok(())
	/// }
	/// ```
	///
	/// Using a dynamic, run-time scheme
	///
	/// ```no_run
	/// use serde::{Serialize, Deserialize};
	/// use std::borrow::Cow;
	/// use surrealdb::Surreal;
	/// use surrealdb::engines::any::Any;
	/// use surrealdb::opt::auth::Root;
	///
	/// // Creates a new static instance of the client
	/// static DB: Surreal<Any> = Surreal::init();
	///
	/// #[derive(Serialize, Deserialize)]
	/// struct Person {
	///     name: Cow<'static, str>,
	/// }
	///
	/// #[tokio::main]
	/// async fn main() -> surrealdb::Result<()> {
	///     // Connect to the database
	///     DB.connect("ws://localhost:8000").await?;
	///
	///     // Log into the database
	///     DB.signin(Root {
	///         username: "root",
	///         password: "root",
	///     }).await?;
	///
	///     // Select a namespace/database
	///     DB.use_ns("namespace").use_db("database").await?;
	///
	///     // Create or update a specific record
	///     let tobie: Option<Person> = DB.update(("person", "tobie"))
	///         .content(Person {
	///             name: "Tobie".into(),
	///         }).await?;
	///
	///     Ok(())
	/// }
	/// ```
	pub const fn init() -> Self {
		Self {
			router: OnceCell::new(),
		}
	}

	/// Connects to a local or remote database endpoint
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engines::remote::ws::{Ws, Wss};
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// // Connect to a local endpoint
	/// let db = Surreal::new::<Ws>("localhost:8000").await?;
	///
	/// // Connect to a remote endpoint
	/// let db = Surreal::new::<Wss>("cloud.surrealdb.com").await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn new<P>(address: impl IntoEndpoint<P, Client = C>) -> Connect<'static, C, Self> {
		Connect {
			router: None,
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
			response_type: PhantomData,
		}
	}

	#[doc(hidden)] // Not supported yet
	pub fn transaction(self) -> Begin<C> {
		Begin {
			client: self,
		}
	}

	/// Switch to a specific namespace
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// db.use_ns("namespace").use_db("database").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn use_ns(&self, ns: impl Into<String>) -> UseNs<C> {
		UseNs {
			router: self.router.extract(),
			ns: ns.into(),
		}
	}

	/// Assigns a value as a parameter for this connection
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// #[derive(Serialize)]
	/// struct Name<'a> {
	///     first: &'a str,
	///     last: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Assign the variable on the connection
	/// db.set("name", Name {
	///     first: "Tobie",
	///     last: "Morgan Hitchcock",
	/// }).await?;
	///
	/// // Use the variable in a subsequent query
	/// db.query("CREATE person SET name = $name").await?;
	///
	/// // Use the variable in a subsequent query
	/// db.query("SELECT * FROM person WHERE name.first = $name.first").await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn set(&self, key: impl Into<String>, value: impl Serialize) -> Set<C> {
		Set {
			router: self.router.extract(),
			key: key.into(),
			value: Ok(from_json(json!(value))),
		}
	}

	/// Removes a parameter from this connection
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// #[derive(Serialize)]
	/// struct Name<'a> {
	///     first: &'a str,
	///     last: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Assign the variable on the connection
	/// db.set("name", Name {
	///     first: "Tobie",
	///     last: "Morgan Hitchcock",
	/// }).await?;
	///
	/// // Use the variable in a subsequent query
	/// db.query("CREATE person SET name = $name").await?;
	///
	/// // Remove the variable from the connection
	/// db.unset("name").await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn unset(&self, key: impl Into<String>) -> Unset<C> {
		Unset {
			router: self.router.extract(),
			key: key.into(),
		}
	}

	/// Signs up a user to a specific authentication scope
	///
	/// # Support
	///
	/// Currently only supported by the WS and HTTP protocols.
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Serialize;
	/// use surrealdb::sql;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Scope;
	///
	/// #[derive(Debug, Serialize)]
	/// struct AuthParams<'a> {
	///     email: &'a str,
	///     password: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root",
	///     password: "root",
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Define the scope
	/// let sql = sql! {
	///     DEFINE SCOPE user_scope SESSION 24h
	///     SIGNUP ( CREATE user SET email = $email, password = crypto::argon2::generate($password) )
	///     SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(password, $password) )
	/// };
	/// db.query(sql).await?.check()?;
	///
	/// // Sign a user up
	/// db.signup(Scope {
	///     namespace: "namespace",
	///     database: "database",
	///     scope: "user_scope",
	///     params: AuthParams {
	///         email: "john.doe@example.com",
	///         password: "password123",
	///     },
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn signup<R>(&self, credentials: impl Credentials<auth::Signup, R>) -> Signup<C, R> {
		Signup {
			router: self.router.extract(),
			credentials: Ok(from_json(json!(credentials))),
			response_type: PhantomData,
		}
	}

	/// Signs this connection in to a specific authentication scope
	///
	/// # Support
	///
	/// Currently only supported by the WS and HTTP protocols.
	///
	/// # Examples
	///
	/// Namespace signin
	///
	/// ```no_run
	/// use surrealdb::sql;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Namespace;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root",
	///     password: "root",
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Define the login
	/// let sql = sql!(DEFINE LOGIN johndoe ON NAMESPACE PASSWORD "password123");
	/// db.query(sql).await?.check()?;
	///
	/// // Sign a user in
	/// db.signin(Namespace {
	///     namespace: "namespace",
	///     username: "johndoe",
	///     password: "password123",
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Database signin
	///
	/// ```no_run
	/// use surrealdb::sql;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Database;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root",
	///     password: "root",
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Define the login
	/// let sql = sql!(DEFINE LOGIN johndoe ON DATABASE PASSWORD "password123");
	/// db.query(sql).await?.check()?;
	///
	/// // Sign a user in
	/// db.signin(Database {
	///     namespace: "namespace",
	///     database: "database",
	///     username: "johndoe",
	///     password: "password123",
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Scope signin
	///
	/// ```no_run
	/// use serde::Serialize;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Scope;
	///
	/// #[derive(Debug, Serialize)]
	/// struct AuthParams<'a> {
	///     email: &'a str,
	///     password: &'a str,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Sign a user in
	/// db.signin(Scope {
	///     namespace: "namespace",
	///     database: "database",
	///     scope: "user_scope",
	///     params: AuthParams {
	///         email: "john.doe@example.com",
	///         password: "password123",
	///     },
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn signin<R>(&self, credentials: impl Credentials<auth::Signin, R>) -> Signin<C, R> {
		Signin {
			router: self.router.extract(),
			credentials: Ok(from_json(json!(credentials))),
			response_type: PhantomData,
		}
	}

	/// Invalidates the authentication for the current connection
	///
	/// # Support
	///
	/// Currently only supported by the WS and HTTP protocols.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// db.invalidate().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn invalidate(&self) -> Invalidate<C> {
		Invalidate {
			router: self.router.extract(),
		}
	}

	/// Authenticates the current connection with a JWT token
	///
	/// # Support
	///
	/// Currently only supported by the WS and HTTP protocols.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// # let token = String::new();
	/// db.authenticate(token).await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn authenticate(&self, token: impl Into<Jwt>) -> Authenticate<C> {
		Authenticate {
			router: self.router.extract(),
			token: token.into(),
		}
	}

	/// Runs a set of SurrealQL statements against the database
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::sql;
	///
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Run queries
	/// let mut result = db
	///     .query(sql!(CREATE person))
	///     .query(sql!(SELECT * FROM type::table($table)))
	///     .bind(("table", "person"))
	///     .await?;
	///
	/// // Get the first result from the first query
	/// let created: Option<Person> = result.take(0)?;
	///
	/// // Get all of the results from the second query
	/// let people: Vec<Person> = result.take(1)?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn query(&self, query: impl opt::IntoQuery) -> Query<C> {
		Query {
			router: self.router.extract(),
			query: vec![query.into_query()],
			bindings: Ok(Default::default()),
		}
	}

	/// Selects all records in a table, or a specific record
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// #
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Select all records from a table
	/// let people: Vec<Person> = db.select("person").await?;
	///
	/// // Select a specific record from a table
	/// let person: Option<Person> = db.select(("person", "h5wxrf2ewk8xjxosxtyc")).await?;
	///
	/// // You can skip an unnecessary option if you know the record already exists
	/// let person: Person = db.select(("person", "h5wxrf2ewk8xjxosxtyc")).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn select<R>(&self, resource: impl opt::IntoResource<R>) -> Select<C, R> {
		Select {
			router: self.router.extract(),
			resource: resource.into_resource(),
			range: None,
			response_type: PhantomData,
		}
	}

	/// Creates a record in the database
	///
	/// # Examples
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// #
	/// #[derive(Serialize)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct User<'a> {
	///     name: &'a str,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Create a record with a random ID
	/// let person: Person = db.create("person").await?;
	///
	/// // Create a record with a specific ID
	/// let record: Person = db.create(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie",
	///         settings: Settings {
	///             active: true,
	///             marketing: true,
	///         },
	///     })
	///     .await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn create<R>(&self, resource: impl opt::IntoResource<R>) -> Create<C, R> {
		Create {
			router: self.router.extract(),
			resource: resource.into_resource(),
			response_type: PhantomData,
		}
	}

	/// Updates all records in a table, or a specific record
	///
	/// # Examples
	///
	/// Replace the current document / record data with the specified data.
	///
	/// ```no_run
	/// use serde::Serialize;
	///
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// #
	/// #[derive(Serialize)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct User<'a> {
	///     name: &'a str,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person").await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.update(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie",
	///         settings: Settings {
	///             active: true,
	///             marketing: true,
	///         },
	///     })
	///     .await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Merge the current document / record data with the specified data.
	///
	/// ```no_run
	/// use serde::Serialize;
	/// use time::OffsetDateTime;
	///
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// #
	/// #[derive(Serialize)]
	/// struct UpdatedAt {
	///     updated_at: OffsetDateTime,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct User {
	///     updated_at: OffsetDateTime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person")
	///     .merge(UpdatedAt {
	///         updated_at: OffsetDateTime::now_utc(),
	///     })
	///     .await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.update(("person", "tobie"))
	///     .merge(User {
	///         updated_at: OffsetDateTime::now_utc(),
	///         settings: Settings {
	///             active: true,
	///         },
	///     })
	///     .await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Apply [JSON Patch](https://jsonpatch.com) changes to all records, or a specific record, in the database.
	///
	/// ```no_run
	/// use serde::Serialize;
	/// use surrealdb::opt::PatchOp;
	/// use time::OffsetDateTime;
	///
	/// # #[derive(serde::Deserialize)]
	/// # struct Person;
	/// #
	/// #[derive(Serialize)]
	/// struct UpdatedAt {
	///     updated_at: OffsetDateTime,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(Serialize)]
	/// struct User {
	///     updated_at: OffsetDateTime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person")
	///     .patch(PatchOp::replace("/created_at", OffsetDateTime::now_utc()))
	///     .await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.update(("person", "tobie"))
	///     .patch(PatchOp::replace("/settings/active", false))
	///     .patch(PatchOp::add("/tags", ["developer", "engineer"]))
	///     .patch(PatchOp::remove("/temp"))
	///     .await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn update<R>(&self, resource: impl opt::IntoResource<R>) -> Update<C, R> {
		Update {
			router: self.router.extract(),
			resource: resource.into_resource(),
			range: None,
			response_type: PhantomData,
		}
	}

	/// Deletes all records, or a specific record
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// // Delete all records from a table
	/// db.delete("person").await?;
	///
	/// // Delete a specific record from a table
	/// db.delete(("person", "h5wxrf2ewk8xjxosxtyc")).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn delete<R>(&self, resource: impl opt::IntoResource<R>) -> Delete<C, R> {
		Delete {
			router: self.router.extract(),
			resource: resource.into_resource(),
			range: None,
			response_type: PhantomData,
		}
	}

	/// Returns the version of the server
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// let version = db.version().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn version(&self) -> Version<C> {
		Version {
			router: self.router.extract(),
		}
	}

	/// Checks whether the server is healthy or not
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// db.health().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn health(&self) -> Health<C> {
		Health {
			router: self.router.extract(),
		}
	}

	#[doc(hidden)] // Not supported yet
	pub fn kill(&self, query_id: Uuid) -> Kill<C> {
		Kill {
			router: self.router.extract(),
			query_id,
		}
	}

	#[doc(hidden)] // Not supported yet
	pub fn live(&self, table_name: impl Into<String>) -> Live<C> {
		Live {
			router: self.router.extract(),
			table_name: table_name.into(),
		}
	}

	/// Dumps the database contents to a file
	///
	/// # Support
	///
	/// Currently only supported by HTTP and the local engines. *Not* supported on WebAssembly.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// db.export("backup.sql").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn export<P>(&self, file: P) -> Export<C>
	where
		P: AsRef<Path>,
	{
		Export {
			router: self.router.extract(),
			file: file.as_ref().to_owned(),
		}
	}

	/// Restores the database from a file
	///
	/// # Support
	///
	/// Currently only supported by HTTP and the local engines. *Not* supported on WebAssembly.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engines::any::connect("mem://").await?;
	/// // Select the namespace/database to use
	/// db.use_ns("namespace").use_db("database").await?;
	///
	/// db.import("backup.sql").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn import<P>(&self, file: P) -> Import<C>
	where
		P: AsRef<Path>,
	{
		Import {
			router: self.router.extract(),
			file: file.as_ref().to_owned(),
		}
	}
}
