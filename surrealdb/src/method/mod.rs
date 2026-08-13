//! Methods to use when interacting with a SurrealDB instance.
use std::borrow::Cow;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::opt::auth::{Credentials, Token};
use crate::opt::{IntoEndpoint, IntoExportDestination, WaitFor, auth};
use crate::types::{SurrealValue, Value, Variables};
use crate::{Connect, Connection, OnceLockExt, SessionClone, Surreal};

pub(crate) mod live;
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
mod insert;
mod insert_relation;
mod invalidate;
mod merge;
mod patch;
mod run;
mod select;
mod set;
mod signin;
mod signup;
mod transaction;
mod unset;
mod update;
mod upsert;
mod use_db;
mod use_defaults;
mod use_ns;
mod version;

#[cfg(test)]
mod tests;

pub use authenticate::Authenticate;
pub use begin::Begin;
pub use cancel::Cancel;
pub use commit::Commit;
pub use content::Content;
pub use create::Create;
pub use delete::Delete;
pub use export::{Backup, Export};
use futures::Future;
pub use health::Health;
pub use import::Import;
pub use insert::Insert;
pub use invalidate::Invalidate;
pub use live::Stream;
pub use merge::Merge;
pub use patch::Patch;
pub use query::{IntoVariables, ItemStream, Query, QueryStream, StreamItem};
pub use run::{IntoFn, Run};
pub use select::Select;
pub use set::Set;
pub use signin::Signin;
pub use signup::Signup;
use tokio::sync::watch;
pub use transaction::Transaction;
pub use unset::Unset;
pub use update::Update;
pub use upsert::Upsert;
pub use use_db::UseDb;
pub use use_defaults::UseDefaults;
pub use use_ns::UseNs;
pub use version::Version;

use super::opt::{CreateResource, IntoResource};

/// An alias for an often used type of future returned by async methods in this
/// library.
///
/// These futures are `Send` but not `Sync`. `Sync` was dropped in 3.3: an
/// engine may await a future from a client library that is not itself `Sync`
/// (tonic's are not), and requiring it here would rule those engines out.
/// Awaiting, spawning and moving between threads are all unaffected; only
/// code that explicitly demands `Sync` of a SurrealDB future needs changing.
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Type-state marker for [`Export::ml`](Export::ml) on an [`Export`] from
/// [`Surreal::export`](crate::Surreal::export).
pub struct Model;

/// Type-state marker for [`Export::with_config`](Export::with_config) on an [`Export`] from
/// [`Surreal::export`](crate::Surreal::export).
pub struct ExportConfig;

/// Type-state marker for [`Select::live`](Select::live) on a [`Select`] from
/// [`Surreal::select`](crate::Surreal::select).
pub struct Live;

/// Relation marker type
pub struct Relation;

/// Time elapsed for a single statement, paired by [`WithStats::take`](WithStats::take) after
/// [`Query::with_stats`](crate::method::Query::with_stats).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct Stats {
	/// The time taken to execute the query
	pub execution_time: Option<Duration>,
}

/// Wraps the [`Query`] output from [`Query::with_stats`](crate::method::Query::with_stats) so each
/// [`WithStats::take`](WithStats::take) includes [`Stats`].
#[derive(Debug)]
pub struct WithStats<T>(pub T);

impl<C> Surreal<C>
where
	C: Connection,
{
	/// Initialises a new unconnected instance of the client
	///
	/// This makes it easy to create a static singleton of the client. The
	/// static singleton pattern in the example below ensures that a single
	/// database instance is available across very large or complicated
	/// applications. With the singleton, only one connection to the database
	/// is instantiated, and the database connection does not have to be shared
	/// across components or controllers.
	///
	/// # Examples
	///
	/// Using a static, compile-time scheme
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::types::SurrealValue;
	/// use surrealdb::engine::remote::ws::Ws;
	/// use surrealdb::engine::remote::ws::Client;
	///
	/// // Creates a new static instance of the client
	/// static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);
	///
	/// #[derive(SurrealValue)]
	/// struct Person {
	///     name: String,
	/// }
	///
	/// #[tokio::main]
	/// async fn main() -> surrealdb::Result<()> {
	///     // Connect to the database
	///     DB.connect::<Ws>("localhost:8000").await?;
	///
	///     // Log into the database
	///     DB.signin(Root {
	///         username: "root".into(),
	///         password: "root".into(),
	///     }).await?;
	///
	///     // Select a namespace/database
	///     DB.use_ns("main").use_db("main").await?;
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
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::any::Any;
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::types::SurrealValue;
	///
	/// // Creates a new static instance of the client
	/// static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);
	///
	/// #[derive(SurrealValue)]
	/// struct Person {
	///     name: String,
	/// }
	///
	/// #[tokio::main]
	/// async fn main() -> surrealdb::Result<()> {
	///     // Connect to the database
	///     DB.connect("ws://localhost:8000").await?;
	///
	///     // Log into the database
	///     DB.signin(Root {
	///         username: "root".into(),
	///         password: "root".into(),
	///     }).await?;
	///
	///     // Select a namespace/database
	///     DB.use_ns("main").use_db("main").await?;
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
	pub fn init() -> Self {
		Arc::new(super::Inner {
			router: OnceLock::new(),
			waiter: watch::channel(None),
			session_clone: SessionClone::new(),
		})
		.into()
	}

	/// Connects to a local or remote database endpoint
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::{Ws, Wss};
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
	pub fn new<P>(address: impl IntoEndpoint<P, Client = C>) -> Connect<C, Self> {
		Connect {
			surreal: Surreal::init(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}

	/// Starts a client transaction and returns a handle for running
	/// [`Transaction::query`], [`Transaction::select`], and other operations
	/// in the same transaction until you call [`Transaction::commit`] or
	/// [`Transaction::cancel`].
	///
	/// This is differs from writing `BEGIN` / `COMMIT` inside a raw SurrealQL
	/// string in that the client issues a dedicated transaction begin command, then
	/// keeps a [`Transaction`] that tags each subsequent
	/// statement with the same transaction id.
	///
	/// The return type, [`Begin`], resolves to a [`Transaction`]. You can
	/// end the transaction with [`Transaction::commit`] to apply changes, or
	/// [`Transaction::cancel`] to roll them back. Both of those yield the original [`Surreal`]
	/// client again on success so you can keep using the connection.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # db.use_ns("main").use_db("main").await?;
	/// let tx = db.begin().await?;
	/// tx.query("CREATE person SET name = 'Ann'").await?;
	/// let db = tx.commit().await?;
	/// # let _ = db;
	/// # Ok(())
	/// # }
	/// ```
	pub fn begin(self) -> Begin<C> {
		Begin {
			client: self,
		}
	}

	/// Use the default namespace and database configuration
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// db.use_defaults().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn use_defaults(&self) -> UseDefaults<'_, C> {
		UseDefaults {
			client: Cow::Borrowed(self),
		}
	}

	/// Switch to a specific namespace
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// db.use_ns("main").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn use_ns(&self, ns: impl Into<String>) -> UseNs<'_, C> {
		UseNs {
			client: Cow::Borrowed(self),
			ns: ns.into(),
		}
	}

	/// Switch to a specific database
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// db.use_db("main").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn use_db(&self, db: impl Into<String>) -> UseDb<'_, C> {
		UseDb {
			client: Cow::Borrowed(self),
			ns: None,
			db: db.into(),
		}
	}

	/// Assigns a value as a parameter for this connection
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::types::SurrealValue;
	///
	/// #[derive(SurrealValue)]
	/// struct Name {
	///     first: String,
	///     last: String,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Assign the variable on the connection
	/// db.set("name", Name {
	///     first: "Tobie".into(),
	///     last: "Morgan Hitchcock".into(),
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
	pub fn set(&self, key: impl Into<String>, value: impl SurrealValue) -> Set<'_, C> {
		Set {
			client: Cow::Borrowed(self),
			key: key.into(),
			value: value.into_value(),
		}
	}

	/// Removes a parameter from this connection
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::types::SurrealValue;
	///
	/// #[derive(SurrealValue)]
	/// struct Name {
	///     first: String,
	///     last: String,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Assign the variable on the connection
	/// db.set("name", Name {
	///     first: "Tobie".into(),
	///     last: "Morgan Hitchcock".into(),
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
	pub fn unset(&'_ self, key: impl Into<String>) -> Unset<'_, C> {
		Unset {
			client: Cow::Borrowed(self),
			key: key.into(),
		}
	}

	/// Signs up a user with a specific record access method
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Record;
	/// use surrealdb::types::SurrealValue;
	///
	/// #[derive(Debug, SurrealValue)]
	/// struct AuthParams {
	///     email: String,
	///     password: String,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root".into(),
	///     password: "root".into(),
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Define the user record access
	/// let surql = r#"
	///     DEFINE ACCESS user_access ON DATABASE TYPE RECORD DURATION 24h
	///     SIGNUP ( CREATE user SET email = $email, password = crypto::argon2::generate($password) )
	///     SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(password, $password) )
	/// "#;
	/// db.query(surql).await?.check()?;
	///
	/// // Sign a user up
	/// db.signup(Record {
	///     namespace: "main".into(),
	///     database: "main".into(),
	///     access: "user_access".into(),
	///     params: AuthParams {
	///         email: "john.doe@example.com".into(),
	///         password: "password123".into(),
	///     },
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn signup(&'_ self, credentials: impl Credentials<auth::Signup>) -> Signup<'_, C> {
		Signup {
			client: Cow::Borrowed(self),
			credentials: credentials.into_value(),
		}
	}

	/// Signs this connection in to a specific authentication level
	///
	/// # Examples
	///
	/// Namespace signin
	///
	/// ```no_run
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Namespace;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root".into(),
	///     password: "root".into(),
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Define the user
	/// let surql = "DEFINE USER johndoe ON NAMESPACE PASSWORD 'password123'";
	/// db.query(surql).await?.check()?;
	///
	/// // Sign a user in
	/// db.signin(Namespace {
	///     namespace: "main".into(),
	///     username: "johndoe".into(),
	///     password: "password123".into(),
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Database signin
	///
	/// ```no_run
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Database;
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Sign in as root
	/// db.signin(Root {
	///     username: "root".into(),
	///     password: "root".into(),
	/// })
	/// .await?;
	///
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Define the user
	/// let surql = "DEFINE USER johndoe ON DATABASE PASSWORD 'password123'";
	/// db.query(surql).await?.check()?;
	///
	/// // Sign a user in
	/// db.signin(Database {
	///     namespace: "main".into(),
	///     database: "main".into(),
	///     username: "johndoe".into(),
	///     password: "password123".into(),
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Record signin
	///
	/// ```no_run
	/// use surrealdb::opt::auth::Root;
	/// use surrealdb::opt::auth::Record;
	/// use surrealdb::types::SurrealValue;
	///
	/// #[derive(Debug, SurrealValue)]
	/// struct AuthParams {
	///     email: String,
	///     password: String,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Sign a user in
	/// db.signin(Record {
	///     namespace: "main".into(),
	///     database: "main".into(),
	///     access: "user_access".into(),
	///     params: AuthParams {
	///         email: "john.doe@example.com".into(),
	///         password: "password123".into(),
	///     },
	/// }).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn signin(&'_ self, credentials: impl Credentials<auth::Signin>) -> Signin<'_, C> {
		Signin {
			client: Cow::Borrowed(self),
			credentials: credentials.into_value(),
		}
	}

	/// Invalidates the authentication for the current connection
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// db.invalidate().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn invalidate(&'_ self) -> Invalidate<'_, C> {
		Invalidate {
			client: Cow::Borrowed(self),
			token: Value::None,
			typ: PhantomData,
		}
	}

	/// Authenticates the current connection with a JWT token
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// # let token = String::new();
	/// db.authenticate(token).await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn authenticate(&'_ self, token: impl Into<Token>) -> Authenticate<'_, C> {
		Authenticate {
			client: Cow::Borrowed(self),
			token: token.into(),
			token_type: PhantomData,
		}
	}

	/// Runs a set of SurrealQL statements against the database
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::types::{SurrealValue, Table};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Run queries
	/// let mut result = db
	///     .query("CREATE person; SELECT * FROM $table")
	///     .bind(("table", Table::from("person")))
	///     .await?;
	///
	/// // Get the first result from the first query
	/// let created: Option<Person> = result.take(0)?;
	///
	/// // Get all of the results from the second query
	/// let people: Vec<Person> = result.take(1)?;
	///
	/// #[derive(SurrealValue)]
	/// struct Country {
	///     name: String
	/// }
	///
	/// // The .take() method can be used for error handling
	///
	/// // If the table has no defined schema, this query will
	/// // create a `country` on the SurrealDB side, but...
	/// let mut result = db
	///     .query("CREATE country")
	///     .await?;
	///
	/// // It won't convert into a Country struct, because the
	/// // record has no `name` field
	/// if let Err(e) = result.take::<Option<Country>>(0) {
	///     println!("Failed to make a country: {e:#?}");
	/// }
	/// #
	/// # Ok(())
	/// # }
	/// ```
	#[must_use = "queries do nothing unless you `.await` or poll them"]
	pub fn query<'client>(&'client self, query: impl Into<Cow<'client, str>>) -> Query<'client, C> {
		Query {
			txn: None,
			client: Cow::Borrowed(self),
			queries: vec![query.into()],
			variables: Ok(Variables::new()),
		}
	}

	/// Selects all records in a table, or a specific record
	///
	/// # Examples
	///
	/// ```no_run
	/// # use futures::StreamExt;
	/// # use surrealdb::opt::Resource;
	/// # use surrealdb::types::SurrealValue;
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Select all records from a table
	/// let people: Vec<Person> = db.select("person").await?;
	///
	/// // Select a range of records from a table
	/// let people: Vec<Person> = db.select("person").range("jane".."john").await?;
	///
	/// // Select a specific record from a table
	/// let person: Option<Person> = db.select(("person", "h5wxrf2ewk8xjxosxtyc")).await?;
	///
	/// // To listen for updates as they happen on a record, a range of records
	/// // or entire table use a live query. This is done by simply calling `.live()`
	/// // after this method. That gives you a stream of notifications you can listen on.
	/// # let resource = Resource::from("person");
	/// let mut stream = db.select(resource).live().await?;
	///
	/// while let Some(notification) = stream.next().await {
	///     // Use the notification
	/// }
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn select<O>(&'_ self, resource: impl IntoResource<O>) -> Select<'_, C, O> {
		Select {
			txn: None,
			client: Cow::Borrowed(self),
			resource: resource.into_resource(),
			response_type: PhantomData,
			query_type: PhantomData,
		}
	}

	/// Creates a record in the database
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::types::SurrealValue;
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     name: String,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Create a record with a random ID
	/// let person: Option<Person> = db.create("person").await?;
	///
	/// // Create a record with a specific ID
	/// let record: Option<Person> = db.create(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie".into(),
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
	pub fn create<R>(&'_ self, resource: impl CreateResource<R>) -> Create<'_, C, R> {
		Create {
			txn: None,
			client: Cow::Borrowed(self),
			resource: resource.into_resource(),
			response_type: PhantomData,
		}
	}

	/// Insert a record or records into a table
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::types::{RecordId, SurrealValue};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     name: String,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Insert a record with a specific ID
	/// let person: Option<Person> = db.insert(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie".into(),
	///         settings: Settings {
	///             active: true,
	///             marketing: true,
	///         },
	///     })
	///     .await?;
	///
	/// // Insert multiple records into the table
	/// let people: Vec<Person> = db.insert("person")
	///     .content(vec![
	///         User {
	///             name: "Tobie".into(),
	///             settings: Settings {
	///                 active: true,
	///                 marketing: false,
	///             },
	///         },
	///         User {
	///             name: "Jaime".into(),
	///             settings: Settings {
	///                 active: true,
	///                 marketing: true,
	///             },
	///         },
	///     ])
	///     .await?;
	///
	/// // Insert multiple records with pre-defined IDs
	/// #[derive(SurrealValue)]
	/// struct UserWithId {
	///     id: RecordId,
	///     name: String,
	///     settings: Settings,
	/// }
	///
	/// let people: Vec<Person> = db.insert("person")
	///     .content(vec![
	///         UserWithId {
	///             id: RecordId::new("person", "tobie"),
	///             name: "Tobie".into(),
	///             settings: Settings {
	///                 active: true,
	///                 marketing: false,
	///             },
	///         },
	///         UserWithId {
	///             id: RecordId::new("person", "jaime"),
	///             name: "Jaime".into(),
	///             settings: Settings {
	///                 active: true,
	///                 marketing: true,
	///             },
	///         },
	///     ])
	///     .await?;
	///
	/// // Insert relations
	/// #[derive(SurrealValue)]
	/// struct Founded {
	///     #[surreal(rename = "in")]
	///     founder: RecordId,
	///     #[surreal(rename = "out")]
	///     company: RecordId,
	/// }
	///
	/// let founded: Vec<Founded> = db.insert("founded")
	///     .relation(vec![
	///         Founded {
	///             founder: RecordId::new("person", "tobie"),
	///             company: RecordId::new("company", "surrealdb"),
	///         },
	///         Founded {
	///             founder: RecordId::new("person", "jaime"),
	///             company: RecordId::new("company", "surrealdb"),
	///         },
	///     ])
	///     .await?;
	///
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn insert<O>(&self, resource: impl IntoResource<O>) -> Insert<'_, C, O> {
		Insert {
			txn: None,
			client: Cow::Borrowed(self),
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
	/// use surrealdb::types::SurrealValue;
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     name: String,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.upsert("person").await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.upsert(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie".into(),
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
	/// use surrealdb::types::{Datetime, SurrealValue};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct UpdatedAt {
	///     updated_at: Datetime,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     updated_at: Datetime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.upsert("person")
	///     .merge(UpdatedAt {
	///         updated_at: Datetime::now(),
	///     })
	///     .await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.upsert(("person", "tobie"))
	///     .merge(User {
	///         updated_at: Datetime::now(),
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
	/// use surrealdb::opt::PatchOp;
	/// use surrealdb::types::{Datetime, SurrealValue};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct UpdatedAt {
	///     updated_at: Datetime,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     updated_at: Datetime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.upsert("person")
	///     .patch(PatchOp::replace("/created_at", Datetime::now()))
	///     .await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.upsert(("person", "tobie"))
	///     .patch(PatchOp::replace("/settings/active", false))
	///     .patch(PatchOp::add("/tags", ["developer", "engineer"]))
	///     .patch(PatchOp::remove("/temp"))
	///     .await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn upsert<O>(&'_ self, resource: impl IntoResource<O>) -> Upsert<'_, C, O> {
		Upsert {
			txn: None,
			client: Cow::Borrowed(self),
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
	/// use surrealdb::types::SurrealValue;
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	///     marketing: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     name: String,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person").await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.update(("person", "tobie"))
	///     .content(User {
	///         name: "Tobie".into(),
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
	/// use surrealdb::types::{Datetime, SurrealValue};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct UpdatedAt {
	///     updated_at: Datetime,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     updated_at: Datetime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person")
	///     .merge(UpdatedAt {
	///         updated_at: Datetime::now(),
	///     })
	///     .await?;
	///
	/// // Update a record with a specific ID
	/// let person: Option<Person> = db.update(("person", "tobie"))
	///     .merge(User {
	///         updated_at: Datetime::now(),
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
	/// use surrealdb::opt::PatchOp;
	/// use surrealdb::types::{Datetime, SurrealValue};
	///
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// #[derive(SurrealValue)]
	/// struct UpdatedAt {
	///     updated_at: Datetime,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct Settings {
	///     active: bool,
	/// }
	///
	/// #[derive(SurrealValue)]
	/// struct User {
	///     updated_at: Datetime,
	///     settings: Settings,
	/// }
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Update all records in a table
	/// let people: Vec<Person> = db.update("person")
	///     .patch(PatchOp::replace("/created_at", Datetime::now()))
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
	pub fn update<O>(&'_ self, resource: impl IntoResource<O>) -> Update<'_, C, O> {
		Update {
			txn: None,
			client: Cow::Borrowed(self),
			resource: resource.into_resource(),
			response_type: PhantomData,
		}
	}

	/// Deletes all records, or a specific record
	///
	/// # Examples
	///
	/// ```no_run
	/// # use surrealdb::types::SurrealValue;
	/// # #[derive(SurrealValue)]
	/// # struct Person;
	/// #
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// #
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Delete all records from a table
	/// let people: Vec<Person> = db.delete("person").await?;
	///
	/// // Delete a specific record from a table
	/// let person: Option<Person> = db.delete(("person", "h5wxrf2ewk8xjxosxtyc")).await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn delete<O>(&'_ self, resource: impl IntoResource<O>) -> Delete<'_, C, O> {
		Delete {
			txn: None,
			client: Cow::Borrowed(self),
			resource: resource.into_resource(),
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
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// let version = db.version().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn version(&'_ self) -> Version<'_, C> {
		Version {
			client: Cow::Borrowed(self),
		}
	}

	/// Runs a function
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// // Specify no args by not calling `.args()`
	/// let foo: usize = db.run("fn::foo").await?; // fn::foo()
	/// // A single value will be turned into one argument
	/// let bar: usize = db.run("fn::bar").args(42).await?; // fn::bar(42)
	/// // Arrays are treated as single arguments
	/// let count: usize = db.run("count").args(vec![1,2,3]).await?;
	/// // Specify multiple args using a tuple
	/// let two: usize = db.run("math::log").args((100, 10)).await?; // math::log(100, 10)
	///
	/// # Ok(())
	/// # }
	/// ```
	pub fn run<R>(&'_ self, function: impl IntoFn) -> Run<'_, C, R> {
		Run {
			client: Cow::Borrowed(self),
			function: function.into_fn(),
			args: Value::None,
			response_type: PhantomData,
		}
	}

	/// Checks whether the server is healthy or not
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// db.health().await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn health(&'_ self) -> Health<'_, C> {
		Health {
			client: Cow::Borrowed(self),
		}
	}

	/// Wait for the selected event to happen before proceeding
	pub async fn wait_for(&'_ self, event: WaitFor) {
		let mut rx = self.inner.waiter.0.subscribe();
		rx.wait_for(|current| match current {
			// The connection hasn't been initialised yet.
			None => false,
			// The connection has been initialised. Only the connection even matches.
			Some(WaitFor::Connection) => matches!(event, WaitFor::Connection),
			// The database has been selected. Connection and database events both match.
			Some(WaitFor::Database) => matches!(event, WaitFor::Connection | WaitFor::Database),
		})
		.await
		.ok();
	}

	/// Dumps the database contents to a file
	///
	/// # Support
	///
	/// Currently only supported by HTTP and the local engines. *Not* supported
	/// on WebAssembly.
	///
	/// # Examples
	///
	/// ```no_run
	/// # use futures::StreamExt;
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// // Export to a file
	/// db.export("backup.sql").await?;
	///
	/// // Export to a stream of bytes
	/// let mut backup = db.export(()).await?;
	/// while let Some(result) = backup.next().await {
	///     match result {
	///         Ok(bytes) => {
	///             // Do something with the bytes received...
	///         }
	///         Err(error) => {
	///             // Handle the export error
	///         }
	///     }
	/// }
	/// # Ok(())
	/// # }
	/// ```
	pub fn export<R>(&'_ self, target: impl IntoExportDestination<R>) -> Export<'_, C, R> {
		Export {
			client: Cow::Borrowed(self),
			target: target.into_export_destination(),
			ml_config: None,
			db_config: None,
			response: PhantomData,
			export_type: PhantomData,
		}
	}

	/// Restores the database from a file
	///
	/// # Support
	///
	/// Currently only supported by HTTP and the local engines. *Not* supported
	/// on WebAssembly.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// # let db = surrealdb::engine::any::connect("mem://").await?;
	/// // Select the namespace/database to use
	/// db.use_ns("main").use_db("main").await?;
	///
	/// db.import("backup.sql").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn import<P>(&'_ self, file: P) -> Import<'_, C>
	where
		P: AsRef<Path>,
	{
		Import {
			client: Cow::Borrowed(self),
			file: file.as_ref().to_owned(),
			is_ml: false,
			import_type: PhantomData,
		}
	}
}

fn validate_data(data: &Value, error_message: &str) -> crate::Result<()> {
	match data {
		Value::Object(_) => Ok(()),
		Value::Array(v) if v.iter().all(Value::is_object) => Ok(()),
		_ => Err(crate::Error::validation(error_message.to_owned(), None)),
	}
}
