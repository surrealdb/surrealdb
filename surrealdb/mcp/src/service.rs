//! MCP ServerHandler implementation for SurrealDB.
//!
//! `McpService` is the core MCP server type. One instance is created per MCP
//! session via the factory closure in `StreamableHttpService`.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::sync::OnceCell;

use crate::session::McpSession;
use crate::tools::{connection, crud, query, schema};
use crate::{completions, prompts, resources};

const LOG: &str = "surrealdb::mcp";

/// The MCP server handler for SurrealDB.
#[derive(Clone)]
pub struct McpService {
	session: Arc<OnceCell<McpSession>>,
	datastore: Arc<Datastore>,
	default_ns: Option<String>,
	default_db: Option<String>,
	tool_router: ToolRouter<Self>,
}

impl McpService {
	pub fn new(
		datastore: Arc<Datastore>,
		default_ns: Option<String>,
		default_db: Option<String>,
	) -> Self {
		Self {
			session: Arc::new(OnceCell::new()),
			datastore,
			default_ns,
			default_db,
			tool_router: Self::tool_router(),
		}
	}

	fn session(&self) -> Result<&McpSession, McpError> {
		self.session
			.get()
			.ok_or_else(|| McpError::internal_error("MCP session not initialized", None))
	}

	/// Get a reference to the inner session, if initialized.
	pub fn session_ref(&self) -> Result<&McpSession, McpError> {
		self.session()
	}

	/// Initialize the session. Called during MCP handshake.
	pub fn init_session(&self, session: Session) -> Result<(), McpError> {
		let mcp_session = McpSession::new(self.datastore.clone(), session);
		self.session
			.set(mcp_session)
			.map_err(|_| McpError::internal_error("Session already initialized", None))
	}
}

// ---------------------------------------------------------------------------
// Tool implementations -- use types from tools/ modules directly
// ---------------------------------------------------------------------------

#[tool_router]
impl McpService {
	#[tool(
		description = "Execute a SurrealQL query with optional parameterized inputs. Use $param syntax for placeholders and provide bindings in the parameters object."
	)]
	async fn query(
		&self,
		Parameters(p): Parameters<query::QueryParams>,
	) -> Result<CallToolResult, McpError> {
		query::execute(self.session()?, p).await
	}

	#[tool(
		description = "SELECT records with optional filtering, sorting, and pagination. WHERE clauses are SurrealQL expressions."
	)]
	async fn select(
		&self,
		Parameters(p): Parameters<crud::SelectParams>,
	) -> Result<CallToolResult, McpError> {
		crud::select(self.session()?, p).await
	}

	#[tool(
		description = "CREATE a new record with optional content data. Data is bound as a typed variable."
	)]
	async fn create(
		&self,
		Parameters(p): Parameters<crud::CreateParams>,
	) -> Result<CallToolResult, McpError> {
		crud::create(self.session()?, p).await
	}

	#[tool(
		description = "INSERT records into a table. Data is bound as a typed variable. Supports IGNORE and RELATION flags."
	)]
	async fn insert(
		&self,
		Parameters(p): Parameters<crud::InsertParams>,
	) -> Result<CallToolResult, McpError> {
		crud::insert(self.session()?, p).await
	}

	#[tool(
		description = "UPSERT records with CONTENT, MERGE, or PATCH mode. Data is bound as a typed variable."
	)]
	async fn upsert(
		&self,
		Parameters(p): Parameters<crud::UpsertParams>,
	) -> Result<CallToolResult, McpError> {
		crud::upsert(self.session()?, p).await
	}

	#[tool(
		description = "UPDATE existing records with CONTENT, MERGE, or PATCH mode. Data is bound as a typed variable."
	)]
	async fn update(
		&self,
		Parameters(p): Parameters<crud::UpdateParams>,
	) -> Result<CallToolResult, McpError> {
		crud::update(self.session()?, p).await
	}

	#[tool(description = "DELETE records with an optional WHERE clause.")]
	async fn delete(
		&self,
		Parameters(p): Parameters<crud::DeleteParams>,
	) -> Result<CallToolResult, McpError> {
		crud::delete(self.session()?, p).await
	}

	#[tool(
		description = "RELATE records to create graph edges (from->table->to). Optional content is bound as a typed variable."
	)]
	async fn relate(
		&self,
		Parameters(p): Parameters<crud::RelateParams>,
	) -> Result<CallToolResult, McpError> {
		crud::relate(self.session()?, p).await
	}

	#[tool(description = "View schema information. Target: 'root', 'ns', 'db', or a table name.")]
	async fn info(
		&self,
		Parameters(p): Parameters<schema::InfoParams>,
	) -> Result<CallToolResult, McpError> {
		schema::info(self.session()?, p).await
	}

	#[tool(description = "List all accessible namespaces.")]
	async fn list_namespaces(&self) -> Result<CallToolResult, McpError> {
		schema::list_namespaces(self.session()?).await
	}

	#[tool(description = "List all databases in the current namespace.")]
	async fn list_databases(&self) -> Result<CallToolResult, McpError> {
		schema::list_databases(self.session()?).await
	}

	#[tool(description = "List all tables in the current database.")]
	async fn list_tables(&self) -> Result<CallToolResult, McpError> {
		schema::list_tables(self.session()?).await
	}

	#[tool(
		description = "Get full schema for a specific table including fields, indexes, events, and permissions."
	)]
	async fn describe_table(
		&self,
		Parameters(p): Parameters<schema::DescribeTableParams>,
	) -> Result<CallToolResult, McpError> {
		schema::describe_table(self.session()?, p).await
	}

	#[tool(description = "Get SurrealDB version information.")]
	async fn version(&self) -> Result<CallToolResult, McpError> {
		Ok(schema::version())
	}

	#[tool(
		description = "Show the execution plan for a SurrealQL query. Use 'full' for actual execution statistics."
	)]
	async fn explain(
		&self,
		Parameters(p): Parameters<query::ExplainParams>,
	) -> Result<CallToolResult, McpError> {
		query::explain(self.session()?, p).await
	}

	#[tool(description = "Switch to a different namespace.")]
	async fn use_namespace(
		&self,
		Parameters(p): Parameters<connection::UseNamespaceParams>,
	) -> Result<CallToolResult, McpError> {
		connection::use_namespace(self.session()?, p).await
	}

	#[tool(description = "Switch to a different database.")]
	async fn use_database(
		&self,
		Parameters(p): Parameters<connection::UseDatabaseParams>,
	) -> Result<CallToolResult, McpError> {
		connection::use_database(self.session()?, p).await
	}
}

// ---------------------------------------------------------------------------
// ServerHandler -- wires tools, resources, prompts, completions
// ---------------------------------------------------------------------------

#[tool_handler]
impl ServerHandler for McpService {
	fn get_info(&self) -> ServerInfo {
		ServerInfo::new(
			ServerCapabilities::builder()
				.enable_tools()
				.enable_resources()
				.enable_prompts()
				.enable_completions()
				.build(),
		)
		.with_server_info(Implementation::from_build_env())
		.with_instructions(resources::instructions::get_instructions().to_string())
	}

	#[tracing::instrument(skip_all, target = "surrealdb::mcp")]
	async fn initialize(
		&self,
		_request: InitializeRequestParams,
		ctx: RequestContext<RoleServer>,
	) -> Result<InitializeResult, McpError> {
		let mut session = ctx
			.extensions
			.get::<http::request::Parts>()
			.and_then(crate::auth::extract_session_from_parts)
			.unwrap_or_else(|| {
				tracing::debug!(target: LOG, "No session in request context, using default");
				Session::default()
			});

		if session.ns.is_none()
			&& let Some(ns) = &self.default_ns
		{
			session.ns = Some(ns.clone());
		}
		if session.db.is_none()
			&& let Some(db) = &self.default_db
		{
			session.db = Some(db.clone());
		}

		self.init_session(session)?;
		tracing::info!(target: LOG, "MCP session initialized");
		Ok(self.get_info())
	}

	async fn list_resources(
		&self,
		_: Option<PaginatedRequestParams>,
		_: RequestContext<RoleServer>,
	) -> Result<ListResourcesResult, McpError> {
		Ok(ListResourcesResult {
			resources: resources::list_resources(),
			next_cursor: None,
			meta: None,
		})
	}

	async fn read_resource(
		&self,
		request: ReadResourceRequestParams,
		_: RequestContext<RoleServer>,
	) -> Result<ReadResourceResult, McpError> {
		resources::read_resource(self.session()?, &request.uri).await
	}

	async fn list_prompts(
		&self,
		_: Option<PaginatedRequestParams>,
		_: RequestContext<RoleServer>,
	) -> Result<ListPromptsResult, McpError> {
		Ok(ListPromptsResult {
			prompts: prompts::list_prompts(),
			next_cursor: None,
			meta: None,
		})
	}

	async fn get_prompt(
		&self,
		request: GetPromptRequestParams,
		_: RequestContext<RoleServer>,
	) -> Result<GetPromptResult, McpError> {
		let args = request
			.arguments
			.as_ref()
			.map(|a| serde_json::to_value(a).unwrap_or_default())
			.unwrap_or_default();
		prompts::get_prompt(&request.name, &args).ok_or_else(|| {
			McpError::invalid_params(format!("Unknown prompt: {}", request.name), None)
		})
	}

	async fn complete(
		&self,
		request: CompleteRequestParams,
		_: RequestContext<RoleServer>,
	) -> Result<CompleteResult, McpError> {
		Ok(completions::handle_completion(self.session()?, &request).await)
	}
}

// ---------------------------------------------------------------------------
// Stdio transport
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-io")]
pub use stdio_service::*;

#[cfg(feature = "transport-io")]
mod stdio_service {
	use super::*;

	/// Serve the MCP server over stdio (stdin/stdout).
	pub async fn serve_stdio(service: McpService) -> Result<(), anyhow::Error> {
		let stdin = tokio::io::stdin();
		let stdout = tokio::io::stdout();
		rmcp::ServiceExt::serve(service, (stdin, stdout))
			.await
			.map_err(|e| anyhow::anyhow!("MCP stdio error: {e}"))?
			.waiting()
			.await
			.map_err(|e| anyhow::anyhow!("MCP stdio error: {e}"))?;
		Ok(())
	}
}

// ---------------------------------------------------------------------------
// HTTP service factory
// ---------------------------------------------------------------------------

#[cfg(feature = "server-http")]
pub use http_service::*;

#[cfg(feature = "server-http")]
mod http_service {
	use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
	use rmcp::transport::streamable_http_server::{
		StreamableHttpServerConfig, StreamableHttpService,
	};

	use super::*;

	/// The fully-typed MCP HTTP service.
	pub type McpHttpService = StreamableHttpService<McpService, LocalSessionManager>;

	/// Create a `StreamableHttpService` backed by the given datastore.
	pub fn create_http_service(ds: Arc<Datastore>) -> McpHttpService {
		let mut config = StreamableHttpServerConfig::default();
		config.stateful_mode = true;
		StreamableHttpService::new(
			move || Ok(McpService::new(ds.clone(), None, None)),
			Arc::new(LocalSessionManager::default()),
			config,
		)
	}
}
