use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::ClientInfo;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_sql_compat::DialectTranslator;
use surrealdb_sql_compat::postgres::PostgresTranslator;

use super::results::query_results_to_response;

pub struct SurrealQueryHandler {
	datastore: Arc<Datastore>,
	translator: PostgresTranslator,
	query_parser: Arc<NoopQueryParser>,
}

impl SurrealQueryHandler {
	pub fn new(datastore: Arc<Datastore>) -> Self {
		Self {
			datastore,
			translator: PostgresTranslator,
			query_parser: Arc::new(NoopQueryParser::new()),
		}
	}
}

fn pg_error(sqlstate: &str, message: impl ToString) -> PgWireError {
	PgWireError::UserError(Box::new(ErrorInfo::new(
		"ERROR".to_owned(),
		sqlstate.to_owned(),
		message.to_string(),
	)))
}

#[async_trait]
impl SimpleQueryHandler for SurrealQueryHandler {
	async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
	where
		C: ClientInfo + Unpin + Send + Sync,
	{
		let plan = self.translator.translate(query).map_err(|e| pg_error("42601", e))?;
		let session = build_session(client);
		let results = self
			.datastore
			.process_plan(plan, &session, None)
			.await
			.map_err(|e| pg_error("XX000", e))?;

		query_results_to_response(results)
	}
}

#[async_trait]
impl ExtendedQueryHandler for SurrealQueryHandler {
	type Statement = String;
	type QueryParser = NoopQueryParser;

	fn query_parser(&self) -> Arc<Self::QueryParser> {
		self.query_parser.clone()
	}

	async fn do_query<C>(
		&self,
		_client: &mut C,
		_portal: &Portal<Self::Statement>,
		_max_rows: usize,
	) -> PgWireResult<Response>
	where
		C: ClientInfo + Unpin + Send + Sync,
	{
		Err(pg_error("0A000", "Extended query protocol not yet implemented"))
	}

	async fn do_describe_statement<C>(
		&self,
		_client: &mut C,
		_stmt: &StoredStatement<Self::Statement>,
	) -> PgWireResult<DescribeStatementResponse>
	where
		C: ClientInfo + Unpin + Send + Sync,
	{
		Err(pg_error("0A000", "Extended query protocol not yet implemented"))
	}

	async fn do_describe_portal<C>(
		&self,
		_client: &mut C,
		_portal: &Portal<Self::Statement>,
	) -> PgWireResult<DescribePortalResponse>
	where
		C: ClientInfo + Unpin + Send + Sync,
	{
		Err(pg_error("0A000", "Extended query protocol not yet implemented"))
	}
}

fn build_session<C: ClientInfo>(client: &C) -> Session {
	let mut session = Session::owner();
	if let Some(db) = client.metadata().get(pgwire::api::METADATA_DATABASE) {
		if let Some((ns, db_name)) = db.split_once('.') {
			session.ns = Some(ns.to_string());
			session.db = Some(db_name.to_string());
		} else {
			session.ns = Some("default".to_string());
			session.db = Some(db.to_string());
		}
	}
	session
}
