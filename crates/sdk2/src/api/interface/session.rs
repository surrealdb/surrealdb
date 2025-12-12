use crate::api::{SurrealContext, SurrealSession, SurrealTransaction};
use crate::auth::{AccessRecordAuth, AuthParams};
use crate::method::{NullableString, Request, UseDefaults, UseNamespaceDatabase};
use anyhow::Result;
use surrealdb_types::{SurrealValue, Tokens};

pub(crate) trait SessionControls: SurrealContext
where
	Self: Sized,
{
	#[inline]
	async fn begin_transaction(&self) -> Result<SurrealTransaction> {
		let tx_id = self.controller().begin_transaction(self.session_id()).await?;
		Ok(SurrealTransaction::new(self.controller().clone(), self.session_id(), tx_id))
	}

	#[inline]
	fn use_ns<T: Into<NullableString>>(&self, namespace: T) -> Request<UseNamespaceDatabase> {
		UseNamespaceDatabase::req_from_namespace(self, namespace)
	}

	#[inline]
	fn use_db<T: Into<NullableString>>(&self, database: T) -> Request<UseNamespaceDatabase> {
		UseNamespaceDatabase::req_from_database(self, database)
	}

	#[inline]
	fn use_defaults(&self) -> Request<UseDefaults> {
		Request::new(self, UseDefaults)
	}

	#[inline]
	async fn set<N: Into<String>, V: SurrealValue>(&self, name: N, value: V) -> Result<()> {
		self.controller().set_variable(self.session_id(), name.into(), value.into_value()).await
	}

	#[inline]
	async fn unset<N: Into<String>>(&self, name: N) -> Result<()> {
		self.controller().drop_variable(self.session_id(), name.into()).await
	}

	#[inline]
	async fn signup(&self, credentials: AccessRecordAuth) -> Result<Tokens> {
		let params: AuthParams = credentials.into();
		self.controller().signup(self.session_id(), params.into_vars()).await
	}

	#[inline]
	async fn signin<T: Into<AuthParams>>(&self, credentials: T) -> Result<Tokens> {
		let params: AuthParams = credentials.into();
		self.controller().signin(self.session_id(), params.into_vars()).await
	}

	#[inline]
	async fn authenticate<T: Into<Tokens>>(&self, tokens: T) -> Result<Tokens> {
		let tokens: Tokens = tokens.into();
		if tokens.refresh.is_some() {
			return self.controller().refresh(self.session_id(), tokens).await
		} 

		if let Some(access) = tokens.access {
			self.controller().authenticate(self.session_id(), access.clone()).await?;
			return Ok(Tokens {
				access: Some(access),
				refresh: None,
			})
		}

		Ok(tokens)
	}

	#[inline]
	async fn invalidate(&self) -> Result<()> {
		self.controller().invalidate(self.session_id()).await
	}

	#[inline]
	async fn fork_session(&self) -> Result<SurrealSession> {
		self.controller().fork_session(self.session_id()).await
	}
}
