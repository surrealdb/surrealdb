use crate::controller::Controller;
use crate::{api::SurrealContext, impl_queryable};
use anyhow::Result;
use uuid::Uuid;

pub struct SurrealTransaction {
	controller: Controller,
	session_id: Option<Uuid>,
	tx_id: Uuid,
}

impl_queryable!(SurrealTransaction);
impl SurrealContext for SurrealTransaction {
	fn controller(&self) -> Controller {
		self.controller.clone()
	}

	fn session_id(&self) -> Option<Uuid> {
		self.session_id
	}

	fn tx_id(&self) -> Option<Uuid> {
		Some(self.tx_id)
	}
}

impl SurrealTransaction {
	pub fn new(controller: Controller, session_id: Option<Uuid>, tx_id: Uuid) -> Self {
		Self {
			controller,
			session_id,
			tx_id,
		}
	}

	pub async fn commit(self) -> Result<()> {
		let bridge = self.controller.bridge().await?;
		bridge.commit_transaction(self.session_id(), self.tx_id).await
	}

	pub async fn cancel(self) -> Result<()> {
		let bridge = self.controller.bridge().await?;
		bridge.cancel_transaction(self.session_id(), self.tx_id).await
	}
}
