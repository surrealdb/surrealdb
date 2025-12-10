use crate::api::{SurrealContext, SurrealTransaction};
use crate::method::Request;
use crate::method::Use;
use anyhow::Result;

pub(crate) trait SessionControls: SurrealContext
where
	Self: Sized,
{
	#[inline]
	async fn begin_transaction(&self) -> Result<SurrealTransaction> {
		let bridge = self.controller().bridge().await?;
		let tx_id = bridge.begin_transaction(self.session_id()).await?;
		Ok(SurrealTransaction::new(self.controller().clone(), self.session_id(), tx_id))
	}

	#[inline]
	fn r#use(&self) -> Request<Use> {
		Request::new(self, Use)
	}

	#[inline]
	fn set(&self) -> Result<()> {
		todo!()
	}

	#[inline]
	fn unset(&self) -> Result<()> {
		todo!()
	}

	#[inline]
	fn signup(&self) -> Result<Self> {
		todo!()
	}

	#[inline]
	fn signin(&self) -> Result<Self> {
		todo!()
	}

	#[inline]
	fn authenticate(&self) -> Result<Self> {
		todo!()
	}

	#[inline]
	fn invalidate(&self) -> Result<Self> {
		todo!()
	}
}
