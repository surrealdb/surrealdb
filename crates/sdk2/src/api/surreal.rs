use crate::api::{SurrealContext, SurrealSession};
use crate::utils::{ConstructableEngine, Subscribeable};
use crate::{controller::Controller, events::SurrealEvents, utils::Publisher};
use crate::{impl_events, impl_queryable, impl_session_controls};
use anyhow::Result;
use uuid::Uuid;

pub struct Surreal {
	controller: Controller,
}

impl_queryable!(Surreal);
impl_session_controls!(Surreal);
impl_events!(Surreal for SurrealEvents);

impl Subscribeable<SurrealEvents> for Surreal {
	fn publisher(&self) -> &Publisher<SurrealEvents> {
		self.controller.publisher()
	}
}

impl SurrealContext for Surreal {
	fn controller(&self) -> Controller {
		self.controller.clone()
	}

	fn session_id(&self) -> Option<Uuid> {
		None
	}

	fn tx_id(&self) -> Option<Uuid> {
		None
	}
}

impl Surreal {
	pub fn new() -> Self {
		let controller = Controller::new();
		Self { controller }
	}

	pub fn attach_engine<E: ConstructableEngine>(mut self) -> Self {
		self.controller.attach_engine::<E>();
		self
	}

	pub async fn connect(&self, url: &str) -> Result<()> {
		self.controller.connect(url).await
	}

    pub async fn new_session(&self) -> Result<SurrealSession> {
        Ok(self.controller().new_session())
    }
}
