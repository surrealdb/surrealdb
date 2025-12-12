use crate::api::{SurrealContext, SurrealSession};
use crate::events::{Auth, Connected, Connecting, Disconnected, Error, Reconnecting, Using};
use crate::utils::{ConstructableEngine, Subscribeable};
use crate::{controller::Controller, events::SurrealEvents, utils::Publisher};
use crate::{impl_events, impl_queryable, impl_session_controls};
use anyhow::Result;
use uuid::Uuid;

pub struct Surreal {
	controller: Controller,
	publisher: Publisher<SurrealEvents>,
}

impl_queryable!(Surreal);
impl_session_controls!(Surreal);
impl_events!(Surreal for SurrealEvents);

impl Subscribeable<SurrealEvents> for Surreal {
	fn publisher(&self) -> &Publisher<SurrealEvents> {
		&self.publisher
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
		let publisher = Publisher::new(16);
		let this = Self { controller, publisher };
		this.pipe_controller_events();
		this
	}

	fn pipe_controller_events(&self) {
		// Pipe through connection events directly
		self.controller.pipe::<Connecting, _>(self.publisher.clone());
		self.controller.pipe::<Connected, _>(self.publisher.clone());
		self.controller.pipe::<Reconnecting, _>(self.publisher.clone());
		self.controller.pipe::<Disconnected, _>(self.publisher.clone());
		self.controller.pipe::<Error, _>(self.publisher.clone());

		// Pipe Auth events, filtered to default session only
		self.controller.pipe_filtered::<Auth, _, _>(self.publisher.clone(), move |e| {
			e.session_id.is_none()
		});

		// Pipe Using events, filtered to default session only
		self.controller.pipe_filtered::<Using, _, _>(self.publisher.clone(), move |e| {
			e.session_id.is_none()
		});
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
