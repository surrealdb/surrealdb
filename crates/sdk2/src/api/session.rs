use anyhow::Result;
use uuid::Uuid;

use crate::api::SurrealContext;
use crate::controller::Controller;
use crate::events::SessionEvents;
use crate::impl_events;
use crate::impl_queryable;
use crate::impl_session_controls;
use crate::utils::{Publisher, Subscribeable};

pub struct SurrealSession {
	publisher: Publisher<SessionEvents>,
	controller: Controller,
	session_id: Uuid,
}

impl_queryable!(SurrealSession);
impl_session_controls!(SurrealSession);
impl_events!(SurrealSession on publisher for SessionEvents);

impl Subscribeable<SessionEvents> for SurrealSession {
	fn publisher(&self) -> &Publisher<SessionEvents> {
		&self.publisher
	}
}

impl SurrealContext for SurrealSession {
	fn controller(&self) -> Controller {
		self.controller.clone()
	}

	fn session_id(&self) -> Option<Uuid> {
		Some(self.session_id)
	}

	fn tx_id(&self) -> Option<Uuid> {
		None
	}
}

impl SurrealSession {
	pub fn new(controller: Controller, session_id: Uuid) -> Self {
		Self {
			controller,
			session_id,
			publisher: Publisher::new(16),
		}
	}

	pub fn fork_session(&self) -> Result<Self> {
		todo!()
	}

	pub fn close_session(&self) -> Result<Self> {
		todo!()
	}
}
