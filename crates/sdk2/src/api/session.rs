use anyhow::Result;
use uuid::Uuid;

use crate::api::SurrealContext;
use crate::controller::Controller;
use crate::events::Auth;
use crate::events::SessionEvents;
use crate::events::Using;
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
impl_events!(SurrealSession for SessionEvents);

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
		let this = Self {
			controller,
			session_id,
			publisher: Publisher::new(16),
		};
		this.pipe_controller_events();
		this
	}

	fn pipe_controller_events(&self) {
		let session_id = self.session_id;

		// Pipe Auth events, filtered to this session only
		self.controller.pipe_filtered::<Auth, _, _>(self.publisher.clone(), move |e| {
			e.session_id.is_some_and(|id| id == session_id)
		});

		// Pipe Using events, filtered to this session only
		self.controller.pipe_filtered::<Using, _, _>(self.publisher.clone(), move |e| {
			e.session_id.is_some_and(|id| id == session_id)
		});
	}
}
