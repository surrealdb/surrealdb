use crate::controller::Controller;
use uuid::Uuid;

pub(crate) trait SurrealContext {
	fn controller(&self) -> Controller;
	fn session_id(&self) -> Option<Uuid>;
	fn tx_id(&self) -> Option<Uuid>;
}
