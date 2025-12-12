use crate::event_set;
use uuid::Uuid;

event_set! {
	pub SessionEvents {
		Auth {
			session_id: Option<Uuid>,
			token: Option<String>,
		}
		Using {
			session_id: Option<Uuid>,
			namespace: Option<String>,
			database: Option<String>,
		}
	}
}

event_set! {
	pub SurrealEvents extends SessionEvents {
		Connecting {}
		Connected {
			version: String,
		}
		Reconnecting {}
		Disconnected {}
		Error {
			message: String,
		}
	}
}

event_set! {
	pub EngineEvents {
		EngineConnected {}
		EngineReconnecting {}
		EngineDisconnected {}
		EngineError { message: String }
	}
}
