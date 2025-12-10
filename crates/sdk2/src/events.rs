use crate::event_set;

event_set! {
	pub SessionEvents {
		Auth {
			token: Option<String>,
		}
		Using {
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
