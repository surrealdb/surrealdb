use crate::events::{Connected, Connecting, Disconnected, Error, Reconnecting};

#[derive(Clone)]
pub enum ConnectionStatus {
	Disconnected,
	Connecting,
	Reconnecting,
	Connected(Connected)
}

impl ConnectionStatus {
	pub fn is_connected(&self) -> bool {
		matches!(self, ConnectionStatus::Connected(_))
	}
	pub fn is_connecting(&self) -> bool {
		matches!(self, ConnectionStatus::Connecting)
	}
	pub fn is_reconnecting(&self) -> bool {
		matches!(self, ConnectionStatus::Reconnecting)
	}
	pub fn is_disconnected(&self) -> bool {
		matches!(self, ConnectionStatus::Disconnected)
	}
}

impl From<Connected> for ConnectionStatus {
	fn from(event: Connected) -> Self {
		ConnectionStatus::Connected(event)
	}
}

impl From<Disconnected> for ConnectionStatus {
	fn from(_: Disconnected) -> Self {
		ConnectionStatus::Disconnected
	}
}

impl From<Error> for ConnectionStatus {
	fn from(_: Error) -> Self {
		ConnectionStatus::Disconnected
	}
}

impl From<Reconnecting> for ConnectionStatus {
	fn from(_: Reconnecting) -> Self {
		ConnectionStatus::Reconnecting
	}
}

impl From<Connecting> for ConnectionStatus {
	fn from(_: Connecting) -> Self {
		ConnectionStatus::Connecting
	}
}