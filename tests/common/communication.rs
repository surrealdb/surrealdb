use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Communication {
	Text(String),
	Binary(Vec<u8>),
}
