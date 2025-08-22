use md5::Digest;
use revision::revisioned;
use sha2::Sha256;
use uuid::Uuid;

use crate::kvs::impl_kv_value_revisioned;
use crate::val::{Datetime, RecordId};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Subject {
	Record(RecordId),
	User(String),
}

impl Subject {
	// Returns the main identifier of a subject as a string.
	pub fn id(&self) -> String {
		match self {
			Subject::Record(id) => id.to_string(),
			Subject::User(name) => name.clone(),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Grant {
	Jwt(GrantJwt),
	Record(GrantRecord),
	Bearer(GrantBearer),
}

impl Grant {
	// Returns the type of the grant as a string.
	pub fn variant(&self) -> &str {
		match self {
			Grant::Jwt(_) => "jwt",
			Grant::Record(_) => "record",
			Grant::Bearer(_) => "bearer",
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GrantJwt {
	pub jti: Uuid,             // JWT ID
	pub token: Option<String>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GrantRecord {
	pub rid: Uuid,             // Record ID
	pub jti: Uuid,             // JWT ID
	pub token: Option<String>, // JWT. Will not be stored after being returned.
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GrantBearer {
	pub id: String, // Key ID
	// Key. Will not be stored and be returned as redacted.
	// Immediately after generation, it will contain the plaintext key.
	// Will be hashed before storage so that the plaintext key is not stored.
	pub key: String,
}

impl GrantBearer {
	pub fn hashed(self) -> Self {
		let mut hasher = Sha256::new();
		hasher.update(self.key.as_str());
		let hash = hasher.finalize();
		let hash_hex = format!("{hash:x}");

		Self {
			key: hash_hex,
			..self
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AccessGrant {
	pub id: String,                   // Unique grant identifier.
	pub ac: String,                   // Access method used to create the grant.
	pub creation: Datetime,           // Grant creation time.
	pub expiration: Option<Datetime>, // Grant expiration time, if any.
	pub revocation: Option<Datetime>, // Grant revocation time, if any.
	pub subject: Subject,             // Subject of the grant.
	pub grant: Grant,                 // Grant data.
}
impl_kv_value_revisioned!(AccessGrant);

impl AccessGrant {
	/// Returns a version of the statement where potential secrets are redacted.
	/// This function should be used when displaying the statement to datastore users.
	/// This function should NOT be used when displaying the statement for export purposes.
	pub fn redacted(mut self) -> AccessGrant {
		self.grant = match self.grant {
			Grant::Jwt(mut gr) => {
				// Token should not even be stored. We clear it just as a precaution.
				gr.token = None;
				Grant::Jwt(gr)
			}
			Grant::Record(mut gr) => {
				// Token should not even be stored. We clear it just as a precaution.
				gr.token = None;
				Grant::Record(gr)
			}
			Grant::Bearer(mut gr) => {
				// Key is stored, but should not usually be displayed.
				gr.key = "[REDACTED]".into();
				Grant::Bearer(gr)
			}
		};
		self
	}
}
