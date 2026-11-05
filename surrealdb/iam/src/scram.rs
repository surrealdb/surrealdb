//! SCRAM-SHA-256 credential derivation and verification (RFC 5802 / RFC 7677).
//!
//! SurrealDB stores an Argon2 hash for its own signin path, but that hash is not
//! usable material for the SCRAM challenge-response exchange spoken by, for
//! example, Postgres wire-protocol clients. To support SCRAM we additionally
//! derive and persist SCRAM verifier material ([`ScramCredential`]) whenever a
//! user is defined with a plaintext password.
//!
//! This module is deliberately **transport-agnostic**: it only performs the
//! cryptographic core given a caller-assembled `AuthMessage`. The caller (e.g.
//! the Postgres SASL handler) owns nonce generation, the wire framing, base64 of
//! the exchanged messages, and any channel binding.
//!
//! ## Password normalization
//!
//! Passwords are normalized with SASLprep (RFC 4013) before key derivation. If
//! SASLprep fails (e.g. the password contains prohibited code points), we fall
//! back to the raw UTF-8 bytes, matching PostgreSQL's `pg_saslprep` behavior.
//! This keeps derivation infallible. Because the same fallback runs on both the
//! verifier-generation side here and on a PostgreSQL-compatible client (libpq
//! applies the identical fallback), such passwords still authenticate over the
//! Postgres wire protocol; only a strict RFC 5802 client that fails closed on a
//! prohibited code point would compute a different `SaltedPassword`.

use std::borrow::Cow;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use revision::revisioned;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// The mechanism name for the only SCRAM variant we support.
pub const MECHANISM: &str = "SCRAM-SHA-256";

/// Default PBKDF2 iteration count. Matches PostgreSQL's default and satisfies
/// the RFC 7677 recommended minimum of 4096.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// Upper bound on the accepted iteration count for an imported verifier. RFC
/// 7677 sets no maximum, but the count is handed to the client, which runs
/// PBKDF2 for that many rounds; an unbounded value (e.g. `u32::MAX`) would let a
/// malicious or corrupt verifier stall any client that authenticates against it.
/// 16M is far above any sane deployment while still bounding the work.
pub const MAX_ITERATIONS: u32 = 1 << 24;

/// Length in bytes of the randomly generated per-user salt.
const SALT_LEN: usize = 16;

/// Output length of SHA-256 / HMAC-SHA-256, in bytes.
const KEY_LEN: usize = 32;

/// Error returned when parsing a SCRAM verifier string fails.
#[derive(Debug, thiserror::Error)]
pub enum ScramParseError {
	#[error("invalid SCRAM verifier: {0}")]
	Malformed(&'static str),
	#[error("invalid SCRAM verifier: unsupported mechanism")]
	Mechanism,
	#[error("invalid SCRAM verifier: iteration count is not a valid integer")]
	Iterations,
	#[error(
		"invalid SCRAM verifier: iteration count out of range (must be between {DEFAULT_ITERATIONS} and {MAX_ITERATIONS})"
	)]
	IterationsOutOfRange,
	#[error("invalid SCRAM verifier: base64 decode failed")]
	Base64,
	#[error("invalid SCRAM verifier: salt or key has an invalid length")]
	InvalidLength,
}

/// Compute `HMAC-SHA-256(key, data)`.
///
/// Infallible: HMAC accepts a key of any length.
fn hmac(key: &[u8], data: &[u8]) -> [u8; KEY_LEN] {
	let mut mac =
		<Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts a key of any length");
	mac.update(data);
	let out = mac.finalize().into_bytes();
	let mut buf = [0u8; KEY_LEN];
	buf.copy_from_slice(&out);
	buf
}

/// Compute `SHA-256(data)`.
fn sha256(data: &[u8]) -> [u8; KEY_LEN] {
	let out = Sha256::digest(data);
	let mut buf = [0u8; KEY_LEN];
	buf.copy_from_slice(&out);
	buf
}

/// Apply SASLprep (RFC 4013), falling back to the raw password on error.
fn saslprep(password: &str) -> Cow<'_, str> {
	match stringprep::saslprep(password) {
		Ok(p) => p,
		Err(_) => Cow::Borrowed(password),
	}
}

/// SCRAM-SHA-256 verifier material for a user.
///
/// This is stored alongside the Argon2 `hash` on `UserDefinition` so that
/// transports which negotiate SCRAM (e.g. the Postgres wire protocol) can
/// authenticate a user without SurrealDB ever holding the plaintext password.
///
/// The mechanism is fixed to SCRAM-SHA-256, so it is not stored. See
/// this module for the derivation and verification logic.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ScramCredential {
	/// PBKDF2 iteration count used to derive the salted password.
	pub iterations: u32,
	/// Random per-user salt.
	pub salt: Vec<u8>,
	/// `H(HMAC(SaltedPassword, "Client Key"))` — used to verify a client proof.
	pub stored_key: Vec<u8>,
	/// `HMAC(SaltedPassword, "Server Key")` — used to sign the server's final message.
	pub server_key: Vec<u8>,
}

impl ScramCredential {
	/// Derive a fresh SCRAM-SHA-256 credential for `password` using
	/// [`DEFAULT_ITERATIONS`] and a random salt.
	pub fn generate(password: &str) -> Self {
		let mut salt = [0u8; SALT_LEN];
		OsRng.fill_bytes(&mut salt);
		Self::generate_with(password, &salt, DEFAULT_ITERATIONS)
	}

	/// Derive a SCRAM-SHA-256 credential for `password` with an explicit salt
	/// and iteration count. Deterministic — used for tests and for re-deriving a
	/// known credential.
	pub fn generate_with(password: &str, salt: &[u8], iterations: u32) -> Self {
		let normalized = saslprep(password);
		// SaltedPassword := PBKDF2(HMAC, password, salt, i)
		let mut salted = [0u8; KEY_LEN];
		pbkdf2::pbkdf2_hmac::<Sha256>(normalized.as_bytes(), salt, iterations, &mut salted);
		// ClientKey := HMAC(SaltedPassword, "Client Key")
		let client_key = hmac(&salted, b"Client Key");
		// StoredKey := H(ClientKey)
		let stored_key = sha256(&client_key);
		// ServerKey := HMAC(SaltedPassword, "Server Key")
		let server_key = hmac(&salted, b"Server Key");
		Self {
			iterations,
			salt: salt.to_vec(),
			stored_key: stored_key.to_vec(),
			server_key: server_key.to_vec(),
		}
	}

	/// Verify a client proof against this credential for the given `AuthMessage`.
	///
	/// Computes `ClientSignature = HMAC(StoredKey, AuthMessage)`, recovers
	/// `ClientKey = ClientProof XOR ClientSignature`, and checks in constant time
	/// that `H(ClientKey) == StoredKey`.
	pub fn verify_client_proof(&self, auth_message: &[u8], client_proof: &[u8]) -> bool {
		let client_signature = hmac(&self.stored_key, auth_message);
		if client_proof.len() != client_signature.len() {
			return false;
		}
		let recovered: Vec<u8> =
			client_proof.iter().zip(client_signature.iter()).map(|(p, s)| p ^ s).collect();
		let computed = sha256(&recovered);
		computed.ct_eq(&self.stored_key).into()
	}

	/// Compute the server signature `HMAC(ServerKey, AuthMessage)` for the
	/// server-final message.
	pub fn server_signature(&self, auth_message: &[u8]) -> [u8; KEY_LEN] {
		hmac(&self.server_key, auth_message)
	}

	/// Render this credential in PostgreSQL's SCRAM verifier string format:
	/// `SCRAM-SHA-256$<iterations>:<b64 salt>$<b64 stored_key>:<b64 server_key>`.
	pub fn to_verifier_string(&self) -> String {
		format!(
			"{MECHANISM}${}:{}${}:{}",
			self.iterations,
			STANDARD.encode(&self.salt),
			STANDARD.encode(&self.stored_key),
			STANDARD.encode(&self.server_key),
		)
	}

	/// Parse a PostgreSQL-format SCRAM verifier string. This is the only fallible
	/// entry point; callers validate at parse/import time so downstream
	/// conversions stay infallible.
	pub fn from_verifier_string(s: &str) -> Result<Self, ScramParseError> {
		// SCRAM-SHA-256$<iter>:<salt>$<stored>:<server>
		let (mechanism, rest) =
			s.split_once('$').ok_or(ScramParseError::Malformed("missing mechanism separator"))?;
		if mechanism != MECHANISM {
			return Err(ScramParseError::Mechanism);
		}
		let (iter_salt, keys) =
			rest.split_once('$').ok_or(ScramParseError::Malformed("missing key separator"))?;
		let (iterations, salt) = iter_salt
			.split_once(':')
			.ok_or(ScramParseError::Malformed("missing salt separator"))?;
		let (stored_key, server_key) = keys
			.split_once(':')
			.ok_or(ScramParseError::Malformed("missing server-key separator"))?;

		let iterations: u32 = iterations.parse().map_err(|_| ScramParseError::Iterations)?;
		// Reject counts below the RFC 7677 minimum (a downgrade — `i=0` would be a
		// degenerate KDF) or above a sane ceiling (a client-side DoS lever).
		if !(DEFAULT_ITERATIONS..=MAX_ITERATIONS).contains(&iterations) {
			return Err(ScramParseError::IterationsOutOfRange);
		}
		let salt = STANDARD.decode(salt).map_err(|_| ScramParseError::Base64)?;
		let stored_key = STANDARD.decode(stored_key).map_err(|_| ScramParseError::Base64)?;
		let server_key = STANDARD.decode(server_key).map_err(|_| ScramParseError::Base64)?;

		// StoredKey and ServerKey are SHA-256 / HMAC-SHA-256 outputs, so they must
		// be exactly KEY_LEN bytes; a shorter (but base64-valid) key would parse
		// yet never verify (the constant-time compare fails on a length mismatch),
		// silently producing a user that cannot authenticate via SCRAM. The salt
		// must be present.
		if salt.is_empty() || stored_key.len() != KEY_LEN || server_key.len() != KEY_LEN {
			return Err(ScramParseError::InvalidLength);
		}

		Ok(Self {
			iterations,
			salt,
			stored_key,
			server_key,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Build the AuthMessage the way a real exchange would, then derive a client
	/// proof from a known SaltedPassword so we can drive `verify_client_proof`.
	fn client_proof(password: &str, cred: &ScramCredential, auth_message: &[u8]) -> Vec<u8> {
		let normalized = saslprep(password);
		let mut salted = [0u8; KEY_LEN];
		pbkdf2::pbkdf2_hmac::<Sha256>(
			normalized.as_bytes(),
			&cred.salt,
			cred.iterations,
			&mut salted,
		);
		let client_key = hmac(&salted, b"Client Key");
		let client_signature = hmac(&cred.stored_key, auth_message);
		client_key.iter().zip(client_signature.iter()).map(|(k, s)| k ^ s).collect()
	}

	#[test]
	fn verifier_string_round_trips() {
		let cred = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096);
		let s = cred.to_verifier_string();
		assert!(s.starts_with("SCRAM-SHA-256$4096:"));
		let parsed = ScramCredential::from_verifier_string(&s).unwrap();
		assert_eq!(cred, parsed);
	}

	#[test]
	fn generate_with_is_deterministic() {
		let a = ScramCredential::generate_with("secret", b"same-salt-000000", 4096);
		let b = ScramCredential::generate_with("secret", b"same-salt-000000", 4096);
		assert_eq!(a, b);
		assert_eq!(a.iterations, 4096);
		assert_eq!(a.stored_key.len(), KEY_LEN);
		assert_eq!(a.server_key.len(), KEY_LEN);
	}

	#[test]
	fn generate_uses_random_salt() {
		let a = ScramCredential::generate("secret");
		let b = ScramCredential::generate("secret");
		assert_ne!(a.salt, b.salt);
		assert_eq!(a.salt.len(), SALT_LEN);
	}

	#[test]
	fn full_exchange_verifies() {
		let cred = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096);
		let auth_message = b"n=user,r=clientnonce,s=salt,i=4096,c=biws,r=fullnonce";
		let proof = client_proof("pencil", &cred, auth_message);
		assert!(cred.verify_client_proof(auth_message, &proof));

		// Server signature is deterministic for a given AuthMessage.
		let sig = cred.server_signature(auth_message);
		assert_eq!(sig, hmac(&cred.server_key, auth_message));
	}

	#[test]
	fn wrong_proof_is_rejected() {
		let cred = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096);
		let auth_message = b"n=user,r=clientnonce,s=salt,i=4096,c=biws,r=fullnonce";
		let mut proof = client_proof("pencil", &cred, auth_message);
		proof[0] ^= 0xff;
		assert!(!cred.verify_client_proof(auth_message, &proof));
		// A proof of the wrong length is rejected too.
		assert!(!cred.verify_client_proof(auth_message, &proof[..10]));
	}

	#[test]
	fn wrong_password_does_not_verify() {
		let cred = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096);
		let auth_message = b"n=user,r=clientnonce,s=salt,i=4096,c=biws,r=fullnonce";
		let proof = client_proof("crayon", &cred, auth_message);
		assert!(!cred.verify_client_proof(auth_message, &proof));
	}

	#[test]
	fn malformed_verifier_strings_error() {
		assert!(ScramCredential::from_verifier_string("not-a-verifier").is_err());
		assert!(ScramCredential::from_verifier_string("SCRAM-SHA-1$4096:AA==$AA==:AA==").is_err());
		assert!(ScramCredential::from_verifier_string("SCRAM-SHA-256$abc:AA==$AA==:AA==").is_err());
		assert!(ScramCredential::from_verifier_string("SCRAM-SHA-256$4096:@@@$AA==:AA==").is_err());
		assert!(ScramCredential::from_verifier_string("SCRAM-SHA-256$4096:AA==").is_err());
	}

	#[test]
	fn known_answer_vector() {
		// Externally-computed verifier for password "pencil", salt
		// "0123456789abcdef" (raw 16 bytes), 4096 iterations. Pinning the exact
		// string (not just a self round-trip) is what guards against a derivation
		// regression — e.g. swapping the "Client Key"/"Server Key" HMAC labels or
		// mis-wiring PBKDF2 would still round-trip but produce different keys, and
		// real Postgres-wire SCRAM clients would silently stop authenticating.
		// This is the same value imported by the `scram.surql` language test; if it
		// ever needs regenerating, cross-check against a Postgres
		// `SCRAM-SHA-256$...` verifier for the same inputs.
		const KAT: &str = "SCRAM-SHA-256$4096:MDEyMzQ1Njc4OWFiY2RlZg==$nQpbZ77WudtqufPwikHXGRt6g2QJ4zns8bZLw273DRM=:jn2amWP1q1h+jgjy0YTO14S6/F02SV7taipOeB7ef20=";
		let cred = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096);
		assert_eq!(cred.to_verifier_string(), KAT);
		// Re-deriving from the emitted verifier string must reproduce the keys.
		let again = ScramCredential::from_verifier_string(&cred.to_verifier_string()).unwrap();
		assert_eq!(cred.stored_key, again.stored_key);
		assert_eq!(cred.server_key, again.server_key);
		// And a proof built from the same password verifies against the stored key.
		let auth = b"test-auth-message";
		assert!(cred.verify_client_proof(auth, &client_proof("pencil", &cred, auth)));
	}

	#[test]
	fn saslprep_failure_falls_back_to_raw_and_stays_self_consistent() {
		// U+0007 (BEL) is a prohibited control code point, so SASLprep errors and
		// derivation falls back to the raw bytes. The credential must still be
		// internally consistent: a proof derived with the same fallback verifies.
		let prohibited = "pass\u{0007}word";
		assert!(stringprep::saslprep(prohibited).is_err());
		let cred = ScramCredential::generate_with(prohibited, b"0123456789abcdef", 4096);
		let auth = b"n=user,r=nonce";
		assert!(cred.verify_client_proof(auth, &client_proof(prohibited, &cred, auth)));
	}

	#[test]
	fn from_verifier_string_rejects_bad_iterations_and_lengths() {
		// A valid template we mutate one field at a time.
		let ok = ScramCredential::generate_with("pencil", b"0123456789abcdef", 4096)
			.to_verifier_string();
		assert!(ScramCredential::from_verifier_string(&ok).is_ok());

		// Iteration count below the RFC 7677 minimum (including 0) is rejected.
		assert!(matches!(
			ScramCredential::from_verifier_string(
				"SCRAM-SHA-256$0:MDEyMzQ1Njc4OWFiY2RlZg==$nQpbZ77WudtqufPwikHXGRt6g2QJ4zns8bZLw273DRM=:jn2amWP1q1h+jgjy0YTO14S6/F02SV7taipOeB7ef20="
			),
			Err(ScramParseError::IterationsOutOfRange)
		));
		// Iteration count above the ceiling is rejected.
		assert!(matches!(
			ScramCredential::from_verifier_string(
				"SCRAM-SHA-256$4294967295:MDEyMzQ1Njc4OWFiY2RlZg==$nQpbZ77WudtqufPwikHXGRt6g2QJ4zns8bZLw273DRM=:jn2amWP1q1h+jgjy0YTO14S6/F02SV7taipOeB7ef20="
			),
			Err(ScramParseError::IterationsOutOfRange)
		));
		// Base64-valid but wrong-length keys are rejected (would never verify).
		assert!(matches!(
			ScramCredential::from_verifier_string("SCRAM-SHA-256$4096:AA==$AA==:AA=="),
			Err(ScramParseError::InvalidLength)
		));
	}
}
