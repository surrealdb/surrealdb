//! Postgres SASL server-side exchange for SCRAM-SHA-256 (RFC 5802 / 7677).
//!
//! The cryptographic core lives in `surrealdb_iam::scram`, reached here
//! through [`ScramAuth`]; this module owns the SCRAM message parsing/formatting,
//! server-nonce generation, and the AuthMessage assembly Postgres SASL requires.
//! Channel binding is not offered (plain `SCRAM-SHA-256`, not `-PLUS`).

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::TryRngCore;
use surrealdb_core::iam::verify::ScramAuth;
use surrealdb_iam::scram;

use super::error::PgError;

/// The only SASL mechanism the listener offers.
pub(super) const MECHANISM: &str = scram::MECHANISM;

/// Length in bytes of the random server nonce (before base64).
const SERVER_NONCE_LEN: usize = 18;

/// A SCRAM-SHA-256 exchange in progress, holding the state needed to assemble
/// the final AuthMessage across the two client round-trips.
pub(super) struct ScramExchange {
	client_first_bare: String,
	server_first: String,
	nonce: String,
}

impl ScramExchange {
	/// Process the client-first message and produce the server-first message
	/// (`r=<combined-nonce>,s=<salt>,i=<iterations>`) from the verifier's salt
	/// and iteration count.
	pub(super) fn start(
		client_first: &[u8],
		salt: &[u8],
		iterations: u32,
	) -> Result<(Self, String), PgError> {
		let client_first = std::str::from_utf8(client_first)
			.map_err(|_| PgError::protocol("SCRAM client-first is not valid UTF-8"))?;
		// The gs2 cbind-flag is the first comma-delimited field: `n` (client uses
		// no channel binding), `y` (client supports it but the server did not
		// advertise `-PLUS`), or `p=<name>` (client requires it). We offer only
		// plain SCRAM-SHA-256, so per RFC 5802 §5 only `n` is acceptable; `y` and
		// `p=` are rejected to preserve downgrade protection.
		if client_first.split(',').next() != Some("n") {
			return Err(PgError::protocol(
				"SCRAM channel binding is not supported; expected the 'n' gs2 flag",
			));
		}
		let bare = client_first_bare(client_first)
			.ok_or_else(|| PgError::protocol("malformed SCRAM client-first message"))?;
		let client_nonce =
			attr(bare, 'r').ok_or_else(|| PgError::protocol("SCRAM client-first missing nonce"))?;
		let nonce = format!("{client_nonce}{}", generate_nonce()?);
		let server_first = format!("r={nonce},s={},i={iterations}", STANDARD.encode(salt));
		Ok((
			Self {
				client_first_bare: bare.to_string(),
				server_first: server_first.clone(),
				nonce,
			},
			server_first,
		))
	}

	/// Verify the client-final message against `scram` and produce the
	/// server-final message (`v=<server-signature>`). Returns an error (mapped to
	/// a failed auth) when the nonce or client proof does not verify.
	pub(super) fn finish(&self, client_final: &[u8], scram: &ScramAuth) -> Result<String, PgError> {
		let (proof, auth_message) = self.client_final_parts(client_final)?;
		if !scram.verify_client_proof(auth_message.as_bytes(), &proof) {
			return Err(PgError::invalid_password("SCRAM authentication failed"));
		}
		let signature = scram.server_signature(auth_message.as_bytes());
		Ok(format!("v={}", STANDARD.encode(signature)))
	}

	/// Parse the client-final message: check that it echoes our combined nonce,
	/// decode its proof, and assemble the AuthMessage
	/// (`client-first-bare , server-first , client-final-without-proof`). Kept
	/// separate from the cryptographic check so the wire assembly is testable.
	fn client_final_parts(&self, client_final: &[u8]) -> Result<(Vec<u8>, String), PgError> {
		let client_final = std::str::from_utf8(client_final)
			.map_err(|_| PgError::protocol("SCRAM client-final is not valid UTF-8"))?;
		let echoed = attr(client_final, 'r')
			.ok_or_else(|| PgError::protocol("SCRAM client-final missing nonce"))?;
		if echoed != self.nonce {
			return Err(PgError::invalid_password("SCRAM nonce mismatch"));
		}
		let proof_b64 = attr(client_final, 'p')
			.ok_or_else(|| PgError::protocol("SCRAM client-final missing proof"))?;
		let proof = STANDARD
			.decode(proof_b64)
			.map_err(|_| PgError::protocol("SCRAM client proof is not valid base64"))?;
		// client-final-without-proof is everything up to the trailing `,p=`.
		let without_proof = client_final
			.rsplit_once(",p=")
			.map(|(head, _)| head)
			.ok_or_else(|| PgError::protocol("malformed SCRAM client-final message"))?;
		let auth_message =
			format!("{},{},{}", self.client_first_bare, self.server_first, without_proof);
		Ok((proof, auth_message))
	}
}

/// The client-first-bare: everything after the gs2 header
/// `<cbind-flag>,<authzid>,`.
fn client_first_bare(msg: &str) -> Option<&str> {
	let mut fields = msg.splitn(3, ',');
	let _cbind = fields.next()?;
	let _authzid = fields.next()?;
	fields.next()
}

/// The value of a SCRAM attribute `<key>=<value>` within a comma-separated
/// message, or `None` if absent.
fn attr(msg: &str, key: char) -> Option<&str> {
	msg.split(',').find_map(|field| {
		let mut chars = field.chars();
		(chars.next()? == key && chars.next()? == '=').then(|| &field[2..])
	})
}

/// A fresh server nonce: comma-free printable ASCII (base64 of random bytes),
/// drawn from OS entropy. Fails closed if the OS RNG is unavailable rather than
/// falling back to a non-cryptographic source, since the nonce underpins the
/// exchange's replay and downgrade protection.
fn generate_nonce() -> Result<String, PgError> {
	let mut bytes = [0u8; SERVER_NONCE_LEN];
	rand::rngs::OsRng
		.try_fill_bytes(&mut bytes)
		.map_err(|_| PgError::internal("failed to obtain secure randomness for the SCRAM nonce"))?;
	Ok(STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_client_first() {
		assert_eq!(client_first_bare("n,,n=user,r=abc"), Some("n=user,r=abc"));
		assert_eq!(attr("n=user,r=abc", 'r'), Some("abc"));
		assert_eq!(attr("n=user,r=abc", 'z'), None);
		// A base64 proof value (with `+`/`/`/`=`) is returned intact.
		assert_eq!(attr("c=biws,r=xy,p=aB+/c9==", 'p'), Some("aB+/c9=="));
	}

	#[test]
	fn start_builds_server_first() {
		let (exchange, server_first) =
			ScramExchange::start(b"n,,n=user,r=clientNONCE", &[0u8; 16], 4096).unwrap();
		assert!(server_first.starts_with("r=clientNONCE"), "server nonce must extend the client's");
		assert!(server_first.contains(",i=4096"));
		assert_eq!(exchange.client_first_bare, "n=user,r=clientNONCE");
	}

	#[test]
	fn rejects_channel_binding_flags() {
		// We advertise plain SCRAM-SHA-256, so a client signalling channel
		// binding (`y` = supported-but-not-advertised, `p=` = required) must be
		// rejected rather than silently accepted (a downgrade signal).
		for header in ["y,,n=user,r=abc", "p=tls-server-end-point,,n=user,r=abc"] {
			assert!(
				ScramExchange::start(header.as_bytes(), &[0u8; 16], 4096).is_err(),
				"expected {header:?} to be rejected"
			);
		}
		// The `n` flag (no channel binding) is accepted.
		assert!(ScramExchange::start(b"n,,n=user,r=abc", &[0u8; 16], 4096).is_ok());
	}

	/// RFC 7677 §3 worked example (user "user", password "pencil"): with the
	/// example's fixed nonce, the client-final message must yield exactly the
	/// example's AuthMessage and decode to the example's proof. Cross-checks the
	/// nonce-echo check and AuthMessage assembly against a known vector.
	#[test]
	fn rfc7677_client_final_assembly() {
		let nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
		let server_first = format!("r={nonce},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096");
		let exchange = ScramExchange {
			client_first_bare: "n=user,r=rOprNGfwEbeRWgbNEkqO".to_string(),
			server_first: server_first.clone(),
			nonce: nonce.to_string(),
		};
		let client_final =
			format!("c=biws,r={nonce},p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=");
		let (proof, auth_message) = exchange.client_final_parts(client_final.as_bytes()).unwrap();
		assert_eq!(
			auth_message,
			format!("n=user,r=rOprNGfwEbeRWgbNEkqO,{server_first},c=biws,r={nonce}")
		);
		assert_eq!(proof, STANDARD.decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=").unwrap());

		// A mismatched echoed nonce is rejected before proof decoding.
		let bad = "c=biws,r=someoneelse,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
		assert!(exchange.client_final_parts(bad.as_bytes()).is_err());
	}
}
