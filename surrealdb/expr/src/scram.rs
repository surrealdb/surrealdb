//! SCRAM verifiers derived from plaintext passwords.

use surrealdb_iam::ScramCredential;

/// The SCRAM verifier stored beside a user's password hash, so the user can
/// also authenticate over SCRAM (the PostgreSQL wire protocol).
///
/// `None` in a build without the `scram` feature: its host owns the wire, so
/// a user defined there stores no verifier and cannot authenticate over SCRAM
/// until a build with the feature sets the password again.
pub fn derive(password: &str) -> Option<ScramCredential> {
	#[cfg(feature = "scram")]
	{
		Some(ScramCredential::generate(password))
	}
	#[cfg(not(feature = "scram"))]
	{
		let _ = password;
		None
	}
}
