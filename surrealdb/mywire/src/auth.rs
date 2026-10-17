//! Authentication mechanisms for the MySQL wire protocol.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rand::TryRngCore;
use rand::rngs::OsRng;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use surrealdb_iam::Auth;

pub const AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
pub const AUTH_PLUGIN_CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
pub const AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";

/// Generate a 20-byte random challenge salt for MySQL authentication.
pub fn generate_auth_salt() -> [u8; 20] {
	let mut salt = [0u8; 20];
	if OsRng.try_fill_bytes(&mut salt).is_err() {
		for b in salt.iter_mut() {
			*b = (rand::random::<u32>() & 0xff) as u8;
		}
	}
	// Avoid null bytes in salt which could terminate strings prematurely
	for b in salt.iter_mut() {
		if *b == 0 {
			*b = 0x7f;
		}
	}
	salt
}

/// Verify a client scramble using mysql_native_password against a plaintext password.
///
/// mysql_native_password computes:
///   stage1 = SHA1(password)
///   stage2 = SHA1(stage1)
///   scramble = stage1 XOR SHA1(salt + stage2)
pub fn verify_mysql_native_password(scramble: &[u8], salt: &[u8], password: &str) -> bool {
	if scramble.is_empty() {
		return password.is_empty();
	}
	if scramble.len() != 20 || salt.len() < 20 {
		return false;
	}

	let stage1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
	let stage2: [u8; 20] = Sha1::digest(stage1).into();

	let mut hasher = Sha1::new();
	hasher.update(&salt[..20]);
	hasher.update(stage2);
	let candidate_xor: [u8; 20] = hasher.finalize().into();

	let mut expected_scramble = [0u8; 20];
	for i in 0..20 {
		expected_scramble[i] = stage1[i] ^ candidate_xor[i];
	}

	scramble == expected_scramble
}

/// Verify a client scramble using mysql_native_password against a stored stage2 verifier.
///
/// If stage2 = SHA1(SHA1(password)), then:
///   candidate_stage1 = scramble XOR SHA1(salt + stage2)
///   valid if SHA1(candidate_stage1) == stage2
pub fn verify_mysql_native_password_fast_auth(
	scramble: &[u8],
	salt: &[u8],
	stage2: &[u8; 20],
) -> bool {
	if scramble.len() != 20 || salt.len() < 20 {
		return false;
	}

	let mut hasher = Sha1::new();
	hasher.update(&salt[..20]);
	hasher.update(stage2);
	let candidate_xor: [u8; 20] = hasher.finalize().into();

	let mut candidate_stage1 = [0u8; 20];
	for i in 0..20 {
		candidate_stage1[i] = scramble[i] ^ candidate_xor[i];
	}

	let candidate_stage2: [u8; 20] = Sha1::digest(candidate_stage1).into();
	candidate_stage2 == *stage2
}

/// Computes the expected scramble given a plaintext password and salt for mysql_native_password.
pub fn compute_mysql_native_password_scramble(password: &str, salt: &[u8]) -> [u8; 20] {
	if password.is_empty() {
		return [0u8; 20];
	}
	let stage1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
	let stage2: [u8; 20] = Sha1::digest(stage1).into();

	let mut hasher = Sha1::new();
	hasher.update(&salt[..salt.len().min(20)]);
	hasher.update(stage2);
	let candidate_xor: [u8; 20] = hasher.finalize().into();

	let mut scramble = [0u8; 20];
	for i in 0..20 {
		scramble[i] = stage1[i] ^ candidate_xor[i];
	}
	scramble
}

/// Computes the expected scramble given a plaintext password and salt for caching_sha2_password.
///
/// caching_sha2_password computes:
///   stage1 = SHA256(password)
///   stage2 = SHA256(stage1)
///   stage3 = SHA256(stage2 + salt)
///   scramble = stage1 XOR stage3
pub fn compute_caching_sha2_password_scramble(password: &str, salt: &[u8]) -> [u8; 32] {
	if password.is_empty() {
		return [0u8; 32];
	}
	let stage1: [u8; 32] = Sha256::digest(password.as_bytes()).into();
	let stage2: [u8; 32] = Sha256::digest(stage1).into();

	let mut hasher = Sha256::new();
	hasher.update(stage2);
	hasher.update(&salt[..20]);
	let stage3: [u8; 32] = hasher.finalize().into();

	let mut scramble = [0u8; 32];
	for i in 0..32 {
		scramble[i] = stage1[i] ^ stage3[i];
	}
	scramble
}

/// Verify a client scramble using caching_sha2_password against a plaintext password.
pub fn verify_caching_sha2_password(scramble: &[u8], salt: &[u8], password: &str) -> bool {
	if scramble.is_empty() {
		return password.is_empty();
	}
	if scramble.len() != 32 || salt.len() < 20 {
		return false;
	}
	let expected = compute_caching_sha2_password_scramble(password, salt);
	scramble == expected
}

/// Verify a client scramble using caching_sha2_password against a stored stage2 verifier.
///
/// If stage2 = SHA256(SHA256(password)), then:
///   candidate_stage1 = scramble XOR SHA256(stage2 + salt)
///   valid if SHA256(candidate_stage1) == stage2
pub fn verify_caching_sha2_password_fast_auth(
	scramble: &[u8],
	salt: &[u8],
	stage2: &[u8; 32],
) -> bool {
	if scramble.len() != 32 || salt.len() < 20 {
		return false;
	}

	let mut hasher = Sha256::new();
	hasher.update(stage2);
	hasher.update(&salt[..20]);
	let stage3: [u8; 32] = hasher.finalize().into();

	let mut candidate_stage1 = [0u8; 32];
	for i in 0..32 {
		candidate_stage1[i] = scramble[i] ^ stage3[i];
	}

	let candidate_stage2: [u8; 32] = Sha256::digest(candidate_stage1).into();
	candidate_stage2 == *stage2
}

/// Stored authentication verifier credentials for fast-auth caching and offline verification.
#[derive(Debug, Clone)]
pub struct CachedCredential {
	pub username: String,
	pub sha2_stage2: [u8; 32],
	pub native_stage2: [u8; 20],
	pub session_auth: Option<Arc<Auth>>,
	pub session_exp: Option<i64>,
}

/// Concurrent thread-safe cache for fast challenge-response authentication.
#[derive(Debug, Default)]
pub struct FastAuthCache {
	entries: RwLock<HashMap<String, CachedCredential>>,
}

impl FastAuthCache {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, username: &str) -> Option<CachedCredential> {
		let lock = self.entries.read().ok()?;
		lock.get(username).cloned()
	}

	pub fn insert(
		&self,
		username: &str,
		password: &str,
		auth: Option<Arc<Auth>>,
		exp: Option<i64>,
	) {
		let stage1: [u8; 32] = Sha256::digest(password.as_bytes()).into();
		let sha2_stage2: [u8; 32] = Sha256::digest(stage1).into();

		let native_stage1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
		let native_stage2: [u8; 20] = Sha1::digest(native_stage1).into();

		if let Ok(mut lock) = self.entries.write() {
			lock.insert(
				username.to_string(),
				CachedCredential {
					username: username.to_string(),
					sha2_stage2,
					native_stage2,
					session_auth: auth,
					session_exp: exp,
				},
			);
		}
	}

	pub fn insert_verifiers(
		&self,
		username: &str,
		sha2_stage2: [u8; 32],
		native_stage2: [u8; 20],
		auth: Option<Arc<Auth>>,
		exp: Option<i64>,
	) {
		if let Ok(mut lock) = self.entries.write() {
			lock.insert(
				username.to_string(),
				CachedCredential {
					username: username.to_string(),
					sha2_stage2,
					native_stage2,
					session_auth: auth,
					session_exp: exp,
				},
			);
		}
	}

	pub fn invalidate(&self, username: &str) {
		if let Ok(mut lock) = self.entries.write() {
			lock.remove(username);
		}
	}

	pub fn clear(&self) {
		if let Ok(mut lock) = self.entries.write() {
			lock.clear();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_mysql_native_password_verification() {
		let salt = generate_auth_salt();
		let password = "surreal_root_password";
		let scramble = compute_mysql_native_password_scramble(password, &salt);

		assert!(verify_mysql_native_password(&scramble, &salt, password));
		assert!(!verify_mysql_native_password(&scramble, &salt, "wrong_password"));

		// Fast auth with stage2
		let stage1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
		let stage2: [u8; 20] = Sha1::digest(stage1).into();
		assert!(verify_mysql_native_password_fast_auth(&scramble, &salt, &stage2));

		let wrong_stage2 = [0xabu8; 20];
		assert!(!verify_mysql_native_password_fast_auth(&scramble, &salt, &wrong_stage2));
	}

	#[test]
	fn test_caching_sha2_password_verification() {
		let salt = generate_auth_salt();
		let password = "surreal_sha2_password";
		let scramble = compute_caching_sha2_password_scramble(password, &salt);

		assert!(verify_caching_sha2_password(&scramble, &salt, password));
		assert!(!verify_caching_sha2_password(&scramble, &salt, "wrong_password"));

		// Fast auth with sha2_stage2
		let stage1: [u8; 32] = Sha256::digest(password.as_bytes()).into();
		let stage2: [u8; 32] = Sha256::digest(stage1).into();
		assert!(verify_caching_sha2_password_fast_auth(&scramble, &salt, &stage2));

		let wrong_stage2 = [0xefu8; 32];
		assert!(!verify_caching_sha2_password_fast_auth(&scramble, &salt, &wrong_stage2));
	}

	#[test]
	fn test_fast_auth_cache_lifecycle() {
		let cache = FastAuthCache::new();
		assert!(cache.get("alice").is_none());

		cache.insert("alice", "pass123", None, Some(123456789));
		let entry = cache.get("alice").expect("entry should exist");
		assert_eq!(entry.username, "alice");
		assert_eq!(entry.session_exp, Some(123456789));

		// Verify scramble against cached stage2
		let salt = generate_auth_salt();
		let scramble = compute_caching_sha2_password_scramble("pass123", &salt);
		assert!(verify_caching_sha2_password_fast_auth(&scramble, &salt, &entry.sha2_stage2));

		cache.invalidate("alice");
		assert!(cache.get("alice").is_none());
	}
}
