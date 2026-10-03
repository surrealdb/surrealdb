//! Authentication mechanisms for the MySQL wire protocol.

use rand::TryRngCore;
use rand::rngs::OsRng;
use sha1::{Digest, Sha1};

pub const AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
pub const AUTH_PLUGIN_CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";

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

/// Computes the expected scramble given a plaintext password and salt (useful for test clients).
pub fn compute_mysql_native_password_scramble(password: &str, salt: &[u8]) -> [u8; 20] {
	if password.is_empty() {
		return [0u8; 20];
	}
	let stage1: [u8; 20] = Sha1::digest(password.as_bytes()).into();
	let stage2: [u8; 20] = Sha1::digest(stage1).into();

	let mut hasher = Sha1::new();
	hasher.update(&salt[..20]);
	hasher.update(stage2);
	let candidate_xor: [u8; 20] = hasher.finalize().into();

	let mut scramble = [0u8; 20];
	for i in 0..20 {
		scramble[i] = stage1[i] ^ candidate_xor[i];
	}
	scramble
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
	}
}
