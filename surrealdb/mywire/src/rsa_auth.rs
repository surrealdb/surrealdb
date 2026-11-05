//! The RSA password exchange of `caching_sha2_password` full authentication.
//!
//! A client without TLS asks for the server's public key, encrypts `password XOR salt` with
//! RSA-OAEP (SHA-1), and sends the result. The key pair is generated once per process, on first
//! use, and shared by every connection. Targets without a native runtime offer no exchange.

/// The byte a client sends in place of its password to request the server's public key.
pub const REQUEST_PUBLIC_KEY: u8 = 0x02;

#[cfg(not(target_arch = "wasm32"))]
mod native {
	use rsa::pkcs8::{EncodePublicKey, LineEnding};
	use rsa::rand_core::OsRng;
	use rsa::{Oaep, RsaPrivateKey};
	use sha1::Sha1;
	use tokio::sync::OnceCell;
	use tokio::task::spawn_blocking;

	const KEY_BITS: usize = 2048;

	struct KeyPair {
		private: RsaPrivateKey,
		public_pem: Vec<u8>,
	}

	static KEY: OnceCell<Option<KeyPair>> = OnceCell::const_new();

	/// Generates the key pair on the blocking pool, so the async workers keep serving. A panic of
	/// the task is raised again.
	async fn generate() -> Option<KeyPair> {
		let task = spawn_blocking(|| {
			let private = RsaPrivateKey::new(&mut OsRng, KEY_BITS).ok()?;
			let pem = private.to_public_key().to_public_key_pem(LineEnding::LF).ok()?;
			Some(KeyPair {
				private,
				public_pem: pem.into_bytes(),
			})
		});
		match task.await {
			Ok(pair) => pair,
			Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
			Err(_) => None,
		}
	}

	/// The server's public key as SPKI PEM, or `None` when no key pair could be generated.
	pub async fn public_key_pem() -> Option<Vec<u8>> {
		KEY.get_or_init(generate).await.as_ref().map(|pair| pair.public_pem.clone())
	}

	/// Recovers the password from an encrypted reply: decrypts on the blocking pool, then XORs
	/// with the salt repeated to the plaintext's length. The result still carries its trailing
	/// NUL. A panic of the task is raised again.
	pub async fn decrypt_password(encrypted: &[u8], salt: &[u8]) -> Option<Vec<u8>> {
		let pair = KEY.get()?.as_ref()?;
		if salt.is_empty() {
			return None;
		}
		let private = pair.private.clone();
		let encrypted = encrypted.to_vec();
		let salt = salt.to_vec();
		let task = spawn_blocking(move || {
			let mut plain = private.decrypt(Oaep::new::<Sha1>(), &encrypted).ok()?;
			for (i, byte) in plain.iter_mut().enumerate() {
				*byte ^= salt[i % salt.len()];
			}
			Some(plain)
		});
		match task.await {
			Ok(plain) => plain,
			Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
			Err(_) => None,
		}
	}

	#[cfg(test)]
	mod tests {
		use rsa::RsaPublicKey;
		use rsa::pkcs8::DecodePublicKey;

		use super::*;

		#[tokio::test]
		async fn a_password_encrypted_to_the_published_key_is_recovered() {
			let pem = public_key_pem().await.expect("a key pair");
			let public = RsaPublicKey::from_public_key_pem(std::str::from_utf8(&pem).unwrap())
				.expect("SPKI PEM");
			let salt: Vec<u8> = (1u8..=20).collect();
			let mut masked = b"s3cret\0".to_vec();
			for (i, byte) in masked.iter_mut().enumerate() {
				*byte ^= salt[i % salt.len()];
			}
			let encrypted =
				public.encrypt(&mut OsRng, Oaep::new::<Sha1>(), &masked).expect("encrypt");
			let plain = decrypt_password(&encrypted, &salt).await.expect("decrypt");
			assert_eq!(plain, b"s3cret\0");
		}

		#[tokio::test]
		async fn a_reply_not_encrypted_to_the_key_is_refused() {
			public_key_pem().await.expect("a key pair");
			assert!(decrypt_password(&[0u8; 256], &[1u8; 20]).await.is_none());
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::{decrypt_password, public_key_pem};

/// Targets without a native runtime offer no key.
#[cfg(target_arch = "wasm32")]
pub async fn public_key_pem() -> Option<Vec<u8>> {
	None
}

#[cfg(target_arch = "wasm32")]
pub async fn decrypt_password(_encrypted: &[u8], _salt: &[u8]) -> Option<Vec<u8>> {
	None
}
