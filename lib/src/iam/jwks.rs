use crate::err::Error;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::{JwkSet, KeyOperations, PublicKeyUse};
use jsonwebtoken::{DecodingKey, Validation};
use once_cell::sync::Lazy;
use reqwest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

static CACHE_EXPIRATION: Lazy<chrono::Duration> =
	Lazy::new(|| match std::env::var("SURREAL_JWKS_CACHE_EXPIRATION") {
		Ok(seconds_str) => {
			let seconds = seconds_str
				.parse::<u64>()
				.expect("Expected a valid number of seconds for SURREAL_JWKS_CACHE_EXPIRATION");
			Duration::seconds(seconds as i64)
		}
		Err(_) => {
			Duration::seconds(43200) // Set default cache expiration of 12 hours.
		}
	});

pub(crate) async fn config(kid: String, url: String) -> Result<(DecodingKey, Validation), Error> {
	// Try to find JWKS in cache
	let jwks = match fetch_jwks_from_cache(url.clone()).await {
		Ok(jwks) => {
			trace!("Successfully fetched JWKS object from local cache");
			jwks
		}
		Err(_) => {
			// Otherwise, fetch JWKS object from URL
			match fetch_jwks_from_url(url.clone()).await {
				Ok(jwks) => {
					trace!("Successfully fetched JWKS object from remote location");
					// If fetch is successful, cache JWKS object
					match cache_jwks(jwks.clone(), url.clone()).await {
						Ok(_) => trace!("Successfully stored JWKS object in local cache"),
						Err(_) => warn!("Failed to store JWKS object in local cache"),
					}
					jwks
				}
				Err(_) => return Err(Error::JwksNotFound(url)),
			}
		}
	};
	// Attempt to retrieve JWK from JWKS by KID
	let jwk = match jwks.find(&kid) {
		Some(jwk) => jwk.to_owned(),
		_ => return Err(Error::JwkNotFound(kid, url)),
	};
	// Check if key is intended to be used for signing
	// Source: https://datatracker.ietf.org/doc/html/rfc7517#section-4.2
	if let Some(PublicKeyUse::Signature) = &jwk.common.public_key_use {
	} else {
		return Err(Error::UnsupportedCryptographicAlgorithm);
	}
	// Check if key operations (if specified) include verification
	// Source: https://datatracker.ietf.org/doc/html/rfc7517#section-4.3
	if let Some(ops) = &jwk.common.key_operations {
		if !ops.iter().any(|op| *op == KeyOperations::Verify) {
			return Err(Error::UnsupportedCryptographicAlgorithm);
		}
	}
	// Check if algorithm specified is supported
	let alg = match jwk.common.algorithm {
		Some(alg) => alg,
		_ => return Err(Error::UnsupportedCryptographicAlgorithm),
	};
	Ok((DecodingKey::from_jwk(&jwk)?, Validation::new(alg)))
}

async fn fetch_jwks_from_url(url: String) -> Result<JwkSet, Error> {
	let res = reqwest::blocking::get(url)?;
	if !res.status().is_success() {
		return Err(Error::JwksNotFound(res.url().to_string()));
	}
	let jwks = res.bytes()?;
	match serde_json::from_slice(&jwks) {
		Ok(jwks) => Ok(jwks),
		Err(_) => Err(Error::JwksMalformed),
	}
}

#[derive(Serialize, Deserialize)]
struct JwksCache {
	jwks: JwkSet,
	time: DateTime<Utc>,
}

fn cache_path_from_url(url: String) -> String {
	let mut hasher = Sha256::new();
	hasher.update(url.as_bytes());
	let result = hasher.finalize();
	format!("jwks/{:x}.json", result)
}

async fn cache_jwks(jwks: JwkSet, url: String) -> Result<(), Error> {
	let jwks_cache = JwksCache {
		jwks,
		time: Utc::now(),
	};
	let path = cache_path_from_url(url);
	match serde_json::to_vec(&jwks_cache) {
		Ok(data) => crate::obs::put(&path, data).await,
		Err(_) => Err(Error::JwksMalformed),
	}
}

async fn fetch_jwks_from_cache(url: String) -> Result<JwkSet, Error> {
	let path = cache_path_from_url(url);
	let bytes = match crate::obs::get(&path).await {
		Ok(bytes) => bytes,
		Err(_) => return Err(Error::JwksNotFound(path)),
	};
	let jwks_cache: JwksCache = match serde_json::from_slice(&bytes) {
		Ok(jwks_cache) => jwks_cache,
		Err(_) => return Err(Error::JwksMalformed),
	};
	// Check if the cached JWKS object has expired.
	if Utc::now().signed_duration_since(jwks_cache.time) > *CACHE_EXPIRATION {
		return Err(Error::InvalidAuth);
	}
	Ok(jwks_cache.jwks)
}
