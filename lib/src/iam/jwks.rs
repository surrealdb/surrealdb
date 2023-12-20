use crate::err::Error;
use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::jwk::{JwkSet, KeyOperations, PublicKeyUse};
use jsonwebtoken::{DecodingKey, Validation};
use once_cell::sync::Lazy;
use reqwest::Client;
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
	// Try to find JWKS object in cache
	let jwks = match fetch_jwks_from_cache(url.clone()).await {
		Ok(jwks) => {
			trace!("Successfully fetched JWKS object from local cache");
			jwks
		}
		Err(err) => {
			trace!("Failed to fetch JWKS object from local cache: '{}'", err);
			// Otherwise, fetch JWKS object from URL
			match fetch_jwks_from_url(url.clone()).await {
				Ok(jwks) => {
					trace!("Successfully fetched JWKS object from remote location");
					// If fetch is successful, cache JWKS object
					match cache_jwks(jwks.clone(), url.clone()).await {
						Ok(_) => trace!("Successfully stored JWKS object in local cache"),
						Err(err) => warn!("Failed to store JWKS object in local cache: '{}'", err),
					}
					jwks
				}
				Err(err) => {
					warn!("Could not find a valid JWKS object localy or remotely: '{}'", err);
					return Err(Error::JwksNotFound(url));
				}
			}
		}
	};
	// Attempt to retrieve JWK from JWKS by the key identifier
	// TODO: This should be attempted before to see if fetching remotely is necessary
	let jwk = match jwks.find(&kid) {
		Some(jwk) => jwk.to_owned(),
		_ => return Err(Error::JwkNotFound(kid, url)),
	};
	// Check if key is intended to be used for signing
	// Source: https://datatracker.ietf.org/doc/html/rfc7517#section-4.2
	if let Some(PublicKeyUse::Signature) = &jwk.common.public_key_use {
	} else {
		warn!(
			"Invalid value for parameter 'use' in JWKS object: '{:?}'",
			jwk.common.public_key_use
		);
		return Err(Error::JwkInvalidParameter("use".to_string()));
	}
	// Check if key operations (if specified) include verification
	// Source: https://datatracker.ietf.org/doc/html/rfc7517#section-4.3
	if let Some(ops) = &jwk.common.key_operations {
		if !ops.iter().any(|op| *op == KeyOperations::Verify) {
			warn!(
				"Invalid value for parameter 'key_ops' in JWKS object: '{:?}'",
				jwk.common.key_operations
			);
			return Err(Error::JwkInvalidParameter("key_ops".to_string()));
		}
	}
	// Check if algorithm specified is supported
	let alg = match jwk.common.algorithm {
		Some(alg) => alg,
		_ => {
			warn!("Invalid value for parameter 'alg' in JWKS object: '{:?}'", jwk.common.algorithm);
			return Err(Error::JwkInvalidParameter("alg".to_string()));
		}
	};
	Ok((DecodingKey::from_jwk(&jwk)?, Validation::new(alg)))
}

async fn fetch_jwks_from_url(url: String) -> Result<JwkSet, Error> {
	let client = Client::new();
	let res = client.get(&url).send().await?;
	if !res.status().is_success() {
		debug!(
			"Fetching JWKS object from remote location returned status code: '{}'",
			res.status()
		);
		return Err(Error::JwksNotFound(res.url().to_string()));
	}
	let jwks = res.bytes().await?;
	match serde_json::from_slice(&jwks) {
		Ok(jwks) => Ok(jwks),
		Err(err) => {
			warn!("Error parsing JWKS object: '{}'", err);
			Err(Error::JwksMalformed)
		}
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
		Err(err) => {
			println!("{}", err);
			Err(Error::JwksMalformed)
		}
	}
}

async fn fetch_jwks_from_cache(url: String) -> Result<JwkSet, Error> {
	let path = cache_path_from_url(url);
	let bytes = crate::obs::get(&path).await?;
	let jwks_cache: JwksCache = match serde_json::from_slice(&bytes) {
		Ok(jwks_cache) => jwks_cache,
		Err(err) => {
			warn!("Error parsing JWKS object: '{}'", err);
			return Err(Error::JwksMalformed);
		}
	};
	// Check if the cached JWKS object has expired.
	if Utc::now().signed_duration_since(jwks_cache.time) > *CACHE_EXPIRATION {
		return Err(Error::JwksNotFound(path));
	}
	Ok(jwks_cache.jwks)
}

#[cfg(test)]
mod tests {
	use super::*;
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	#[tokio::test]
	async fn test_golden_path() {
		let jwks = r#"{"keys":[{"kid":"test","kty":"RSA","use":"sig","alg":"RS256","n":"2nsSvrRnuw6OLJCqltkiRAGV07-35isdPwyTrrWQ3PwxEZc-lDbquQ7Z9Fkx5Y-ldVzBbTHEsbmhDYjBubUlS4dhstvpYD93963Sw6Q6gQjow_T4xWqsaeuj4PpcajPjI_ybbDwLa7bIXEBz7AC3UAgxY0khiERfq2quWIaeK0MLJ7bBcpyGF7hZy1SUehQ187-yBrM9Dsi2qKxQX981JFsctEnJLaabvoWUMQsMucTJXBRp5X_bGJ70XjgB85DNWTVqw7XwEfe_piM5DcvjVcR86bYMw-Qs46a3IzvIDs54X9--frM35IHLNrpwVbfsg4qgmya_GTPF4NSVab0xaQ","e":"AQAB","x5t":"ZffK9pHzOtCslIK_800RhfZZ_ks","x5c":["MIIDBTCCAe2gAwIBAgIJdeyDfUXHLwX+MA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANp7Er60Z7sOjiyQqpbZIkQBldO/t+YrHT8Mk661kNz8MRGXPpQ26rkO2fRZMeWPpXVcwW0xxLG5oQ2Iwbm1JUuHYbLb6WA/d/et0sOkOoEI6MP0+MVqrGnro+D6XGoz4yP8m2w8C2u2yFxAc+wAt1AIMWNJIYhEX6tqrliGnitDCye2wXKchhe4WctUlHoUNfO/sgazPQ7ItqisUF/fNSRbHLRJyS2mm76FlDELDLnEyVwUaeV/2xie9F44AfOQzVk1asO18BH3v6YjOQ3L41XEfOm2DMPkLOOmtyM7yA7OeF/fvn6zN+SByza6cFW37IOKoJsmvxkzxeDUlWm9MWkCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUOeOmT9I3/MJ/zI/lS74gPQmAQfEwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQBDue8iM90XJcLORvr6e+h15f5tlvVjZ/cAzv09487QSHJtUd6qwTlunEABS5818TgMEMFDRafQ7CDX3KaAAXFpo2bnyWY9c+Ozp0PWtp8ChunOs94ayaG+viO0AiTrIY28cc26ehNBZ/4gC4/1k0IlXEk8rd1e03hQuhNiy7SQaxS2f1xkJfR4vCeF8HTN5omjKvIMcWRqkkwFZm4gdgkMfi2lNsV8V6+HXyTc3XUIdcwOUcC+Ms/m+vKxnyxw0UGh0RliB1qBc0ADg83hOsXEqZjneHh1ZhqqVF4IkKSJTfK5ofcc14GqvpLjjTR3s2eX6zxdujzwf4gnHdxjVvdJ"]}]}"#;
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_bytes(jwks);
		Mock::given(method("GET"))
			.and(path("/jwks.json"))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		// Get token configuration from remote URL.
		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_ok());

		// Drop server to force usage of the local cache.
		drop(mock_server);

		// Get token configuration from local cache.
		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_ok());
	}

	#[tokio::test]
	async fn test_encryption_key() {
		let jwks = r#"{"keys":[{"kid":"test","kty":"RSA","use":"enc","alg":"RS256","n":"2nsSvrRnuw6OLJCqltkiRAGV07-35isdPwyTrrWQ3PwxEZc-lDbquQ7Z9Fkx5Y-ldVzBbTHEsbmhDYjBubUlS4dhstvpYD93963Sw6Q6gQjow_T4xWqsaeuj4PpcajPjI_ybbDwLa7bIXEBz7AC3UAgxY0khiERfq2quWIaeK0MLJ7bBcpyGF7hZy1SUehQ187-yBrM9Dsi2qKxQX981JFsctEnJLaabvoWUMQsMucTJXBRp5X_bGJ70XjgB85DNWTVqw7XwEfe_piM5DcvjVcR86bYMw-Qs46a3IzvIDs54X9--frM35IHLNrpwVbfsg4qgmya_GTPF4NSVab0xaQ","e":"AQAB","x5t":"ZffK9pHzOtCslIK_800RhfZZ_ks","x5c":["MIIDBTCCAe2gAwIBAgIJdeyDfUXHLwX+MA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANp7Er60Z7sOjiyQqpbZIkQBldO/t+YrHT8Mk661kNz8MRGXPpQ26rkO2fRZMeWPpXVcwW0xxLG5oQ2Iwbm1JUuHYbLb6WA/d/et0sOkOoEI6MP0+MVqrGnro+D6XGoz4yP8m2w8C2u2yFxAc+wAt1AIMWNJIYhEX6tqrliGnitDCye2wXKchhe4WctUlHoUNfO/sgazPQ7ItqisUF/fNSRbHLRJyS2mm76FlDELDLnEyVwUaeV/2xie9F44AfOQzVk1asO18BH3v6YjOQ3L41XEfOm2DMPkLOOmtyM7yA7OeF/fvn6zN+SByza6cFW37IOKoJsmvxkzxeDUlWm9MWkCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUOeOmT9I3/MJ/zI/lS74gPQmAQfEwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQBDue8iM90XJcLORvr6e+h15f5tlvVjZ/cAzv09487QSHJtUd6qwTlunEABS5818TgMEMFDRafQ7CDX3KaAAXFpo2bnyWY9c+Ozp0PWtp8ChunOs94ayaG+viO0AiTrIY28cc26ehNBZ/4gC4/1k0IlXEk8rd1e03hQuhNiy7SQaxS2f1xkJfR4vCeF8HTN5omjKvIMcWRqkkwFZm4gdgkMfi2lNsV8V6+HXyTc3XUIdcwOUcC+Ms/m+vKxnyxw0UGh0RliB1qBc0ADg83hOsXEqZjneHh1ZhqqVF4IkKSJTfK5ofcc14GqvpLjjTR3s2eX6zxdujzwf4gnHdxjVvdJ"]}]}"#;
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_bytes(jwks);
		Mock::given(method("GET"))
			.and(path("/jwks.json"))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_err());
	}

	#[tokio::test]
	async fn test_invalid_algorithm() {
		let jwks = r#"{"keys":[{"kid":"test","kty":"RSA","use":"sig","alg":"XX999","n":"2nsSvrRnuw6OLJCqltkiRAGV07-35isdPwyTrrWQ3PwxEZc-lDbquQ7Z9Fkx5Y-ldVzBbTHEsbmhDYjBubUlS4dhstvpYD93963Sw6Q6gQjow_T4xWqsaeuj4PpcajPjI_ybbDwLa7bIXEBz7AC3UAgxY0khiERfq2quWIaeK0MLJ7bBcpyGF7hZy1SUehQ187-yBrM9Dsi2qKxQX981JFsctEnJLaabvoWUMQsMucTJXBRp5X_bGJ70XjgB85DNWTVqw7XwEfe_piM5DcvjVcR86bYMw-Qs46a3IzvIDs54X9--frM35IHLNrpwVbfsg4qgmya_GTPF4NSVab0xaQ","e":"AQAB","x5t":"ZffK9pHzOtCslIK_800RhfZZ_ks","x5c":["MIIDBTCCAe2gAwIBAgIJdeyDfUXHLwX+MA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANp7Er60Z7sOjiyQqpbZIkQBldO/t+YrHT8Mk661kNz8MRGXPpQ26rkO2fRZMeWPpXVcwW0xxLG5oQ2Iwbm1JUuHYbLb6WA/d/et0sOkOoEI6MP0+MVqrGnro+D6XGoz4yP8m2w8C2u2yFxAc+wAt1AIMWNJIYhEX6tqrliGnitDCye2wXKchhe4WctUlHoUNfO/sgazPQ7ItqisUF/fNSRbHLRJyS2mm76FlDELDLnEyVwUaeV/2xie9F44AfOQzVk1asO18BH3v6YjOQ3L41XEfOm2DMPkLOOmtyM7yA7OeF/fvn6zN+SByza6cFW37IOKoJsmvxkzxeDUlWm9MWkCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUOeOmT9I3/MJ/zI/lS74gPQmAQfEwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQBDue8iM90XJcLORvr6e+h15f5tlvVjZ/cAzv09487QSHJtUd6qwTlunEABS5818TgMEMFDRafQ7CDX3KaAAXFpo2bnyWY9c+Ozp0PWtp8ChunOs94ayaG+viO0AiTrIY28cc26ehNBZ/4gC4/1k0IlXEk8rd1e03hQuhNiy7SQaxS2f1xkJfR4vCeF8HTN5omjKvIMcWRqkkwFZm4gdgkMfi2lNsV8V6+HXyTc3XUIdcwOUcC+Ms/m+vKxnyxw0UGh0RliB1qBc0ADg83hOsXEqZjneHh1ZhqqVF4IkKSJTfK5ofcc14GqvpLjjTR3s2eX6zxdujzwf4gnHdxjVvdJ"]}]}"#;
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_bytes(jwks);
		Mock::given(method("GET"))
			.and(path("/jwks.json"))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_err());
	}

	#[tokio::test]
	async fn test_invalid_key_type() {
		let jwks = r#"{"keys":[{"kid":"test","kty":"XXX","use":"sig","alg":"RS256","n":"2nsSvrRnuw6OLJCqltkiRAGV07-35isdPwyTrrWQ3PwxEZc-lDbquQ7Z9Fkx5Y-ldVzBbTHEsbmhDYjBubUlS4dhstvpYD93963Sw6Q6gQjow_T4xWqsaeuj4PpcajPjI_ybbDwLa7bIXEBz7AC3UAgxY0khiERfq2quWIaeK0MLJ7bBcpyGF7hZy1SUehQ187-yBrM9Dsi2qKxQX981JFsctEnJLaabvoWUMQsMucTJXBRp5X_bGJ70XjgB85DNWTVqw7XwEfe_piM5DcvjVcR86bYMw-Qs46a3IzvIDs54X9--frM35IHLNrpwVbfsg4qgmya_GTPF4NSVab0xaQ","e":"AQAB","x5t":"ZffK9pHzOtCslIK_800RhfZZ_ks","x5c":["MIIDBTCCAe2gAwIBAgIJdeyDfUXHLwX+MA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANp7Er60Z7sOjiyQqpbZIkQBldO/t+YrHT8Mk661kNz8MRGXPpQ26rkO2fRZMeWPpXVcwW0xxLG5oQ2Iwbm1JUuHYbLb6WA/d/et0sOkOoEI6MP0+MVqrGnro+D6XGoz4yP8m2w8C2u2yFxAc+wAt1AIMWNJIYhEX6tqrliGnitDCye2wXKchhe4WctUlHoUNfO/sgazPQ7ItqisUF/fNSRbHLRJyS2mm76FlDELDLnEyVwUaeV/2xie9F44AfOQzVk1asO18BH3v6YjOQ3L41XEfOm2DMPkLOOmtyM7yA7OeF/fvn6zN+SByza6cFW37IOKoJsmvxkzxeDUlWm9MWkCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUOeOmT9I3/MJ/zI/lS74gPQmAQfEwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQBDue8iM90XJcLORvr6e+h15f5tlvVjZ/cAzv09487QSHJtUd6qwTlunEABS5818TgMEMFDRafQ7CDX3KaAAXFpo2bnyWY9c+Ozp0PWtp8ChunOs94ayaG+viO0AiTrIY28cc26ehNBZ/4gC4/1k0IlXEk8rd1e03hQuhNiy7SQaxS2f1xkJfR4vCeF8HTN5omjKvIMcWRqkkwFZm4gdgkMfi2lNsV8V6+HXyTc3XUIdcwOUcC+Ms/m+vKxnyxw0UGh0RliB1qBc0ADg83hOsXEqZjneHh1ZhqqVF4IkKSJTfK5ofcc14GqvpLjjTR3s2eX6zxdujzwf4gnHdxjVvdJ"]}]}"#;
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_bytes(jwks);
		Mock::given(method("GET"))
			.and(path("/jwks.json"))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_err());
	}

	#[tokio::test]
	async fn test_remote_down() {
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(500);
		Mock::given(method("GET"))
			.and(path("/jwks.json"))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		// Get token configuration from remote URL respnding with Internal Server Error.
		assert!(config("test".to_string(), format!("{}/jwks.json", &url)).await.is_err());
	}
}
