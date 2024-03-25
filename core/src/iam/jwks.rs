use chrono::{DateTime, Utc};
use jsonwebtoken::jwk::JwkSet;
use serde::{Deserialize, Serialize};

#[cfg(test)]
static CACHE_EXPIRATION: Lazy<chrono::Duration> = Lazy::new(|| Duration::seconds(1));

#[derive(Serialize, Deserialize)]
struct JwksCache {
	jwks: JwkSet,
	time: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::capabilities::{Capabilities, NetTarget, Targets};
	use rand::{distributions::Alphanumeric, Rng};
	use wiremock::matchers::{method, path};
	use wiremock::{Mock, MockServer, ResponseTemplate};

	// Use unique path to prevent accidental cache reuse
	fn random_path() -> String {
		let rng = rand::thread_rng();
		rng.sample_iter(&Alphanumeric).take(8).map(char::from).collect()
	}

	static DEFAULT_JWKS: Lazy<JwkSet> = Lazy::new(|| {
		JwkSet{
		keys: vec![Jwk{
			common: jsonwebtoken::jwk::CommonParameters {
				public_key_use: Some(jsonwebtoken::jwk::PublicKeyUse::Signature),
				key_operations: None,
				algorithm: Some(jsonwebtoken::Algorithm::RS256),
				key_id: Some("test_1".to_string()),
				x509_url: None,
				x509_chain: Some(vec![
					"MIIDBTCCAe2gAwIBAgIJdeyDfUXHLwX+MA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANp7Er60Z7sOjiyQqpbZIkQBldO/t+YrHT8Mk661kNz8MRGXPpQ26rkO2fRZMeWPpXVcwW0xxLG5oQ2Iwbm1JUuHYbLb6WA/d/et0sOkOoEI6MP0+MVqrGnro+D6XGoz4yP8m2w8C2u2yFxAc+wAt1AIMWNJIYhEX6tqrliGnitDCye2wXKchhe4WctUlHoUNfO/sgazPQ7ItqisUF/fNSRbHLRJyS2mm76FlDELDLnEyVwUaeV/2xie9F44AfOQzVk1asO18BH3v6YjOQ3L41XEfOm2DMPkLOOmtyM7yA7OeF/fvn6zN+SByza6cFW37IOKoJsmvxkzxeDUlWm9MWkCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUOeOmT9I3/MJ/zI/lS74gPQmAQfEwDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQBDue8iM90XJcLORvr6e+h15f5tlvVjZ/cAzv09487QSHJtUd6qwTlunEABS5818TgMEMFDRafQ7CDX3KaAAXFpo2bnyWY9c+Ozp0PWtp8ChunOs94ayaG+viO0AiTrIY28cc26ehNBZ/4gC4/1k0IlXEk8rd1e03hQuhNiy7SQaxS2f1xkJfR4vCeF8HTN5omjKvIMcWRqkkwFZm4gdgkMfi2lNsV8V6+HXyTc3XUIdcwOUcC+Ms/m+vKxnyxw0UGh0RliB1qBc0ADg83hOsXEqZjneHh1ZhqqVF4IkKSJTfK5ofcc14GqvpLjjTR3s2eX6zxdujzwf4gnHdxjVvdJ".to_string(),
				]),
				x509_sha1_fingerprint: None,
				x509_sha256_fingerprint: None,
			},
			algorithm: jsonwebtoken::jwk::AlgorithmParameters::RSA(
				jsonwebtoken::jwk::RSAKeyParameters{
				key_type: jsonwebtoken::jwk::RSAKeyType::RSA,
				n: "2nsSvrRnuw6OLJCqltkiRAGV07-35isdPwyTrrWQ3PwxEZc-lDbquQ7Z9Fkx5Y-ldVzBbTHEsbmhDYjBubUlS4dhstvpYD93963Sw6Q6gQjow_T4xWqsaeuj4PpcajPjI_ybbDwLa7bIXEBz7AC3UAgxY0khiERfq2quWIaeK0MLJ7bBcpyGF7hZy1SUehQ187-yBrM9Dsi2qKxQX981JFsctEnJLaabvoWUMQsMucTJXBRp5X_bGJ70XjgB85DNWTVqw7XwEfe_piM5DcvjVcR86bYMw-Qs46a3IzvIDs54X9--frM35IHLNrpwVbfsg4qgmya_GTPF4NSVab0xaQ".to_string(),
				e: "AQAB".to_string(),
				}
			),
		},
		Jwk{
			common: jsonwebtoken::jwk::CommonParameters {
				public_key_use: Some(jsonwebtoken::jwk::PublicKeyUse::Signature),
				key_operations: None,
				algorithm: Some(jsonwebtoken::Algorithm::RS256),
				key_id: Some("test_2".to_string()),
				x509_url: None,
				x509_chain: Some(vec![
					"MIIDBTCCAe2gAwIBAgIJUzJ062XCgOVQMA0GCSqGSIb3DQEBCwUAMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTAeFw0yMzEwMzExNzI5MDBaFw0zNzA3MDkxNzI5MDBaMCAxHjAcBgNVBAMTFWdlcmF1c2VyLmV1LmF1dGgwLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL7TgjrojPySPO5kPBOiAgiS0guSpYkfb0AtLNVyVeANe5vgjJoBQDe7vAGm68SHft841GQBPp5KWpxDTO+liECVAnbdR9YHwuZWOGuPRVVwNqtVmS8A75YG/mTWGV4tr2h+dLjjV3jvV0hvXRJwVFShlUS9+BqgevFBoF6zxi5AHIx/k1tCg1y2fhSlzYUHxEiFRgx0RhtJfizyv9QHoLSY3RFI4QOAkPtYwN5C1X69nEHPK0Q+W+POkeV7wuMQZWTRRT+xZuYn+JIYQCQviZ52FoJsrTzOEO5jlmrUa9PMEJpn0Aw68OdyLHjQPsip8B2JSegoVP1LTc0tDoqVGqUCAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQUYp67WM42b2pqF7ES0LsFvAI/Qy8wDgYDVR0PAQH/BAQDAgKEMA0GCSqGSIb3DQEBCwUAA4IBAQANOhmYz0jxNJG6pZ0klNtH00E6SoEsM/MNYH+atraTVZNeqPLAZH9514gMcqdu7+rBfQ/pRjpQG1YbkdZGQBaq5cZNlE6hNCT4BSgKddBYsN0WbfTGxstDVdoXLySgGCYpyKO6Hek4ULxwAf1LmMyOYpn4JrECy4mYShsCcfe504qfzUTd7pz1VaZ4minclOhz0dZbgYa+slUepe0C2+w+T3US138x0lPB9C266SLDakb6n/JTum+Czn2xlFBf4K4w6eWuSknvTlRrqTGE8RX3vzOiKTM3hpDdjU7Tu7eNsZpLkDR1e+w33m5NMi9iYgJcyTGsIeeHr0xjrRPD9Dwh".to_string(),
				]),
				x509_sha1_fingerprint: None,
				x509_sha256_fingerprint: None,
			},
			algorithm: jsonwebtoken::jwk::AlgorithmParameters::RSA(
				jsonwebtoken::jwk::RSAKeyParameters{
				key_type: jsonwebtoken::jwk::RSAKeyType::RSA,
				n: "vtOCOuiM_JI87mQ8E6ICCJLSC5KliR9vQC0s1XJV4A17m-CMmgFAN7u8AabrxId-3zjUZAE-nkpanENM76WIQJUCdt1H1gfC5lY4a49FVXA2q1WZLwDvlgb-ZNYZXi2vaH50uONXeO9XSG9dEnBUVKGVRL34GqB68UGgXrPGLkAcjH-TW0KDXLZ-FKXNhQfESIVGDHRGG0l-LPK_1AegtJjdEUjhA4CQ-1jA3kLVfr2cQc8rRD5b486R5XvC4xBlZNFFP7Fm5if4khhAJC-JnnYWgmytPM4Q7mOWatRr08wQmmfQDDrw53IseNA-yKnwHYlJ6ChU_UtNzS0OipUapQ".to_string(),
				e: "AQAB".to_string(),
				}
			),
		}
		],
	}
	});

	#[tokio::test]
	async fn test_golden_path() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Get first token configuration from remote location
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_ok(), "Failed to validate token the first time: {:?}", res.err());

		// Drop server to force usage of the local cache
		drop(mock_server);

		// Get second token configuration from local cache
		let res = config(&ds, "test_2", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_ok(), "Failed to validate token the second time: {:?}", res.err());
	}

	#[tokio::test]
	async fn test_capabilities_default() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::default());
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(0)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Get token configuration from unallowed remote location
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_err(), "Unexpected success validating token from unallowed remote location");
	}

	#[tokio::test]
	async fn test_capabilities_specific_port() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1:443").unwrap()].into(), // Different port from server
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(0)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Get token configuration from unallowed remote location
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_err(), "Unexpected success validating token from unallowed remote location");
	}

	#[tokio::test]
	async fn test_cache_expiration() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(2)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Get token configuration from remote location
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_ok(), "Failed to validate token the first time: {:?}", res.err());

		// Wait for cache to expire
		std::thread::sleep((*CACHE_EXPIRATION + Duration::seconds(1)).to_std().unwrap());

		// Get same token configuration again after cache has expired
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_ok(), "Failed to validate token the second time: {:?}", res.err());

		// The server will panic if it does not receive exactly two expected requests
	}

	#[tokio::test]
	async fn test_cache_cooldown() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(1)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Use token with invalid key identifier claim to force cache refresh
		let res = config(&ds, "invalid", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_err(), "Unexpected success validating token with invalid key identifier");

		// Use token with invalid key identifier claim to force cache refresh again before cooldown
		let res = config(&ds, "invalid", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_err(), "Unexpected success validating token with invalid key identifier");

		// The server will panic if it receives more than the single expected request
	}

	#[tokio::test]
	async fn test_cache_expiration_remote_down() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.up_to_n_times(1) // Only respond the first time
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		// Get token configuration from remote location
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(res.is_ok(), "Failed to validate token the first time: {:?}", res.err());

		// Wait for cache to expire
		std::thread::sleep((*CACHE_EXPIRATION + Duration::seconds(1)).to_std().unwrap());

		// Get same token configuration again after cache has expired
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token with an expired cache and remote down"
		);
	}

	#[tokio::test]
	async fn test_unsupported_algorithm() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let mut jwks = DEFAULT_JWKS.clone();
		jwks.keys[0].common.algorithm = None;

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token with key using unsupported algorithm"
		);
	}

	#[tokio::test]
	async fn test_no_key_use() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let mut jwks = DEFAULT_JWKS.clone();
		jwks.keys[0].common.public_key_use = None;

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_ok(),
			"Failed to validate token with key that does not specify use: {:?}",
			res.err()
		);
	}

	#[tokio::test]
	async fn test_key_use_enc() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let mut jwks = DEFAULT_JWKS.clone();
		jwks.keys[0].common.public_key_use = Some(jsonwebtoken::jwk::PublicKeyUse::Encryption);

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token with key that only supports encryption"
		);
	}

	#[tokio::test]
	async fn test_key_ops_encrypt_only() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let mut jwks = DEFAULT_JWKS.clone();
		jwks.keys[0].common.key_operations = Some(vec![jsonwebtoken::jwk::KeyOperations::Encrypt]);

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200).set_body_json(jwks);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token with key that only supports encryption"
		);
	}

	#[tokio::test]
	async fn test_remote_down() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(500);
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.mount(&mock_server)
			.await;

		let url = mock_server.uri();

		// Get token configuration from remote location responding with Internal Server Error
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token configuration with unavailable remote location"
		);
	}

	#[tokio::test]
	#[cfg(not(target_arch = "wasm32"))]
	async fn test_remote_timeout() {
		let ds = Datastore::new("memory").await.unwrap().with_capabilities(
			Capabilities::default().with_network_targets(Targets::<NetTarget>::Some(
				[NetTarget::from_str("127.0.0.1").unwrap()].into(),
			)),
		);
		let jwks = DEFAULT_JWKS.clone();

		let jwks_path = format!("{}/jwks.json", random_path());
		let mock_server = MockServer::start().await;
		let response = ResponseTemplate::new(200)
			.set_body_json(jwks)
			.set_delay((*REMOTE_TIMEOUT + Duration::seconds(10)).to_std().unwrap());
		Mock::given(method("GET"))
			.and(path(&jwks_path))
			.respond_with(response)
			.expect(1)
			.mount(&mock_server)
			.await;
		let url = mock_server.uri();

		let start_time = Utc::now();
		// Get token configuration from remote location responding very slowly
		let res = config(&ds, "test_1", &format!("{}/{}", &url, &jwks_path)).await;
		assert!(
			res.is_err(),
			"Unexpected success validating token configuration with unavailable remote location"
		);
		assert!(
			Utc::now() - start_time < *REMOTE_TIMEOUT + Duration::seconds(1),
			"Remote request was not aborted immediately after timeout"
		);
	}
}
