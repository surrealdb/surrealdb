//! Per-database AI configuration overlay.
//!
//! When a database has DEFINE CONFIG AI, optional keys are applied on top of
//! environment variables. The ai crate stays free of catalog dependency by
//! using this plain struct.

use anyhow::Result;

/// Optional overrides for AI provider credentials and base URLs.
///
/// Each field, when `Some`, overrides the corresponding environment variable
/// for that provider when running in a database context.
#[derive(Clone, Debug, Default)]
pub struct AiConfigOverlay {
	pub openai_api_key: Option<String>,
	pub openai_base_url: Option<String>,
	pub anthropic_api_key: Option<String>,
	pub anthropic_base_url: Option<String>,
	pub google_api_key: Option<String>,
	pub google_base_url: Option<String>,
	pub voyage_api_key: Option<String>,
	pub voyage_base_url: Option<String>,
	pub huggingface_api_key: Option<String>,
	pub huggingface_base_url: Option<String>,
}

const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
const ANTHROPIC_DEFAULT_BASE_URL: &str = "https://api.anthropic.com/v1";
const GOOGLE_DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const VOYAGE_DEFAULT_BASE_URL: &str = "https://api.voyageai.com/v1";
const HUGGINGFACE_DEFAULT_BASE_URL: &str =
	"https://api-inference.huggingface.co/pipeline/feature-extraction";
const HUGGINGFACE_DEFAULT_GENERATION_BASE_URL: &str = "https://api-inference.huggingface.co/models";

/// Resolve OpenAI API key and base URL from overlay or environment.
pub fn openai_credentials(config: Option<&AiConfigOverlay>) -> Result<(String, String)> {
	let (api_key, base_url) = match config {
		Some(c) => (
			c.openai_api_key.clone(),
			c.openai_base_url.clone().unwrap_or_else(|| OPENAI_DEFAULT_BASE_URL.to_owned()),
		),
		None => (
			Some(std::env::var("SURREAL_AI_OPENAI_API_KEY").map_err(|_| {
				anyhow::anyhow!(
					"SURREAL_AI_OPENAI_API_KEY environment variable is not set. \
						 Set it to your OpenAI API key to use 'openai:' models."
				)
			})?),
			std::env::var("SURREAL_AI_OPENAI_BASE_URL")
				.unwrap_or_else(|_| OPENAI_DEFAULT_BASE_URL.to_owned()),
		),
	};
	let api_key = api_key.ok_or_else(|| {
		anyhow::anyhow!(
			"SURREAL_AI_OPENAI_API_KEY is not set (environment or DEFINE CONFIG AI). \
			 Set it to use 'openai:' models."
		)
	})?;
	Ok((api_key, base_url))
}

/// Resolve Anthropic API key and base URL from overlay or environment.
pub fn anthropic_credentials(config: Option<&AiConfigOverlay>) -> Result<(String, String)> {
	let (api_key, base_url) = match config {
		Some(c) => (
			c.anthropic_api_key.clone(),
			c.anthropic_base_url.clone().unwrap_or_else(|| ANTHROPIC_DEFAULT_BASE_URL.to_owned()),
		),
		None => (
			Some(std::env::var("SURREAL_AI_ANTHROPIC_API_KEY").map_err(|_| {
				anyhow::anyhow!(
					"SURREAL_AI_ANTHROPIC_API_KEY environment variable is not set. \
						 Set it to use 'anthropic:' or 'claude:' models."
				)
			})?),
			std::env::var("SURREAL_AI_ANTHROPIC_BASE_URL")
				.unwrap_or_else(|_| ANTHROPIC_DEFAULT_BASE_URL.to_owned()),
		),
	};
	let api_key = api_key.ok_or_else(|| {
		anyhow::anyhow!(
			"SURREAL_AI_ANTHROPIC_API_KEY is not set (environment or DEFINE CONFIG AI). \
			 Set it to use 'anthropic:' or 'claude:' models."
		)
	})?;
	Ok((api_key, base_url))
}

/// Resolve Google API key and base URL from overlay or environment.
pub fn google_credentials(config: Option<&AiConfigOverlay>) -> Result<(String, String)> {
	let (api_key, base_url) = match config {
		Some(c) => (
			c.google_api_key.clone(),
			c.google_base_url.clone().unwrap_or_else(|| GOOGLE_DEFAULT_BASE_URL.to_owned()),
		),
		None => (
			Some(std::env::var("SURREAL_AI_GOOGLE_API_KEY").map_err(|_| {
				anyhow::anyhow!(
					"SURREAL_AI_GOOGLE_API_KEY environment variable is not set. \
						 Set it to use 'google:' or 'gemini:' models."
				)
			})?),
			std::env::var("SURREAL_AI_GOOGLE_BASE_URL")
				.unwrap_or_else(|_| GOOGLE_DEFAULT_BASE_URL.to_owned()),
		),
	};
	let api_key = api_key.ok_or_else(|| {
		anyhow::anyhow!(
			"SURREAL_AI_GOOGLE_API_KEY is not set (environment or DEFINE CONFIG AI). \
			 Set it to use 'google:' or 'gemini:' models."
		)
	})?;
	Ok((api_key, base_url))
}

/// Resolve Voyage API key and base URL from overlay or environment.
pub fn voyage_credentials(config: Option<&AiConfigOverlay>) -> Result<(String, String)> {
	let (api_key, base_url) = match config {
		Some(c) => (
			c.voyage_api_key.clone(),
			c.voyage_base_url.clone().unwrap_or_else(|| VOYAGE_DEFAULT_BASE_URL.to_owned()),
		),
		None => (
			Some(std::env::var("SURREAL_AI_VOYAGE_API_KEY").map_err(|_| {
				anyhow::anyhow!(
					"SURREAL_AI_VOYAGE_API_KEY environment variable is not set. \
						 Set it to use 'voyage:' models."
				)
			})?),
			std::env::var("SURREAL_AI_VOYAGE_BASE_URL")
				.unwrap_or_else(|_| VOYAGE_DEFAULT_BASE_URL.to_owned()),
		),
	};
	let api_key = api_key.ok_or_else(|| {
		anyhow::anyhow!(
			"SURREAL_AI_VOYAGE_API_KEY is not set (environment or DEFINE CONFIG AI). \
			 Set it to use 'voyage:' models."
		)
	})?;
	Ok((api_key, base_url))
}

/// Resolve HuggingFace API key and base URLs from overlay or environment.
pub fn huggingface_credentials(
	config: Option<&AiConfigOverlay>,
) -> Result<(String, String, String)> {
	match config {
		Some(c) => {
			let api_key = c.huggingface_api_key.clone().unwrap_or_else(|| {
				std::env::var("SURREAL_AI_HUGGINGFACE_API_KEY").unwrap_or_default()
			});
			let base_url = c
				.huggingface_base_url
				.clone()
				.unwrap_or_else(|| HUGGINGFACE_DEFAULT_BASE_URL.to_owned());
			let generation_base_url = c
				.huggingface_base_url
				.clone()
				.unwrap_or_else(|| HUGGINGFACE_DEFAULT_GENERATION_BASE_URL.to_owned());
			Ok((api_key, base_url, generation_base_url))
		}
		None => {
			let api_key =
				std::env::var("SURREAL_AI_HUGGINGFACE_API_KEY").unwrap_or_else(|_| String::new());
			let base_url = std::env::var("SURREAL_AI_HUGGINGFACE_BASE_URL")
				.unwrap_or_else(|_| HUGGINGFACE_DEFAULT_BASE_URL.to_owned());
			let generation_base_url = std::env::var("SURREAL_AI_HUGGINGFACE_GENERATION_BASE_URL")
				.unwrap_or_else(|_| HUGGINGFACE_DEFAULT_GENERATION_BASE_URL.to_owned());
			Ok((api_key, base_url, generation_base_url))
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Mutex;

	use super::*;

	// Env-var tests must run sequentially to avoid data races on the process env.
	static ENV_LOCK: Mutex<()> = Mutex::new(());

	// ---- OpenAI ----

	#[test]
	fn openai_no_config_no_env_returns_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
			std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");
		}
		let result = openai_credentials(None);
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(msg.contains("SURREAL_AI_OPENAI_API_KEY"), "unexpected error: {msg}");
	}

	#[test]
	fn openai_env_fallback_returns_env_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_OPENAI_API_KEY", "sk-env-test");
			std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");
		}
		let (key, base) = openai_credentials(None).expect("should succeed with env key");
		assert_eq!(key, "sk-env-test");
		assert_eq!(base, OPENAI_DEFAULT_BASE_URL);
		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
		}
	}

	#[test]
	fn openai_env_fallback_custom_base_url() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_OPENAI_API_KEY", "sk-env-test");
			std::env::set_var("SURREAL_AI_OPENAI_BASE_URL", "https://custom.example.com/v1");
		}
		let (key, base) = openai_credentials(None).expect("should succeed");
		assert_eq!(key, "sk-env-test");
		assert_eq!(base, "https://custom.example.com/v1");
		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
			std::env::remove_var("SURREAL_AI_OPENAI_BASE_URL");
		}
	}

	#[test]
	fn openai_overlay_takes_priority_over_env() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_OPENAI_API_KEY", "sk-env-ignored");
		}
		let overlay = AiConfigOverlay {
			openai_api_key: Some("sk-from-config".into()),
			..Default::default()
		};
		let (key, base) = openai_credentials(Some(&overlay)).expect("should succeed");
		assert_eq!(key, "sk-from-config");
		assert_eq!(base, OPENAI_DEFAULT_BASE_URL);
		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
		}
	}

	#[test]
	fn openai_overlay_with_no_key_returns_config_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_OPENAI_API_KEY");
		}
		let overlay = AiConfigOverlay::default();
		let result = openai_credentials(Some(&overlay));
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(msg.contains("DEFINE CONFIG AI"), "expected config hint in: {msg}");
	}

	// ---- Google ----

	#[test]
	fn google_no_config_no_env_returns_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_GOOGLE_API_KEY");
			std::env::remove_var("SURREAL_AI_GOOGLE_BASE_URL");
		}
		let result = google_credentials(None);
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(msg.contains("SURREAL_AI_GOOGLE_API_KEY"), "unexpected error: {msg}");
	}

	#[test]
	fn google_env_fallback_returns_env_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_GOOGLE_API_KEY", "google-env-key");
			std::env::remove_var("SURREAL_AI_GOOGLE_BASE_URL");
		}
		let (key, base) = google_credentials(None).expect("should succeed with env key");
		assert_eq!(key, "google-env-key");
		assert_eq!(base, GOOGLE_DEFAULT_BASE_URL);
		unsafe {
			std::env::remove_var("SURREAL_AI_GOOGLE_API_KEY");
		}
	}

	#[test]
	fn google_overlay_takes_priority_over_env() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_GOOGLE_API_KEY", "google-env-ignored");
		}
		let overlay = AiConfigOverlay {
			google_api_key: Some("google-from-config".into()),
			..Default::default()
		};
		let (key, _) = google_credentials(Some(&overlay)).expect("should succeed");
		assert_eq!(key, "google-from-config");
		unsafe {
			std::env::remove_var("SURREAL_AI_GOOGLE_API_KEY");
		}
	}

	// ---- Anthropic ----

	#[test]
	fn anthropic_no_config_no_env_returns_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_ANTHROPIC_API_KEY");
			std::env::remove_var("SURREAL_AI_ANTHROPIC_BASE_URL");
		}
		let result = anthropic_credentials(None);
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(msg.contains("SURREAL_AI_ANTHROPIC_API_KEY"), "unexpected error: {msg}");
	}

	#[test]
	fn anthropic_env_fallback_returns_env_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_ANTHROPIC_API_KEY", "sk-ant-env-key");
			std::env::remove_var("SURREAL_AI_ANTHROPIC_BASE_URL");
		}
		let (key, base) = anthropic_credentials(None).expect("should succeed with env key");
		assert_eq!(key, "sk-ant-env-key");
		assert_eq!(base, ANTHROPIC_DEFAULT_BASE_URL);
		unsafe {
			std::env::remove_var("SURREAL_AI_ANTHROPIC_API_KEY");
		}
	}

	#[test]
	fn anthropic_overlay_takes_priority_over_env() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_ANTHROPIC_API_KEY", "sk-ant-env-ignored");
		}
		let overlay = AiConfigOverlay {
			anthropic_api_key: Some("sk-ant-from-config".into()),
			..Default::default()
		};
		let (key, _) = anthropic_credentials(Some(&overlay)).expect("should succeed");
		assert_eq!(key, "sk-ant-from-config");
		unsafe {
			std::env::remove_var("SURREAL_AI_ANTHROPIC_API_KEY");
		}
	}

	// ---- Voyage ----

	#[test]
	fn voyage_no_config_no_env_returns_error() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_VOYAGE_API_KEY");
			std::env::remove_var("SURREAL_AI_VOYAGE_BASE_URL");
		}
		let result = voyage_credentials(None);
		assert!(result.is_err());
		let msg = result.unwrap_err().to_string();
		assert!(msg.contains("SURREAL_AI_VOYAGE_API_KEY"), "unexpected error: {msg}");
	}

	#[test]
	fn voyage_env_fallback_returns_env_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_VOYAGE_API_KEY", "voyage-env-key");
			std::env::remove_var("SURREAL_AI_VOYAGE_BASE_URL");
		}
		let (key, base) = voyage_credentials(None).expect("should succeed with env key");
		assert_eq!(key, "voyage-env-key");
		assert_eq!(base, VOYAGE_DEFAULT_BASE_URL);
		unsafe {
			std::env::remove_var("SURREAL_AI_VOYAGE_API_KEY");
		}
	}

	#[test]
	fn voyage_overlay_takes_priority_over_env() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_VOYAGE_API_KEY", "voyage-env-ignored");
		}
		let overlay = AiConfigOverlay {
			voyage_api_key: Some("voyage-from-config".into()),
			..Default::default()
		};
		let (key, _) = voyage_credentials(Some(&overlay)).expect("should succeed");
		assert_eq!(key, "voyage-from-config");
		unsafe {
			std::env::remove_var("SURREAL_AI_VOYAGE_API_KEY");
		}
	}

	// ---- HuggingFace ----

	#[test]
	fn huggingface_no_config_no_env_returns_empty_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_API_KEY");
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_BASE_URL");
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_GENERATION_BASE_URL");
		}
		let (key, base, gen_base) = huggingface_credentials(None).expect("huggingface never fails");
		assert_eq!(key, "");
		assert_eq!(base, HUGGINGFACE_DEFAULT_BASE_URL);
		assert_eq!(gen_base, HUGGINGFACE_DEFAULT_GENERATION_BASE_URL);
	}

	#[test]
	fn huggingface_env_fallback_returns_env_key() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_HUGGINGFACE_API_KEY", "hf-env-key");
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_BASE_URL");
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_GENERATION_BASE_URL");
		}
		let (key, _, _) = huggingface_credentials(None).expect("should succeed");
		assert_eq!(key, "hf-env-key");
		unsafe {
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_API_KEY");
		}
	}

	#[test]
	fn huggingface_overlay_takes_priority_over_env() {
		let _guard = ENV_LOCK.lock().unwrap();
		unsafe {
			std::env::set_var("SURREAL_AI_HUGGINGFACE_API_KEY", "hf-env-ignored");
		}
		let overlay = AiConfigOverlay {
			huggingface_api_key: Some("hf-from-config".into()),
			..Default::default()
		};
		let (key, _, _) = huggingface_credentials(Some(&overlay)).expect("should succeed");
		assert_eq!(key, "hf-from-config");
		unsafe {
			std::env::remove_var("SURREAL_AI_HUGGINGFACE_API_KEY");
		}
	}
}
