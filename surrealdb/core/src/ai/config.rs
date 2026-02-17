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
	pub google_api_key: Option<String>,
	pub google_base_url: Option<String>,
	pub voyage_api_key: Option<String>,
	pub voyage_base_url: Option<String>,
	pub huggingface_api_key: Option<String>,
	pub huggingface_base_url: Option<String>,
}

const OPENAI_DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
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
						 Set it to use 'voyage:', 'claude:', or 'anthropic:' models."
				)
			})?),
			std::env::var("SURREAL_AI_VOYAGE_BASE_URL")
				.unwrap_or_else(|_| VOYAGE_DEFAULT_BASE_URL.to_owned()),
		),
	};
	let api_key = api_key.ok_or_else(|| {
		anyhow::anyhow!(
			"SURREAL_AI_VOYAGE_API_KEY is not set (environment or DEFINE CONFIG AI). \
			 Set it to use 'voyage:', 'claude:', or 'anthropic:' models."
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
