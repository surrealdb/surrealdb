//! Utilities for working with **preset** Hugging Face model identifiers.
use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

use tokenizers::Tokenizer;

use crate::error::{SurrealError, SurrealErrorStatus};

/// Model identifier files embedded in the binary.
const MIXTRAL_8X7B_V01: &str =
	include_str!("../tokenizers/mistralai-Mixtral-8x7B-v0.1-tokenizer.json");
const MISTRAL_7B_V01: &str = include_str!("../tokenizers/mistralai-Mistral-7B-v0.1-tokenizer.json");
const MISTRALLITE: &str = include_str!("../tokenizers/amazon-MistralLite-tokenizer.json");
const GEMMA_7B: &str = include_str!("../tokenizers/google-gemma-7b-tokenizer.json");
const GEMMA_2B: &str = include_str!("../tokenizers/google-gemma-2b-tokenizer.json");
const GEMMA_3_4B_IT: &str = include_str!("../tokenizers/google-gemma-3-4b-it-tokenizer.json");
const FALCON_7B: &str = include_str!("../tokenizers/tiiuae-falcon-7b-tokenizer.json");
const BERT_BASE_UNCASED: &str =
	include_str!("../tokenizers/google-bert-base-uncased-tokenizer.json");

// const MISTRAL_7B_V01: &str =
//     include_str!("../tokenizers/mistralai-Mistral-7B-v0.1-tokenizer.json");
// const MISTRALLITE: &str = include_str!("../tokenizers/amazon-MistralLite-tokenizer.json");
// const GEMMA_7B: &str = include_str!("../tokenizers/google-gemma-7b-tokenizer.json");
// const GEMMA_2B: &str = include_str!("../tokenizers/google-gemma-2b-tokenizer.json");
// const GEMMA_3_4B_IT: &str =
//     include_str!("../tokenizers/google-gemma-3-4b-it-tokenizer.json");
// const FALCON_7B: &str = include_str!("../tokenizers/tiiuae-falcon-7b-tokenizer.json");

/// Identifiers for the built-in models bundled with this crate.
///
/// # Variants
/// * `Mixtral8x7Bv01` — `mistralai/Mixtral-8x7B-v0.1`
/// * `Mistral7Bv01` — `mistralai/Mistral-7B-v0.1`
/// * `MistralLite` — `amazon/MistralLite`
/// * `Gemma7B` — `google/gemma-7b`
/// * `Gemma2B` — `google/gemma-2b`
/// * `Gemma3_4BIt` — `google/gemma-3-4b-it`
/// * `Falcon7B` — `tiiuae/falcon-7b`
/// * `BertBaseUncased` — `google-bert/bert-base-uncased`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PresetTokenizers {
	Mixtral8x7Bv01,
	Mistral7Bv01,
	MistralLite,
	Gemma7B,
	Gemma2B,
	Gemma3_4BIt,
	Falcon7B,
	BertBaseUncased,
}

impl TryFrom<&str> for PresetTokenizers {
	type Error = String;

	fn try_from(value: &str) -> Result<Self, Self::Error> {
		match value {
			"mistralai/Mixtral-8x7B-v0.1" => Ok(PresetTokenizers::Mixtral8x7Bv01),
			"mistralai/Mistral-7B-v0.1" => Ok(PresetTokenizers::Mistral7Bv01),
			"amazon/MistralLite" => Ok(PresetTokenizers::MistralLite),
			"google/gemma-7b" => Ok(PresetTokenizers::Gemma7B),
			"google/gemma-2b" => Ok(PresetTokenizers::Gemma2B),
			"google/gemma-3-4b-it" => Ok(PresetTokenizers::Gemma3_4BIt),
			"tiiuae/falcon-7b" => Ok(PresetTokenizers::Falcon7B),
			"google-bert/bert-base-uncased" => Ok(PresetTokenizers::BertBaseUncased),
			_ => Err(format!("{} is not a preset tokenizer", value)),
		}
	}
}

impl fmt::Display for PresetTokenizers {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let s = match self {
			PresetTokenizers::Mixtral8x7Bv01 => "mistralai/Mixtral-8x7B-v0.1",
			PresetTokenizers::Mistral7Bv01 => "mistralai/Mistral-7B-v0.1",
			PresetTokenizers::MistralLite => "amazon/MistralLite",
			PresetTokenizers::Gemma7B => "google/gemma-7b",
			PresetTokenizers::Gemma2B => "google/gemma-2b",
			PresetTokenizers::Gemma3_4BIt => "google/gemma-3-4b-it",
			PresetTokenizers::Falcon7B => "tiiuae/falcon-7b",
			PresetTokenizers::BertBaseUncased => "google-bert/bert-base-uncased",
		};
		write!(f, "{s}")
	}
}

impl PresetTokenizers {
	/// Retrieve the embedded tokenizer identifier for this variant.
	///
	/// # Returns
	/// * `Result<Tokenizer, SurrealError> - A fully initialised [`Tokenizer`] ready for encoding
	///   and decoding.
	pub fn retrieve_tokenizer(&self) -> Result<Tokenizer, SurrealError> {
		let data: &'static str = match self {
			PresetTokenizers::Mixtral8x7Bv01 => MIXTRAL_8X7B_V01,
			PresetTokenizers::Mistral7Bv01 => MISTRAL_7B_V01,
			PresetTokenizers::MistralLite => MISTRALLITE,
			PresetTokenizers::Gemma7B => GEMMA_7B,
			PresetTokenizers::Gemma2B => GEMMA_2B,
			PresetTokenizers::Gemma3_4BIt => GEMMA_3_4B_IT,
			PresetTokenizers::Falcon7B => FALCON_7B,
			PresetTokenizers::BertBaseUncased => BERT_BASE_UNCASED,
		};

		Tokenizer::from_str(data).map_err(|e| {
			SurrealError::new(
				format!("Failed to parse preset tokenizer: {}", e),
				SurrealErrorStatus::BadRequest,
			)
		})
	}
}

impl FromStr for PresetTokenizers {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		s.try_into()
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::PresetTokenizers;

	macro_rules! assert_tokenizers {
        ($($model_name:expr => $token:expr),*) => {
            $(
                assert_eq!(
                    PresetTokenizers::from_str($model_name),
                    Ok($token)
                );
            )*
        };
    }

	#[test]
	fn from_str_recognises_valid_model_names() {
		// Each known model string should map to the correct enum variant.
		assert_tokenizers!(
			"mistralai/Mixtral-8x7B-v0.1" => PresetTokenizers::Mixtral8x7Bv01,
			"mistralai/Mistral-7B-v0.1" => PresetTokenizers::Mistral7Bv01,
			"amazon/MistralLite" => PresetTokenizers::MistralLite,
			"google/gemma-7b" => PresetTokenizers::Gemma7B,
			"google/gemma-2b" => PresetTokenizers::Gemma2B,
			"google/gemma-3-4b-it" => PresetTokenizers::Gemma3_4BIt,
			"tiiuae/falcon-7b" => PresetTokenizers::Falcon7B,
			"google-bert/bert-base-uncased" => PresetTokenizers::BertBaseUncased,
			"tiiuae/falcon-7b" => PresetTokenizers::Falcon7B
		);
	}

	#[test]
	fn from_str_unknown_model_returns_error() {
		assert!(PresetTokenizers::from_str("some-random-model").is_err());
	}

	#[test]
	fn presets_load_successfully() {
		let presets = [
			PresetTokenizers::Mixtral8x7Bv01,
			PresetTokenizers::Mistral7Bv01,
			PresetTokenizers::MistralLite,
			PresetTokenizers::Gemma7B,
			PresetTokenizers::Gemma2B,
			PresetTokenizers::Gemma3_4BIt,
			PresetTokenizers::Falcon7B,
			PresetTokenizers::BertBaseUncased,
		];

		for preset in presets {
			println!("Testing preset: {:?}", preset);
			// Should produce Ok(Tokenizer)
			let tok = preset.retrieve_tokenizer().expect("preset tokenizer must load");

			// Sanity: tokenizer should yield at least one token for a short input
			let enc = tok.encode("test", true).unwrap();
			assert!(!enc.get_ids().is_empty(), "produced empty token sequence");
		}
	}
}
