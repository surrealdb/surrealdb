//! Sentiment‑analysis **utility layer** wrapping the low‑level BERT classifier
//! in [`crate::bert_sentiment::model`].  The goal is to provide a _turn‑key_
//! helper: *download weights from HuggingFace Hub, cache them locally, and expose
//! a few ergonomic functions for preprocessing and post‑processing*.
//!
//! ```no_run
//! use surrealml_tokenizers::{load_local_tokenizer, PresetTokenizers};
//! use crate::bert_sentiment::{encoding_to_tensors, get_sentiment_model};
//! use candle_core::Device;
//!
//! // 1 Build model & tokenizer (downloads on first use, cached thereafter).
//! let model     = get_sentiment_model(None)?;             // None = public model, no token needed
//! let tokenizer = load_local_tokenizer(
//!     PresetTokenizers::BertBaseUncased.to_string(),
//! )?;
//!
//! // 2 Encode text → tensors.
//! let enc  = tokenizer.encode("I love this movie!", true).unwrap();
//! let (ids, _type_ids, mask) = encoding_to_tensors(&enc, &Device::Cpu)?;
//!
//! // 3 Forward pass.
//! let logits = model.predict(&ids, &mask)?;               // (1, 2)
//! ```
//!
//! ## Remote model loading
//! * Model: `Tech-oriented/bert-base-uncased-finetuned-sst2` from HuggingFace Hub
//! * Format: SafeTensors (~420MB)
//! * Cached to: `~/.cache/huggingface/hub/` (standard HF cache location)
//! * `BERT_MODEL` – JSON config embedded at compile‑time (small, ~2KB)
//!
//! ## Public helpers
//! | Function                 | Purpose                                                |
//! |------------------------- |------------------------------------------------------- |
//! | `get_sentiment_model`    | Build a ready‑to‑run [`BertForSequenceClassification`] |
//! | `load_label_map`         | Extract `{id → label}` mapping from the JSON config    |
//! | `encoding_to_tensors`    | Convert *tokenizers‑rs* [`Encoding`] into Candle input |
//!
//! ---

pub mod model;
pub mod pooler;

use std::collections::HashMap;

use candle_core::{DType, Device, Result as CandleResult, Tensor};
use candle_nn::VarBuilder;
pub use candle_nn::ops::softmax;
use candle_transformers::models::bert::Config;
use serde_json;
use surrealml_llms::tensors::fetch_tensors::fetch_safetensors;
use tokenizers::Encoding; // re‑export for convenience in demos/tests

/// BERT model identifier on HuggingFace Hub
/// Using Tech-oriented version which has safetensors format
const BERT_MODEL_ID: &str = "Tech-oriented/bert-base-uncased-finetuned-sst2";
/// BERT configuration (`config.json`) embedded at compile‑time.
const BERT_MODEL: &str = include_str!("../../transformers/sent_two_config.json");

/// Build a **ready‑to‑inference** [`model::BertForSequenceClassification`].
///
/// The procedure mirrors the HuggingFace loader:
/// 1. Read the JSON config (`BERT_MODEL`).
/// 2. Download model weights from HuggingFace Hub (cached after first download).
/// 3. Load the tensors from the cached safetensors file.
/// 4. Duplicate LayerNorm `{weight,bias}` → `{gamma,beta}` for Candle.
/// 5. Ensure embedding LayerNorm exists (older checkpoints omit it).
/// 6. Feed everything into a [`VarBuilder`] and instantiate the model.
///
/// # Arguments
/// * `hf_token` - Optional HuggingFace API token (not needed for this public model).
///
/// # Returns
/// * `Ok(BertForSequenceClassification)` - Ready-to-use sentiment classifier.
/// * `Err` - If download fails or model cannot be loaded.
pub fn get_sentiment_model(
	hf_token: Option<&str>,
) -> anyhow::Result<model::BertForSequenceClassification> {
	// 1 Device – CPU by default; swap for `Device::cuda_if_available()` etc.
	let device = Device::Cpu;

	// 2 Parse BERT config.
	let cfg: Config = serde_json::from_str(BERT_MODEL)?;

	// 3 Download model weights from HuggingFace Hub (cached after first use).
	let filenames = vec!["model.safetensors".to_string()];
	let paths = fetch_safetensors(BERT_MODEL_ID, &filenames, hf_token)
		.map_err(|e| anyhow::anyhow!("Failed to fetch model weights: {}", e))?;

	// 4 Load tensors from the cached safetensors file.
	let mut tensors = candle_core::safetensors::load(&paths[0], &device)?;

	// 5 Duplicate LayerNorm parameters so both naming schemes are present.
	let mut extra = Vec::new();
	for (name, t) in tensors.iter() {
		if name.ends_with(".LayerNorm.weight") {
			extra.push((name.replace(".weight", ".gamma"), t.clone()));
		} else if name.ends_with(".LayerNorm.bias") {
			extra.push((name.replace(".bias", ".beta"), t.clone()));
		}
	}
	tensors.extend(extra);

	// 6 Ensure embedding‑norm tensors exist – some BERT variants omit them.
	let h = cfg.hidden_size as usize;
	if !tensors.contains_key("bert.embeddings.LayerNorm.gamma") {
		tensors.insert(
			"bert.embeddings.LayerNorm.gamma".into(),
			Tensor::ones(h, DType::F32, &device)?,
		);
	}
	if !tensors.contains_key("bert.embeddings.LayerNorm.beta") {
		tensors.insert(
			"bert.embeddings.LayerNorm.beta".into(),
			Tensor::zeros(h, DType::F32, &device)?,
		);
	}

	// 7 Wrap everything in a VarBuilder and instantiate the classifier.
	let vb = VarBuilder::from_tensors(tensors, DType::F32, &device);
	let model = model::BertForSequenceClassification::load(vb, &cfg, /* num_labels= */ 2)?;

	Ok(model)
}

/// Return `{class_id → human‑readable label}` as defined in the JSON config.
///
/// When fine‑tuning your own checkpoint simply ensure the `id2label` field is
/// present and this helper will pick it up automatically.
pub fn load_label_map() -> HashMap<usize, String> {
	let meta: serde_json::Value = serde_json::from_str(BERT_MODEL).unwrap_or_default();
	meta.get("id2label")
		.and_then(|m| m.as_object())
		.map(|m| {
			m.iter()
				.filter_map(|(k, v)| k.parse::<usize>().ok().zip(v.as_str().map(|s| s.to_owned())))
				.collect()
		})
		.unwrap_or_default()
}

/// Convert a *tokenizers‑rs* [`Encoding`] into Candle tensors:
/// * **`ids`** – token IDs `(1, seq_len)`  – `DType::U32`
/// * **`type_ids`** – segment IDs `(1, seq_len)` – always zero for single‑sentence tasks
/// * **`mask`** – attention mask `(1, seq_len)` – `DType::F32`
///
/// Returns a tuple `(ids, type_ids, mask)` ready for
/// [`model::BertForSequenceClassification::forward`].
pub fn encoding_to_tensors(
	enc: &Encoding,
	device: &Device,
) -> CandleResult<(Tensor, Tensor, Tensor)> {
	let ids = enc.get_ids();
	let mask = enc.get_attention_mask();
	let len = ids.len();

	// IDs & type‑ids stay in their original integer dtype (U32).
	let ids_tensor = Tensor::from_slice(ids, (1, len), device)?; // U32
	let type_ids = Tensor::zeros((1, len), DType::U32, device)?; // U32

	// Attention mask is expected as F32 by Candle's BERT implementation.
	let mask_tensor = Tensor::from_slice(mask, (1, len), device)?.to_dtype(DType::F32)?;

	Ok((ids_tensor, type_ids, mask_tensor))
}

#[cfg(test)]
mod tests {
	use surrealml_tokenizers::{PresetTokenizers, load_local_tokenizer};

	use super::*;

	/// The model should classify clear‑cut positive/negative phrases correctly
	/// with high confidence (>95 %).
	#[test]
	fn test_extreme_sentiment() -> anyhow::Result<()> {
		let model = get_sentiment_model(None)?;
		let tokenizer = load_local_tokenizer(PresetTokenizers::BertBaseUncased.to_string())?;

		let samples = [
			("I absolutely love this fantastic wonderful amazing incredible movie", "positive"),
			("I completely hate this terrible awful horrible disgusting worst movie", "negative"),
			("This movie is good", "positive"),
			("This movie is bad", "negative"),
			("okay", "positive"),
			("great", "positive"),
			("terrible", "negative"),
		];

		for (text, expected_label) in samples.iter() {
			// here is where the host code runs with the text from the WASM module
			let enc = tokenizer.encode(*text, true).unwrap();
			let (ids, _, mask) = encoding_to_tensors(&enc, &Device::Cpu)?;
			let logits = model.predict(&ids, &mask)?;

			// Convert logits → probabilities.
			let probs = softmax(&logits, 1)?.squeeze(0)?.to_vec1::<f32>()?;
			let (pred_idx, confidence) = probs
				.iter()
				.copied()
				.enumerate()
				.max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
				.unwrap();

			// this is what you return to the user's WASM module
			let predicted_label = if pred_idx == 0 {
				"negative"
			} else {
				"positive"
			};

			// --- Assertions --------------------------------------------------
			assert_eq!(predicted_label, *expected_label, "{}", text);
			assert!(confidence > 0.95, "low confidence {:.2} for \"{}\"", confidence, text);
		}

		Ok(())
	}
}
