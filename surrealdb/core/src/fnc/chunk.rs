//! SurrealQL `ai::chunk::*` function implementations.
//!
//! Provides text chunking strategies for RAG pipelines:
//! - `ai::chunk::fixed` — fixed-size character windows
//! - `ai::chunk::sentence` — sentence-boundary splitting
//! - `ai::chunk::paragraph` — paragraph-boundary splitting
//! - `ai::chunk::recursive` — recursive splitting (paragraph → sentence → fixed)
//! - `ai::chunk::semantic` — embedding-similarity-based splitting

#[cfg(feature = "ai")]
use anyhow::Result;

use crate::err::Error;
use crate::fnc::args::Optional;
use crate::val::Value;

/// Split text into sentences using a simple heuristic.
///
/// Splits on `.`, `!`, or `?` when followed by whitespace or end-of-string,
/// while avoiding splits on common abbreviations.
#[cfg(any(feature = "ai", test))]
pub(crate) fn split_sentences(text: &str) -> Vec<&str> {
	let mut sentences = Vec::new();
	let bytes = text.as_bytes();
	let len = bytes.len();
	let mut start = 0;

	let mut i = 0;
	while i < len {
		let b = bytes[i];
		if b == b'.' || b == b'!' || b == b'?' {
			// Look ahead: the sentence ends if this terminator is followed by
			// whitespace, end-of-string, or a quote/paren then whitespace.
			let mut end = i + 1;
			// Skip closing quotes or parentheses after the terminator
			while end < len && matches!(bytes[end], b'"' | b'\'' | b')' | b']') {
				end += 1;
			}
			if end >= len || bytes[end].is_ascii_whitespace() {
				// Skip common abbreviations for '.' only
				if b == b'.' {
					let word_start = text[start..i].rfind(char::is_whitespace).map_or(start, |p| {
						// p is relative to start, so adjust
						start + p + 1
					});
					let word = &text[word_start..i];
					let lower = word.to_ascii_lowercase();
					if matches!(
						lower.as_str(),
						"mr" | "mrs"
							| "ms" | "dr" | "prof"
							| "sr" | "jr" | "vs" | "etc"
							| "inc" | "ltd" | "st"
							| "e.g" | "i.e" | "fig"
							| "vol" | "no" | "approx"
							| "dept" | "est" | "gen"
							| "gov"
					) {
						i += 1;
						continue;
					}
				}

				let sentence = text[start..end].trim();
				if !sentence.is_empty() {
					sentences.push(sentence);
				}
				// Advance past trailing whitespace
				while end < len && bytes[end].is_ascii_whitespace() {
					end += 1;
				}
				start = end;
				i = end;
				continue;
			}
		}
		i += 1;
	}

	// Remaining text
	let rest = text[start..].trim();
	if !rest.is_empty() {
		sentences.push(rest);
	}

	sentences
}

/// Split text into paragraphs on blank-line boundaries (`\n\n`).
#[cfg(any(feature = "ai", test))]
pub(crate) fn split_paragraphs(text: &str) -> Vec<&str> {
	let mut paragraphs = Vec::new();
	let mut rest = text;

	loop {
		// Find the next blank-line boundary
		let boundary = rest.find("\n\n").or_else(|| rest.find("\r\n\r\n"));
		match boundary {
			Some(pos) => {
				let para = rest[..pos].trim();
				if !para.is_empty() {
					paragraphs.push(para);
				}
				// Skip past the blank line(s)
				let after = &rest[pos..];
				let skip = after.find(|c: char| c != '\n' && c != '\r').unwrap_or(after.len());
				rest = &rest[pos + skip..];
			}
			None => {
				let para = rest.trim();
				if !para.is_empty() {
					paragraphs.push(para);
				}
				break;
			}
		}
	}

	paragraphs
}

/// Group text segments into chunks up to `max_size` characters.
///
/// When `overlap` > 0, the last `overlap` segments of each chunk are
/// prepended to the next chunk.
#[cfg(any(feature = "ai", test))]
pub(crate) fn group_segments(segments: &[&str], max_size: usize, overlap: usize) -> Vec<String> {
	if segments.is_empty() {
		return Vec::new();
	}
	if max_size == 0 {
		return segments.iter().map(|s| (*s).to_string()).collect();
	}

	let mut chunks = Vec::new();
	let mut i = 0;

	while i < segments.len() {
		let mut chunk = String::new();
		let mut count = 0;
		let chunk_start = i;

		while i < segments.len() {
			let sep = if chunk.is_empty() {
				""
			} else {
				" "
			};
			let needed = sep.len() + segments[i].len();
			if !chunk.is_empty() && chunk.len() + needed > max_size {
				break;
			}
			if !chunk.is_empty() {
				chunk.push(' ');
			}
			chunk.push_str(segments[i]);
			count += 1;
			i += 1;
		}

		chunks.push(chunk);

		// Apply overlap: back up by `overlap` segments
		if overlap > 0 && i < segments.len() {
			let back = overlap.min(count);
			i = i.saturating_sub(back);
			// Ensure forward progress
			if i <= chunk_start {
				i = chunk_start + 1;
			}
		}
	}

	chunks
}

/// Split text into fixed-size character windows.
#[cfg(any(feature = "ai", test))]
fn chunk_fixed(text: &str, size: usize, overlap: usize) -> Vec<String> {
	if text.is_empty() || size == 0 {
		return Vec::new();
	}
	let step = size.saturating_sub(overlap).max(1);
	let chars: Vec<char> = text.chars().collect();
	let mut chunks = Vec::new();
	let mut start = 0;

	while start < chars.len() {
		let end = (start + size).min(chars.len());
		let chunk: String = chars[start..end].iter().collect();
		let trimmed = chunk.trim();
		if !trimmed.is_empty() {
			chunks.push(trimmed.to_string());
		}
		start += step;
	}

	chunks
}

/// Recursively chunk text: try paragraphs first, then sentences, then fixed.
#[cfg(any(feature = "ai", test))]
fn chunk_recursive(text: &str, size: usize, overlap: usize) -> Vec<String> {
	let paragraphs = split_paragraphs(text);

	let mut result = Vec::new();
	for para in paragraphs {
		if para.len() <= size {
			result.push(para.to_string());
		} else {
			// Try sentence splitting
			let sentences = split_sentences(para);
			for sent in &sentences {
				if sent.len() <= size {
					result.push((*sent).to_string());
				} else {
					// Fall back to fixed-size splitting
					result.extend(chunk_fixed(sent, size, overlap));
				}
			}
		}
	}

	// Re-group small consecutive chunks up to the target size with overlap
	if result.is_empty() {
		return result;
	}
	let refs: Vec<&str> = result.iter().map(|s| s.as_str()).collect();
	group_segments(&refs, size, overlap)
}

#[cfg(feature = "ai")]
fn validate_size(fn_name: &str, size: i64) -> Result<usize> {
	if size <= 0 {
		anyhow::bail!(Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("The size argument must be a positive integer, got: {size}"),
		});
	}
	Ok(size as usize)
}

#[cfg(feature = "ai")]
fn validate_overlap(fn_name: &str, overlap: i64, size: usize) -> Result<usize> {
	if overlap < 0 {
		anyhow::bail!(Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("The overlap argument must be a non-negative integer, got: {overlap}"),
		});
	}
	let ov = overlap as usize;
	if ov >= size {
		anyhow::bail!(Error::InvalidFunctionArguments {
			name: fn_name.to_owned(),
			message: format!("The overlap ({ov}) must be less than the size ({size})"),
		});
	}
	Ok(ov)
}

#[cfg(feature = "ai")]
fn chunks_to_value(chunks: Vec<String>) -> Value {
	let arr: Vec<Value> = chunks.into_iter().map(Value::from).collect();
	Value::Array(arr.into())
}

// =========================================================================
// ai::chunk::fixed
// =========================================================================

pub mod fixed {
	use anyhow::Result;

	use super::*;

	/// Split text into fixed-size character windows.
	///
	/// # SurrealQL
	///
	/// ```surql
	/// ai::chunk::fixed('Some long document text...', 500)
	/// ai::chunk::fixed('Some long document text...', 500, 50)
	/// ```
	///
	/// Returns an `array<string>` of text chunks.
	#[cfg(not(feature = "ai"))]
	pub fn run((_text, _size, _overlap): (String, i64, Optional<i64>)) -> Result<Value> {
		anyhow::bail!(Error::AiDisabled)
	}

	#[cfg(feature = "ai")]
	pub fn run((text, size, overlap): (String, i64, Optional<i64>)) -> Result<Value> {
		let size = validate_size("ai::chunk::fixed", size)?;
		let overlap = match overlap.0 {
			Some(o) => validate_overlap("ai::chunk::fixed", o, size)?,
			None => 0,
		};
		Ok(chunks_to_value(chunk_fixed(&text, size, overlap)))
	}
}

// =========================================================================
// ai::chunk::sentence
// =========================================================================

pub mod sentence {
	use anyhow::Result;

	use super::*;

	/// Split text on sentence boundaries, optionally grouping by max size.
	///
	/// # SurrealQL
	///
	/// ```surql
	/// ai::chunk::sentence('First. Second. Third.')
	/// ai::chunk::sentence('First. Second. Third.', 500)
	/// ai::chunk::sentence('First. Second. Third.', 500, 1)
	/// ```
	///
	/// Returns an `array<string>` of text chunks.
	#[cfg(not(feature = "ai"))]
	pub fn run((_text, _size, _overlap): (String, Optional<i64>, Optional<i64>)) -> Result<Value> {
		anyhow::bail!(Error::AiDisabled)
	}

	#[cfg(feature = "ai")]
	pub fn run((text, size, overlap): (String, Optional<i64>, Optional<i64>)) -> Result<Value> {
		let sentences = split_sentences(&text);

		match size.0 {
			Some(max) => {
				let max = validate_size("ai::chunk::sentence", max)?;
				let overlap = match overlap.0 {
					Some(o) => validate_overlap("ai::chunk::sentence", o, max)?,
					None => 0,
				};
				Ok(chunks_to_value(group_segments(&sentences, max, overlap)))
			}
			None => {
				let chunks: Vec<String> = sentences.into_iter().map(|s| s.to_string()).collect();
				Ok(chunks_to_value(chunks))
			}
		}
	}
}

// =========================================================================
// ai::chunk::paragraph
// =========================================================================

pub mod paragraph {
	use anyhow::Result;

	use super::*;

	/// Split text on paragraph boundaries, optionally grouping by max size.
	///
	/// # SurrealQL
	///
	/// ```surql
	/// ai::chunk::paragraph($text)
	/// ai::chunk::paragraph($text, 1000)
	/// ai::chunk::paragraph($text, 1000, 1)
	/// ```
	///
	/// Returns an `array<string>` of text chunks.
	#[cfg(not(feature = "ai"))]
	pub fn run((_text, _size, _overlap): (String, Optional<i64>, Optional<i64>)) -> Result<Value> {
		anyhow::bail!(Error::AiDisabled)
	}

	#[cfg(feature = "ai")]
	pub fn run((text, size, overlap): (String, Optional<i64>, Optional<i64>)) -> Result<Value> {
		let paragraphs = split_paragraphs(&text);

		match size.0 {
			Some(max) => {
				let max = validate_size("ai::chunk::paragraph", max)?;
				let overlap = match overlap.0 {
					Some(o) => validate_overlap("ai::chunk::paragraph", o, max)?,
					None => 0,
				};
				Ok(chunks_to_value(group_segments(&paragraphs, max, overlap)))
			}
			None => {
				let chunks: Vec<String> = paragraphs.into_iter().map(|s| s.to_string()).collect();
				Ok(chunks_to_value(chunks))
			}
		}
	}
}

// =========================================================================
// ai::chunk::recursive
// =========================================================================

pub mod recursive {
	use anyhow::Result;

	use super::*;

	/// Recursively chunk text: tries paragraph, then sentence, then fixed.
	///
	/// # SurrealQL
	///
	/// ```surql
	/// ai::chunk::recursive($text, 500)
	/// ai::chunk::recursive($text, 500, 50)
	/// ```
	///
	/// Returns an `array<string>` of text chunks.
	#[cfg(not(feature = "ai"))]
	pub fn run((_text, _size, _overlap): (String, i64, Optional<i64>)) -> Result<Value> {
		anyhow::bail!(Error::AiDisabled)
	}

	#[cfg(feature = "ai")]
	pub fn run((text, size, overlap): (String, i64, Optional<i64>)) -> Result<Value> {
		let size = validate_size("ai::chunk::recursive", size)?;
		let overlap = match overlap.0 {
			Some(o) => validate_overlap("ai::chunk::recursive", o, size)?,
			None => 0,
		};
		Ok(chunks_to_value(chunk_recursive(&text, size, overlap)))
	}
}

// =========================================================================
// ai::chunk::semantic
// =========================================================================

#[cfg(not(feature = "ai"))]
pub mod semantic {
	use anyhow::Result;

	use crate::ctx::FrozenContext;
	use crate::dbs::Options;
	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::val::Value;

	pub async fn run(
		_: (&FrozenContext, &Options),
		(_model_id, _text, _config): (String, String, Optional<Value>),
	) -> Result<Value> {
		anyhow::bail!(Error::AiDisabled)
	}
}

#[cfg(feature = "ai")]
pub mod semantic {
	use anyhow::Result;

	use crate::ctx::FrozenContext;
	use crate::dbs::Options;
	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::val::Value;

	const DEFAULT_THRESHOLD: f64 = 0.5;

	fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
		let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
		let mag_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
		let mag_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
		if mag_a == 0.0 || mag_b == 0.0 {
			return 0.0;
		}
		dot / (mag_a * mag_b)
	}

	fn parse_semantic_config(config: Option<Value>) -> Result<(Option<usize>, f64)> {
		match config {
			None => Ok((None, DEFAULT_THRESHOLD)),
			Some(Value::Object(obj)) => {
				let size = match obj.get("size") {
					Some(Value::Number(n)) => {
						let v = n.as_int();
						if v <= 0 {
							anyhow::bail!(Error::InvalidFunctionArguments {
								name: "ai::chunk::semantic".to_owned(),
								message: format!(
									"The 'size' config field must be a positive integer, got: {v}"
								),
							});
						}
						Some(v as usize)
					}
					Some(_) => {
						anyhow::bail!(Error::InvalidFunctionArguments {
							name: "ai::chunk::semantic".to_owned(),
							message: "The 'size' config field must be a number".to_owned(),
						})
					}
					None => None,
				};

				let threshold = match obj.get("threshold") {
					Some(Value::Number(n)) => {
						let v = n.as_float();
						if !(0.0..=1.0).contains(&v) {
							anyhow::bail!(Error::InvalidFunctionArguments {
								name: "ai::chunk::semantic".to_owned(),
								message: format!(
									"The 'threshold' config field must be between 0.0 and 1.0, got: {v}"
								),
							});
						}
						v
					}
					Some(_) => {
						anyhow::bail!(Error::InvalidFunctionArguments {
							name: "ai::chunk::semantic".to_owned(),
							message: "The 'threshold' config field must be a number".to_owned(),
						})
					}
					None => DEFAULT_THRESHOLD,
				};

				Ok((size, threshold))
			}
			Some(v) => {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "ai::chunk::semantic".to_owned(),
					message: format!("The config argument must be an object, got: {}", v.kind_of()),
				})
			}
		}
	}

	/// Split text at semantic boundaries using embedding similarity.
	///
	/// # SurrealQL
	///
	/// ```surql
	/// ai::chunk::semantic('openai:text-embedding-3-small', $text)
	/// ai::chunk::semantic('openai:text-embedding-3-small', $text, { threshold: 0.5 })
	/// ```
	///
	/// Returns an `array<string>` of text chunks.
	pub async fn run(
		(ctx, opt): (&FrozenContext, &Options),
		(model_id, text, config): (String, String, Optional<Value>),
	) -> Result<Value> {
		use crate::catalog::providers::DatabaseProvider;

		let (max_size, threshold) = parse_semantic_config(config.0)?;

		// Split into sentences first
		let sentences = super::split_sentences(&text);
		if sentences.len() <= 1 {
			let chunks: Vec<String> = sentences.into_iter().map(|s| s.to_string()).collect();
			return Ok(super::chunks_to_value(chunks));
		}

		// Load AI config overlay
		let ai_config = {
			let overlay = async {
				let (ns, db) = ctx.try_ns_db_ids(opt).await.ok().flatten()?;
				let txn = ctx.tx();
				let config = txn.get_db_config(ns, db, "ai").await.ok().flatten()?;
				let catalog_ai = config.try_as_ai().ok()?;
				Some(crate::ai::config::AiConfigOverlay {
					openai_api_key: catalog_ai.openai_api_key.clone(),
					openai_base_url: catalog_ai.openai_base_url.clone(),
					anthropic_api_key: catalog_ai.anthropic_api_key.clone(),
					anthropic_base_url: catalog_ai.anthropic_base_url.clone(),
					google_api_key: catalog_ai.google_api_key.clone(),
					google_base_url: catalog_ai.google_base_url.clone(),
					voyage_api_key: catalog_ai.voyage_api_key.clone(),
					voyage_base_url: catalog_ai.voyage_base_url.clone(),
					huggingface_api_key: catalog_ai.huggingface_api_key.clone(),
					huggingface_base_url: catalog_ai.huggingface_base_url.clone(),
				})
			};
			overlay.await
		};

		// Generate embeddings for each sentence
		let mut embeddings = Vec::with_capacity(sentences.len());
		for sent in &sentences {
			let emb = crate::ai::embed::embed(&model_id, sent, ai_config.as_ref()).await?;
			embeddings.push(emb);
		}

		// Find split points where cosine similarity drops below threshold
		let mut groups: Vec<Vec<&str>> = Vec::new();
		let mut current_group: Vec<&str> = vec![sentences[0]];

		for i in 1..sentences.len() {
			let sim = cosine_similarity(&embeddings[i - 1], &embeddings[i]);
			if sim < threshold {
				groups.push(current_group);
				current_group = vec![sentences[i]];
			} else {
				current_group.push(sentences[i]);
			}
		}
		groups.push(current_group);

		// Join each group and optionally enforce max size
		let mut chunks: Vec<String> = Vec::new();
		for group in groups {
			let joined = group.join(" ");
			match max_size {
				Some(max) if joined.len() > max => {
					chunks.extend(super::group_segments(&group, max, 0));
				}
				_ => {
					chunks.push(joined);
				}
			}
		}

		Ok(super::chunks_to_value(chunks))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_split_sentences_basic() {
		let sentences = split_sentences("Hello world. How are you? I am fine!");
		assert_eq!(sentences, vec!["Hello world.", "How are you?", "I am fine!"]);
	}

	#[test]
	fn test_split_sentences_abbreviations() {
		let sentences = split_sentences("Dr. Smith went home. He was tired.");
		assert_eq!(sentences, vec!["Dr. Smith went home.", "He was tired."]);
	}

	#[test]
	fn test_split_sentences_no_trailing_space() {
		let sentences = split_sentences("One sentence.");
		assert_eq!(sentences, vec!["One sentence."]);
	}

	#[test]
	fn test_split_sentences_no_terminator() {
		let sentences = split_sentences("No terminator here");
		assert_eq!(sentences, vec!["No terminator here"]);
	}

	#[test]
	fn test_split_paragraphs_basic() {
		let text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph.";
		let paras = split_paragraphs(text);
		assert_eq!(paras, vec!["First paragraph.", "Second paragraph.", "Third paragraph."]);
	}

	#[test]
	fn test_split_paragraphs_crlf() {
		let text = "First.\r\n\r\nSecond.";
		let paras = split_paragraphs(text);
		assert_eq!(paras, vec!["First.", "Second."]);
	}

	#[test]
	fn test_group_segments_basic() {
		let segs = vec!["Hello world.", "How are you?", "I am fine."];
		let chunks = group_segments(&segs, 30, 0);
		assert_eq!(chunks, vec!["Hello world. How are you?", "I am fine."]);
	}

	#[test]
	fn test_group_segments_with_overlap() {
		let segs = vec!["A.", "B.", "C.", "D."];
		let chunks = group_segments(&segs, 6, 1);
		// "A. B." (5 chars), overlap 1 -> back to "B."
		// "B. C." (5 chars), overlap 1 -> back to "C."
		// "C. D." (5 chars)
		assert_eq!(chunks, vec!["A. B.", "B. C.", "C. D."]);
	}

	#[test]
	fn test_chunk_fixed_basic() {
		let text = "abcdefghij";
		let chunks = chunk_fixed(text, 4, 0);
		assert_eq!(chunks, vec!["abcd", "efgh", "ij"]);
	}

	#[test]
	fn test_chunk_fixed_with_overlap() {
		let text = "abcdefghij";
		let chunks = chunk_fixed(text, 4, 2);
		assert_eq!(chunks, vec!["abcd", "cdef", "efgh", "ghij", "ij"]);
	}

	#[test]
	fn test_chunk_recursive() {
		let text = "Short paragraph.\n\nAnother short one.";
		let chunks = chunk_recursive(text, 100, 0);
		assert_eq!(chunks, vec!["Short paragraph. Another short one."]);
	}

	#[test]
	fn test_chunk_recursive_splits_large_paragraph() {
		let text = "First sentence. Second sentence. Third sentence.";
		let chunks = chunk_recursive(text, 20, 0);
		assert!(chunks.len() >= 2, "Expected multiple chunks, got: {chunks:?}");
		for chunk in &chunks {
			assert!(chunk.len() <= 20, "Chunk too large: '{chunk}' ({} chars)", chunk.len());
		}
	}
}
