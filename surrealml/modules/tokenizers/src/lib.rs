mod error;
#[cfg(feature = "http-access")]
mod fetch_tokenizer;
mod preset_tokenizers;
mod tokenizer;

pub use crate::error::SurrealError;
pub use preset_tokenizers::PresetTokenizers;
pub use tokenizer::{decode, encode, load_local_tokenizer};
pub use tokenizers::Tokenizer;

#[cfg(feature = "http-access")]
pub use tokenizer::load_tokenizer_with_http;
