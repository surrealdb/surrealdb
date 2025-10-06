use anyhow::Context;
use revision::{DeserializeRevisioned, SerializeRevisioned};

pub fn decode<D: DeserializeRevisioned>(val: &[u8]) -> anyhow::Result<D> {
	revision::from_slice(val).context("Failed to deserialize revision payload")
}

pub fn encode<S: SerializeRevisioned>(val: &S) -> anyhow::Result<Vec<u8>> {
	revision::to_vec(val).context("Failed to serialize revision payload")
}
