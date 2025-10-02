use anyhow::Context;
use revision::Revisioned;

pub fn decode<D: Revisioned>(val: &[u8]) -> anyhow::Result<D> {
	revision::from_slice(val).context("Failed to deserialize revision payload")
}

pub fn encode<S: Revisioned>(val: &S) -> anyhow::Result<Vec<u8>> {
	revision::to_vec(val).context("Failed to serialize revision payload")
}
