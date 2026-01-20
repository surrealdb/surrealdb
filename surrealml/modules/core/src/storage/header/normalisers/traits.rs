//! traits for the normalisers module.

pub trait Normaliser {
    /// Normalises a value.
    fn normalise(&self, input: f32) -> f32;

    fn inverse_normalise(&self, input: f32) -> f32 {
        input
    }

    /// Returns the key of the normaliser.
    fn key() -> String;
}
