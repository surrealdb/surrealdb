//! Defines the key bindings for input data.
use std::collections::HashMap;
use std::fmt;

use crate::errors::error::{SurrealError, SurrealErrorStatus};
use crate::safe_eject_internal;

/// Defines the key bindings for input data.
///
/// # Fields
/// * `store` - A vector of strings that represent the column names. The order of this store is the same as the order
///   in which the columns are expected in the input data.
/// * `reference` - A hashmap that maps the column names to their index in the `self.store` field.
#[derive(Debug, PartialEq)]
pub struct KeyBindings {
    pub store: Vec<String>,
    pub reference: HashMap<String, usize>,
}

impl KeyBindings {
    /// Creates a new key bindings with no columns.
    ///
    /// # Returns
    /// A new key bindings with no columns.
    pub fn fresh() -> Self {
        KeyBindings {
            store: Vec::new(),
            reference: HashMap::new(),
        }
    }

    /// Adds a column name to the `self.store` field. It must be noted that the order in which the columns are added is
    /// the order in which they will be expected in the input data.
    ///
    /// # Arguments
    /// * `column_name` - The name of the column to be added.
    pub fn add_column(&mut self, column_name: String) {
        let index = self.store.len();
        self.store.push(column_name.clone());
        self.reference.insert(column_name, index);
    }

    /// Constructs the key bindings from a string.
    ///
    /// # Arguments
    /// * `data` - The string to be converted into key bindings.
    ///
    /// # Returns
    /// The key bindings constructed from the string.
    pub fn from_string(data: String) -> Self {
        if data.is_empty() {
            return KeyBindings::fresh();
        }
        let mut store = Vec::new();
        let mut reference = HashMap::new();

        let lines = data.split("=>");
        let mut count = 0;

        // I'm referencing count outside of the loop and this confuses clippy
        #[allow(clippy::explicit_counter_loop)]
        for line in lines {
            store.push(line.to_string());
            reference.insert(line.to_string(), count);
            count += 1;
        }
        KeyBindings { store, reference }
    }

    /// Constructs the key bindings from bytes.
    ///
    /// # Arguments
    /// * `data` - The bytes to be converted into key bindings.
    ///
    /// # Returns
    /// The key bindings constructed from the bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, SurrealError> {
        let data = safe_eject_internal!(String::from_utf8(data.to_vec()));
        Ok(Self::from_string(data))
    }

    /// Converts the key bindings to bytes.
    ///
    /// # Returns
    /// The key bindings as bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl fmt::Display for KeyBindings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.store.join("=>"))
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;

    pub fn generate_string() -> String {
        "a=>b=>c=>d=>e=>f".to_string()
    }

    pub fn generate_bytes() -> Vec<u8> {
        "a=>b=>c=>d=>e=>f".to_string().into_bytes()
    }

    fn generate_struct() -> KeyBindings {
        let store = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
        ];
        let mut reference = HashMap::new();
        reference.insert("a".to_string(), 0);
        reference.insert("b".to_string(), 1);
        reference.insert("c".to_string(), 2);
        reference.insert("d".to_string(), 3);
        reference.insert("e".to_string(), 4);
        reference.insert("f".to_string(), 5);
        KeyBindings { store, reference }
    }

    #[test]
    fn test_from_string_with_empty_string() {
        let data = "".to_string();
        let bindings = KeyBindings::from_string(data);
        assert_eq!(bindings.store.len(), 0);
        assert_eq!(bindings.reference.len(), 0);
    }

    #[test]
    fn test_from_string() {
        let data = generate_string();
        let bindings = KeyBindings::from_string(data);
        assert_eq!(bindings.store[0], "a");
        assert_eq!(bindings.store[1], "b");
        assert_eq!(bindings.store[2], "c");
        assert_eq!(bindings.store[3], "d");
        assert_eq!(bindings.store[4], "e");
        assert_eq!(bindings.store[5], "f");

        assert_eq!(bindings.reference["a"], 0);
        assert_eq!(bindings.reference["b"], 1);
        assert_eq!(bindings.reference["c"], 2);
        assert_eq!(bindings.reference["d"], 3);
        assert_eq!(bindings.reference["e"], 4);
        assert_eq!(bindings.reference["f"], 5);
    }

    #[test]
    fn test_to_string() {
        let bindings = generate_struct();
        let data = bindings.to_string();
        assert_eq!(data, generate_string());
    }

    #[test]
    fn test_from_bytes() {
        let data = generate_bytes();
        let bindings = KeyBindings::from_bytes(&data).unwrap();
        assert_eq!(bindings.store[0], "a");
        assert_eq!(bindings.store[1], "b");
        assert_eq!(bindings.store[2], "c");
        assert_eq!(bindings.store[3], "d");
        assert_eq!(bindings.store[4], "e");
        assert_eq!(bindings.store[5], "f");

        assert_eq!(bindings.reference["a"], 0);
        assert_eq!(bindings.reference["b"], 1);
        assert_eq!(bindings.reference["c"], 2);
        assert_eq!(bindings.reference["d"], 3);
        assert_eq!(bindings.reference["e"], 4);
        assert_eq!(bindings.reference["f"], 5);
    }

    #[test]
    fn test_to_bytes() {
        let bindings = generate_struct();
        let data = bindings.to_bytes();
        assert_eq!(data, generate_bytes());
    }

    #[test]
    fn test_add_column() {
        let mut bindings = generate_struct();
        bindings.add_column("g".to_string());
        assert_eq!(bindings.store[6], "g");
        assert_eq!(bindings.reference["g"], 6);

        let mut bindings = KeyBindings::fresh();
        bindings.add_column("a".to_string());
        bindings.add_column("b".to_string());

        assert_eq!(bindings.store[0], "a");
        assert_eq!(bindings.reference["a"], 0);
        assert_eq!(bindings.store[1], "b");
        assert_eq!(bindings.reference["b"], 1);
    }
}
