//! Defines operations for handling memory of a python program that is accessing the rust library.
// Standard library imports
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

// External crate imports
use surrealml_core::storage::surml_file::SurMlFile;

// External library imports
use uuid::Uuid;

/// A hashmap of unique identifiers to loaded machine learning models. As long as the python program keeps the unique
/// identifier it can access the loaded machine learning model. It is best to keep as little as possible on the python
/// side and keep as much as possible on the rust side. Therefore bindings to other languages can be created with ease
/// and a command line tool can also be created without much need for new features. This will also ensure consistency
/// between other languages and the command line tool.
pub static STATE: LazyLock<Arc<Mutex<HashMap<String, SurMlFile>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Generates a unique identifier that can be used to access a loaded machine learning model.
///
/// # Returns
/// A unique identifier that can be used to access a loaded machine learning model.
pub fn generate_unique_id() -> String {
    let uuid = Uuid::new_v4();
    uuid.to_string()
}
