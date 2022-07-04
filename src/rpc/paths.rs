use once_cell::sync::Lazy;
use surrealdb::sql::Part;

pub static ID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);

pub static METHOD: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("method")]);

pub static PARAMS: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("params")]);
