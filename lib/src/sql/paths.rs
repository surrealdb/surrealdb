use crate::sql::part::Part;
use once_cell::sync::Lazy;

pub static ID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);

pub static IN: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("in")]);

pub static OUT: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("out")]);

pub static META: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("__")]);
