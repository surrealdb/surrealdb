use crate::sql::part::Part;
use std::sync::LazyLock;

pub static IN: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("in")]);

pub static OUT: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("out")]);
