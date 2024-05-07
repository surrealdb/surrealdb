use crate::sql::part::Part;
use once_cell::sync::Lazy;

pub static ID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);

pub static IP: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("ip")]);

pub static NS: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("ns")]);

pub static DB: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("db")]);

pub static AC: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("ac")]);

pub static RD: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("rd")]);

pub static OR: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("or")]);

pub static TK: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("tk")]);

pub static IN: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("in")]);

pub static OUT: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("out")]);

pub static META: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("__")]);

pub static EDGE: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("__")]);
