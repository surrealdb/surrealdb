use crate::sql::part::Part;
use once_cell::sync::Lazy;

pub const OBJ_PATH_ACCESS: &str = "ac";
pub const OBJ_PATH_AUTH: &str = "rd";
pub const OBJ_PATH_TOKEN: &str = "tk";

pub static ID: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("id")]);

pub static IP: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("ip")]);

pub static NS: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("ns")]);

pub static DB: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("db")]);

pub static AC: Lazy<[Part; 1]> = Lazy::new(|| [Part::from(OBJ_PATH_ACCESS)]);

pub static RD: Lazy<[Part; 1]> = Lazy::new(|| [Part::from(OBJ_PATH_AUTH)]);

pub static OR: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("or")]);

pub static TK: Lazy<[Part; 1]> = Lazy::new(|| [Part::from(OBJ_PATH_TOKEN)]);

pub static IN: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("in")]);

pub static OUT: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("out")]);

pub static META: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("__")]);

pub static EDGE: Lazy<[Part; 1]> = Lazy::new(|| [Part::from("__")]);
