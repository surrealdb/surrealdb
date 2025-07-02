use crate::expr::{Ident, Part};
use std::sync::LazyLock;

pub const OBJ_PATH_ACCESS: &str = "ac";
pub const OBJ_PATH_AUTH: &str = "rd";
pub const OBJ_PATH_TOKEN: &str = "tk";

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("id".to_owned()))]);

pub static IP: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("ip".to_owned()))]);

pub static NS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("ns".to_owned()))]);

pub static DB: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("db".to_owned()))]);

pub static AC: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::Field(Ident(OBJ_PATH_ACCESS.to_owned()))]);

pub static RD: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::Field(Ident(OBJ_PATH_AUTH.to_owned()))]);

pub static OR: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("or".to_owned()))]);

pub static TK: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::Field(Ident(OBJ_PATH_TOKEN.to_owned()))]);

pub static IN: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("in".to_owned()))]);

pub static OUT: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("out".to_owned()))]);

pub static META: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("__".to_owned()))]);

pub static EDGE: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(Ident("__".to_owned()))]);
