use crate::sql::part::Part;
use std::sync::LazyLock;

pub const OBJ_PATH_ACCESS: &str = "ac";
pub const OBJ_PATH_AUTH: &str = "rd";
pub const OBJ_PATH_TOKEN: &str = "tk";

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("id")]);

pub static IP: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("ip")]);

pub static NS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("ns")]);

pub static DB: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("db")]);

pub static AC: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from(OBJ_PATH_ACCESS)]);

pub static RD: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from(OBJ_PATH_AUTH)]);

pub static OR: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("or")]);

pub static TK: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from(OBJ_PATH_TOKEN)]);

pub static IN: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("in")]);

pub static OUT: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("out")]);

pub static META: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("__")]);

pub static EDGE: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::from("__")]);
