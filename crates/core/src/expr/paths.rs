use std::sync::LazyLock;

use crate::expr::Part;

pub const OBJ_PATH_ACCESS: &str = "ac";
pub const OBJ_PATH_AUTH: &str = "rd";
pub const OBJ_PATH_TOKEN: &str = "tk";

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("id".to_owned()).unwrap()]);

pub static IP: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("ip".to_owned()).unwrap()]);

pub static NS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("ns".to_owned()).unwrap()]);

pub static DB: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("db".to_owned()).unwrap()]);

pub static AC: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::field(OBJ_PATH_ACCESS.to_owned()).unwrap()]);

pub static RD: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::field(OBJ_PATH_AUTH.to_owned()).unwrap()]);

pub static OR: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("or".to_owned()).unwrap()]);

pub static TK: LazyLock<[Part; 1]> =
	LazyLock::new(|| [Part::field(OBJ_PATH_TOKEN.to_owned()).unwrap()]);

pub static IN: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("in".to_owned()).unwrap()]);

pub static OUT: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::field("out".to_owned()).unwrap()]);
