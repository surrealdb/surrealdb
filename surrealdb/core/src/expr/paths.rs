use std::sync::LazyLock;

use crate::expr::Part;

pub const OBJ_PATH_ACCESS: &str = "ac";
pub const OBJ_PATH_AUTH: &str = "rd";
pub const OBJ_PATH_TOKEN: &str = "tk";

pub static ID: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("id".to_owned())]);

pub static IP: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("ip".to_owned())]);

pub static NS: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("ns".to_owned())]);

pub static DB: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("db".to_owned())]);

pub static AC: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(OBJ_PATH_ACCESS.to_owned())]);

pub static RD: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(OBJ_PATH_AUTH.to_owned())]);

pub static OR: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("or".to_owned())]);

pub static TK: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field(OBJ_PATH_TOKEN.to_owned())]);

pub static IN: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("in".to_owned())]);

pub static OUT: LazyLock<[Part; 1]> = LazyLock::new(|| [Part::Field("out".to_owned())]);
