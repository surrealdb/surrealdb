use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(tag = "op")]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Operation {
	Add {
		path: Idiom,
		value: Value,
	},
	Remove {
		path: Idiom,
	},
	Replace {
		path: Idiom,
		value: Value,
	},
	Change {
		path: Idiom,
		value: Value,
	},
	Copy {
		path: Idiom,
		from: Idiom,
	},
	Move {
		path: Idiom,
		from: Idiom,
	},
	Test {
		path: Idiom,
		value: Value,
	},
}
