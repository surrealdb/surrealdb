use crate::sql::idiom::Idiom;
use crate::sql::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(tag = "op")]
#[serde(rename_all = "lowercase")]
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
	Test {
		path: Idiom,
		value: Value,
	},
}
