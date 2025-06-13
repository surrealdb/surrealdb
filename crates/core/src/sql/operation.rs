use crate::sql::{Expr, Idiom};
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
		value: Expr,
	},
	Remove {
		path: Idiom,
	},
	Replace {
		path: Idiom,
		value: Expr,
	},
	Change {
		path: Idiom,
		value: Expr,
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
		value: Expr,
	},
}
