use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::expr::Expr;
use crate::expr::expression::VisitExpression;
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicVariables;
use crate::val::{Object, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[repr(transparent)]
pub(crate) struct Variables(pub(crate) BTreeMap<String, Value>);

impl Variables {
	/// Create a new empty variables map.
	#[allow(dead_code)]
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Insert a new variable into the map.
	#[allow(dead_code)]
	pub fn insert(&mut self, key: String, value: Value) {
		self.0.insert(key, value);
	}

	/// Extend the variables map with the contents of another variables map.
	pub fn extend(&mut self, other: Variables) {
		self.0.extend(other.0);
	}

	/// Create a new variables map from an expression and a context.
	pub(crate) fn from_expr<T: VisitExpression>(expr: &T, ctx: &Context) -> Self {
		let mut vars = BTreeMap::new();
		let mut visitor = |x: &Expr| {
			if let Expr::Param(param) = x {
				if !PROTECTED_PARAM_NAMES.contains(&param.as_str()) {
					if let Some(v) = ctx.value(param.as_str()) {
						vars.insert(param.clone().into_string(), v.clone());
					}
				}
			}
		};

		expr.visit(&mut visitor);
		Self(vars)
	}
}

impl IntoIterator for Variables {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;

	#[inline]
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<(String, Value)> for Variables {
	fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

impl From<Object> for Variables {
	fn from(obj: Object) -> Self {
		Self(obj.0)
	}
}

impl From<BTreeMap<String, Value>> for Variables {
	fn from(map: BTreeMap<String, Value>) -> Self {
		Self(map)
	}
}

impl From<PublicVariables> for Variables {
	fn from(vars: PublicVariables) -> Self {
		let mut map = BTreeMap::new();
		for (key, val) in vars {
			let internal_val = convert_public_value_to_internal(val);
			map.insert(key, internal_val);
		}
		Self(map)
	}
}
