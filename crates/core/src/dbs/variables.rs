use std::collections::BTreeMap;
use std::convert::Infallible;

use serde::{Deserialize, Serialize};

use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::expr::Param;
use crate::expr::visit::{Visit, Visitor};
use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicVariables;
use crate::val::{Object, Value};

/// A visitor pass which will capture the value of parameters in the visited expression from the
/// context.
pub(crate) struct ParameterCapturePass<'a, 'b> {
	pub context: &'a Context,
	pub captures: &'b mut Variables,
}

impl ParameterCapturePass<'_, '_> {
	pub fn capture<V: for<'a, 'b> Visit<ParameterCapturePass<'a, 'b>>>(
		context: &Context,
		v: &V,
	) -> Variables {
		let mut captures = Variables::new();

		let _ = v.visit(&mut ParameterCapturePass {
			context,
			captures: &mut captures,
		});

		captures
	}
}

impl Visitor for ParameterCapturePass<'_, '_> {
	type Error = Infallible;

	fn visit_param(&mut self, param: &Param) -> Result<(), Self::Error> {
		if !PROTECTED_PARAM_NAMES.contains(&param.as_str())
			&& let Some(v) = self.context.value(param.as_str())
		{
			self.captures.0.entry(param.clone().into_string()).or_insert_with(|| v.clone());
		}
		Ok(())
	}
}

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
