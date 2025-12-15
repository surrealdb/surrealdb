use std::collections::{BTreeMap, HashMap};

use surrealdb_types::{Object, SurrealValue, Variables};

pub trait Bindable {
	fn bind(self, vars: &mut Variables);
}

impl<K: Into<String>, V: SurrealValue> Bindable for BTreeMap<K, V> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl<K: Into<String>, V: SurrealValue> Bindable for HashMap<K, V> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl<K: Into<String>, V: SurrealValue> Bindable for Vec<(K, V)> {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self.into_iter().map(|(k, v)| (k.into(), v.into_value())).collect());
	}
}

impl Bindable for Variables {
	fn bind(self, vars: &mut Variables) {
		vars.extend(self);
	}
}

impl Bindable for Object {
	fn bind(self, vars: &mut Variables) {
		vars.extend(Variables::from(self));
	}
}

impl<K: Into<String>, V: SurrealValue> Bindable for (K, V) {
	fn bind(self, vars: &mut Variables) {
		vars.insert(self.0, self.1);
	}
}