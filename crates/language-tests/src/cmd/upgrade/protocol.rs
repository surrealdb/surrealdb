use revision::revisioned;
/// Manual implementations of the oldest revision of surrealdb structs for backwards compatibility.
/// Only the variants that are needed to controll surrealdb are implemented.
use std::collections::BTreeMap;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Hash)]
pub struct ProxyObject(pub BTreeMap<String, ProxyValue>);

impl std::ops::DerefMut for ProxyObject {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl std::ops::Deref for ProxyObject {
	type Target = BTreeMap<String, ProxyValue>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Hash)]
pub struct ProxyStrand(pub String);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Hash)]
pub struct ProxyArray(pub Vec<ProxyValue>);

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Hash)]
pub enum ProxyNumber {
	Int(i64),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Hash)]
pub enum ProxyValue {
	#[revision(override(revision = 1, discriminant = 3))]
	Number(ProxyNumber),
	#[revision(override(revision = 1, discriminant = 4))]
	Strand(ProxyStrand),
	#[revision(override(revision = 1, discriminant = 8))]
	Array(ProxyArray),
	#[revision(override(revision = 1, discriminant = 9))]
	Object(ProxyObject),
}

impl From<&str> for ProxyValue {
	fn from(value: &str) -> Self {
		ProxyValue::Strand(ProxyStrand(value.to_owned()))
	}
}

impl From<Vec<ProxyValue>> for ProxyValue {
	fn from(value: Vec<ProxyValue>) -> Self {
		ProxyValue::Array(ProxyArray(value))
	}
}

impl From<ProxyObject> for ProxyValue {
	fn from(value: ProxyObject) -> Self {
		ProxyValue::Object(value)
	}
}

impl From<i64> for ProxyValue {
	fn from(value: i64) -> Self {
		ProxyValue::Number(ProxyNumber::Int(value))
	}
}
