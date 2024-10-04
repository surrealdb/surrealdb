use crate::sql::{id::Gen, id::Id as NewId, Array, IdRange, Object};
use derive::Key;
use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Key, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum Id {
	Number(i64),
	String(String),
	Array(Array),
	Object(Object),
	Generate(Gen),
	Range(Box<IdRange>),
}

impl Id {
	pub fn fix(&self) -> Option<NewId> {
		match self {
			Self::Number(_) => None,
			Self::String(_) => None,
			Self::Array(x) => Some(NewId::Array(x.to_owned())),
			Self::Object(x) => Some(NewId::Object(x.to_owned())),
			Self::Generate(x) => Some(NewId::Generate(x.to_owned())),
			Self::Range(x) => Some(NewId::Range(x.to_owned())),
		}
	}

	pub fn is_affected(&self) -> bool {
		match self {
			Self::Number(_) => false,
			Self::String(_) => false,
			Self::Array(_) => true,
			Self::Object(_) => true,
			Self::Generate(_) => true,
			Self::Range(_) => true,
		}
	}
}

impl From<Id> for NewId {
	fn from(id: Id) -> Self {
		match id {
			Id::Number(x) => NewId::Number(x),
			Id::String(x) => NewId::String(x),
			Id::Array(x) => NewId::Array(x),
			Id::Object(x) => NewId::Object(x),
			Id::Generate(x) => NewId::Generate(x),
			Id::Range(x) => NewId::Range(x),
		}
	}
}

pub mod key {
	use derive::Key;
	use serde::{Deserialize, Serialize};

	use crate::sql::{id::Id as NewId, Dir};

	use super::Id;

	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
	#[non_exhaustive]
	pub struct Graph<'a> {
		__: u8,
		_a: u8,
		pub ns: &'a str,
		_b: u8,
		pub db: &'a str,
		_c: u8,
		pub tb: &'a str,
		_d: u8,
		pub id: Id,
		pub eg: Dir,
		pub ft: &'a str,
		pub fk: Id,
	}

	impl<'a> Graph<'a> {
		pub fn new(
			ns: &'a str,
			db: &'a str,
			tb: &'a str,
			id: Id,
			eg: Dir,
			ft: &'a str,
			fk: &'a Id,
		) -> Self {
			Self {
				__: b'/',
				_a: b'*',
				ns,
				_b: b'*',
				db,
				_c: b'*',
				tb,
				_d: b'~',
				id,
				eg,
				ft,
				fk: fk.to_owned(),
			}
		}

		pub fn fix(&self) -> Option<crate::key::graph::Graph> {
			let fixed = match (self.id.fix(), self.fk.fix()) {
				(None, None) => return None,
				(Some(id), None) => crate::key::graph::Graph::new_from_id(
					self.ns,
					self.db,
					self.tb,
					id,
					self.eg.to_owned(),
					self.ft,
					NewId::from(self.fk.to_owned()),
				),
				(None, Some(fk)) => crate::key::graph::Graph::new_from_id(
					self.ns,
					self.db,
					self.tb,
					self.id.to_owned().into(),
					self.eg.to_owned(),
					self.ft,
					fk,
				),
				(Some(id), Some(fk)) => crate::key::graph::Graph::new_from_id(
					self.ns,
					self.db,
					self.tb,
					id,
					self.eg.to_owned(),
					self.ft,
					fk,
				),
			};

			Some(fixed)
		}
	}

	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
	#[non_exhaustive]
	pub struct Thing<'a> {
		__: u8,
		_a: u8,
		pub ns: &'a str,
		_b: u8,
		pub db: &'a str,
		_c: u8,
		pub tb: &'a str,
		_d: u8,
		pub id: Id,
	}

	impl<'a> Thing<'a> {
		pub fn new(ns: &'a str, db: &'a str, tb: &'a str, id: Id) -> Self {
			Self {
				__: b'/',
				_a: b'*',
				ns,
				_b: b'*',
				db,
				_c: b'*',
				tb,
				_d: b'*',
				id,
			}
		}

		pub fn fix(&self) -> Option<crate::key::thing::Thing> {
			self.id.fix().map(|id| crate::key::thing::new(self.ns, self.db, self.tb, &id))
		}
	}
}
