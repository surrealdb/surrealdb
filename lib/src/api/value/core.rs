use crate::{
	opt::{Edge, KeyRange, QueryRange},
	Bytes, Datetime, Number, Object, RecordId, RecordIdKey, Uuid, Value,
};
use std::{collections::BTreeMap, ops::Bound};
use surrealdb_core::{
	dbs::{Action as CoreAction, Notification as CoreNotification},
	sql::{
		Array as CoreArray, Bytes as CoreBytes, Datetime as CoreDatetime, Duration as CoreDuration,
		Edges as CoreEdges, Id, Number as CoreNumber, Object as CoreObject, Range as CoreRange,
		Strand, Table as CoreTable, Tables as CoreTables, Thing, Uuid as CoreUuid,
		Value as CoreValue,
	},
};
use trice::Duration;

use super::{Action, Notification};

pub(crate) trait ToCore: Sized {
	type Core;

	fn to_core(self) -> Self::Core;

	fn as_core(&self) -> Self::Core;

	/// Create a lib value from it's core counterpart.
	/// Can return none if the core value cannot be turned into the core value.
	/// This can be the case with forexample value, where it's AST like values are not present on
	/// the lib value.
	fn from_core(this: Self::Core) -> Option<Self>;
}

impl ToCore for Bytes {
	type Core = CoreBytes;

	fn to_core(self) -> Self::Core {
		CoreBytes::from(self.0)
	}

	fn as_core(&self) -> Self::Core {
		self.clone().to_core()
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(Self(this.into_inner()))
	}
}

impl ToCore for Datetime {
	type Core = CoreDatetime;

	fn to_core(self) -> Self::Core {
		CoreDatetime::from(self.0)
	}

	fn as_core(&self) -> Self::Core {
		self.clone().to_core()
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(Self(this.0))
	}
}

impl ToCore for Object {
	type Core = CoreObject;

	fn to_core(self) -> Self::Core {
		CoreObject::from(
			self.0
				.into_iter()
				.map(|(k, v)| (k, v.to_core()))
				.collect::<BTreeMap<String, CoreValue>>(),
		)
	}

	fn as_core(&self) -> Self::Core {
		let map: BTreeMap<String, CoreValue> =
			self.0.iter().map(|(k, v)| (k.clone(), v.as_core())).collect();

		CoreObject::from(map)
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let mut new = BTreeMap::new();
		for (k, v) in this.0.into_iter() {
			new.insert(k, ToCore::from_core(v)?);
		}
		Some(Object(new))
	}
}

impl ToCore for Vec<Value> {
	type Core = CoreArray;

	fn to_core(self) -> Self::Core {
		CoreArray::from(self.into_iter().map(ToCore::to_core).collect::<Vec<CoreValue>>())
	}

	fn as_core(&self) -> Self::Core {
		CoreArray::from(self.iter().map(ToCore::as_core).collect::<Vec<CoreValue>>())
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let len = this.0.len();
		this.0.into_iter().try_fold(Vec::<Value>::with_capacity(len), |mut acc, r| {
			acc.push(Value::from_core(r)?);
			Some(acc)
		})
	}
}

impl ToCore for Number {
	type Core = CoreNumber;

	fn to_core(self) -> Self::Core {
		match self {
			Number::Float(x) => CoreNumber::Float(x),
			Number::Integer(x) => CoreNumber::Int(x),
			Number::Decimal(x) => CoreNumber::Decimal(x),
		}
	}

	fn as_core(&self) -> Self::Core {
		self.clone().to_core()
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let v = match this {
			CoreNumber::Int(x) => Number::Integer(x),
			CoreNumber::Float(x) => Number::Float(x),
			CoreNumber::Decimal(x) => Number::Decimal(x),
			_ => return None,
		};
		Some(v)
	}
}

impl ToCore for Uuid {
	type Core = CoreUuid;

	fn to_core(self) -> Self::Core {
		CoreUuid::from(self)
	}

	fn as_core(&self) -> Self::Core {
		self.clone().to_core()
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(this.0)
	}
}

impl ToCore for String {
	type Core = Strand;

	fn to_core(self) -> Self::Core {
		Strand::from(self)
	}

	fn as_core(&self) -> Self::Core {
		Strand::from(self.as_str())
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(this.0)
	}
}

impl ToCore for Duration {
	type Core = CoreDuration;

	fn to_core(self) -> Self::Core {
		CoreDuration::from(self)
	}

	fn as_core(&self) -> Self::Core {
		CoreDuration::from(*self)
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(this.0)
	}
}

impl ToCore for RecordId {
	type Core = Thing;

	fn to_core(self) -> Self::Core {
		Thing::from((self.table, self.key.to_core()))
	}

	fn as_core(&self) -> Self::Core {
		Thing::from((self.table.clone(), self.key.as_core()))
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(Self {
			table: this.tb,
			key: ToCore::from_core(this.id)?,
		})
	}
}

impl ToCore for RecordIdKey {
	type Core = Id;

	fn to_core(self) -> Self::Core {
		match self {
			RecordIdKey::String(x) => Id::String(x),
			RecordIdKey::Integer(x) => Id::Number(x),
			RecordIdKey::Object(x) => Id::Object(x.to_core()),
			RecordIdKey::Array(x) => Id::Array(x.to_core()),
		}
	}

	fn as_core(&self) -> Self::Core {
		match self {
			RecordIdKey::String(x) => Id::String(x.clone()),
			RecordIdKey::Integer(x) => Id::Number(x.clone()),
			RecordIdKey::Object(x) => Id::Object(x.as_core()),
			RecordIdKey::Array(x) => Id::Array(x.as_core()),
		}
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let v = match this {
			Id::String(x) => RecordIdKey::String(x),
			Id::Number(x) => RecordIdKey::Integer(x),
			Id::Object(x) => RecordIdKey::Object(ToCore::from_core(x)?),
			Id::Array(x) => RecordIdKey::Array(ToCore::from_core(x)?),
			_ => return None,
		};
		Some(v)
	}
}

impl ToCore for Edge {
	type Core = CoreEdges;

	fn to_core(self) -> Self::Core {
		let from = self.from.to_core();
		let mut what = CoreTables::default();
		what.0 = self
			.tables
			.into_iter()
			.map(|x| {
				let mut t = CoreTable::default();
				t.0 = x;
				t
			})
			.collect();
		CoreEdges::new(self.dir, from, what)
	}

	fn as_core(&self) -> Self::Core {
		let from = self.from.as_core();
		let mut what = CoreTables::default();
		what.0 = self
			.tables
			.iter()
			.map(|x| {
				let mut t = CoreTable::default();
				t.0 = x.clone();
				t
			})
			.collect();
		CoreEdges::new(self.dir.clone(), from, what)
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(Edge {
			dir: this.dir,
			from: RecordId::from_core(this.from)?,
			tables: this.what.0.into_iter().map(|x| x.0).collect(),
		})
	}
}

impl ToCore for Notification<Value> {
	type Core = CoreNotification;

	fn to_core(self) -> Self::Core {
		CoreNotification::new(self.query_id.into(), self.action.to_core(), self.data.to_core())
	}

	fn as_core(&self) -> Self::Core {
		CoreNotification::new(self.query_id.into(), self.action.as_core(), self.data.as_core())
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		Some(Notification {
			query_id: this.id.0,
			action: Action::from_core(this.action)?,
			data: Value::from_core(this.result)?,
		})
	}
}

impl ToCore for QueryRange {
	type Core = CoreRange;

	fn to_core(self) -> Self::Core {
		CoreRange::new(
			self.table,
			self.range.start.map(|x| x.to_core()),
			self.range.end.map(|x| x.to_core()),
		)
	}

	fn as_core(&self) -> Self::Core {
		self.clone().to_core()
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let start = match this.beg {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_core(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_core(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match this.end {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_core(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_core(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Some(QueryRange {
			table: this.tb,
			range: KeyRange {
				start,
				end,
			},
		})
	}
}

impl ToCore for Action {
	type Core = CoreAction;

	fn to_core(self) -> Self::Core {
		match self {
			Action::Create => CoreAction::Create,
			Action::Update => CoreAction::Update,
			Action::Delete => CoreAction::Delete,
		}
	}

	fn as_core(&self) -> Self::Core {
		match self {
			Action::Create => CoreAction::Create,
			Action::Update => CoreAction::Update,
			Action::Delete => CoreAction::Delete,
		}
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		match this {
			CoreAction::Create => Some(Action::Create),
			CoreAction::Update => Some(Action::Update),
			CoreAction::Delete => Some(Action::Delete),
			_ => None,
		}
	}
}

impl ToCore for Value {
	type Core = CoreValue;

	fn to_core(self) -> Self::Core {
		match self {
			Value::None => CoreValue::None,
			Value::Bool(x) => CoreValue::Bool(x),
			Value::Number(x) => CoreValue::Number(x.to_core()),
			Value::Object(x) => CoreValue::Object(x.to_core()),
			Value::String(x) => CoreValue::Strand(Strand::from(x)),
			Value::Array(x) => CoreValue::Array(x.to_core()),
			Value::Uuid(x) => CoreValue::Uuid(x.to_core()),
			Value::Datetime(x) => CoreValue::Datetime(x.to_core()),
			Value::Duration(x) => CoreValue::Duration(x.to_core()),
			Value::Bytes(x) => CoreValue::Bytes(x.to_core()),
			Value::RecordId(x) => CoreValue::Thing(x.to_core()),
		}
	}

	fn as_core(&self) -> Self::Core {
		match self {
			Value::None => CoreValue::None,
			Value::Bool(x) => CoreValue::Bool(*x),
			Value::Number(x) => CoreValue::Number(x.as_core()),
			Value::Object(x) => CoreValue::Object(x.as_core()),
			Value::String(x) => CoreValue::Strand(Strand::from(x.as_str())),
			Value::Array(x) => CoreValue::Array(x.as_core()),
			Value::Uuid(x) => CoreValue::Uuid(x.as_core()),
			Value::Datetime(x) => CoreValue::Datetime(x.as_core()),
			Value::Duration(x) => CoreValue::Duration(x.as_core()),
			Value::Bytes(x) => CoreValue::Bytes(x.as_core()),
			Value::RecordId(x) => CoreValue::Thing(x.as_core()),
		}
	}

	fn from_core(this: Self::Core) -> Option<Self> {
		let v = match this {
			CoreValue::None | CoreValue::Null => Value::None,
			CoreValue::Bool(x) => Value::Bool(x),
			CoreValue::Number(x) => Value::Number(ToCore::from_core(x)?),
			CoreValue::Object(x) => Value::Object(ToCore::from_core(x)?),
			CoreValue::Strand(x) => Value::String(ToCore::from_core(x)?),
			CoreValue::Array(x) => Value::Array(ToCore::from_core(x)?),
			CoreValue::Uuid(x) => Value::Uuid(ToCore::from_core(x)?),
			CoreValue::Datetime(x) => Value::Datetime(ToCore::from_core(x)?),
			CoreValue::Duration(x) => Value::Duration(ToCore::from_core(x)?),
			CoreValue::Bytes(x) => Value::Bytes(ToCore::from_core(x)?),
			CoreValue::Thing(x) => Value::RecordId(ToCore::from_core(x)?),
			_ => return None,
		};

		Some(v)
	}
}
