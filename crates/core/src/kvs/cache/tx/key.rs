use uuid::Uuid;

use super::lookup::Lookup;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::val::RecordIdKey;

#[derive(Clone, Hash, Eq, PartialEq)]
pub(crate) enum Key {
	/// A cache key for nodes
	Nds,
	/// A cache key for root users
	Rus,
	/// A cache key for root accesses
	Ras,
	/// A cache key for root access grants
	Rgs(String),
	/// A cache key for namespaces
	Nss,
	/// A cache key for namespace users
	Nus(NamespaceId),
	/// A cache key for namespace accesses
	Nas(NamespaceId),
	/// A cache key for namespace access grants
	Ngs(NamespaceId, String),
	/// A cache key for databases
	Dbs(NamespaceId),
	/// A cache key for database users
	Dus(NamespaceId, DatabaseId),
	/// A cache key for database accesses
	Das(NamespaceId, DatabaseId),
	/// A cache key for database access grants
	Dgs(NamespaceId, DatabaseId, String),
	/// A cache key for apis (on a database)
	Aps(NamespaceId, DatabaseId),
	/// A cache key for analyzers (on a database)
	Azs(NamespaceId, DatabaseId),
	/// A cache key for buckets (on a database)
	Bus(NamespaceId, DatabaseId),
	/// A cache key for functions (on a database)
	Fcs(NamespaceId, DatabaseId),
	/// A cache key for models (on a database)
	Mls(NamespaceId, DatabaseId),
	/// A cache key for configs (on a database)
	Cgs(NamespaceId, DatabaseId),
	/// A cache key for parameters (on a database)
	Pas(NamespaceId, DatabaseId),
	/// A cache key for tables
	Tbs(NamespaceId, DatabaseId),
	/// A cache key for sequences (on a database)
	Sqs(NamespaceId, DatabaseId),
	/// A cache key for events (on a table)
	Evs(NamespaceId, DatabaseId, String),
	/// A cache key for fieds (on a table)
	Fds(NamespaceId, DatabaseId, String),
	/// A cache key for views (on a table)
	Fts(NamespaceId, DatabaseId, String),
	/// A cache key for indexes (on a table)
	Ixs(NamespaceId, DatabaseId, String),
	/// A cache key for live queries (on a table)
	Lvs(NamespaceId, DatabaseId, String),
	/// A cache key for a node
	Nd(Uuid),
	/// A cache key for a root user
	Ru(String),
	/// A cache key for a root access
	Ra(String),
	/// A cache key for a root access grant
	Rg(String, String),
	/// A cache key for a namespace
	NsByName(String),
	/// A cache key for a namespace user
	Nu(NamespaceId, String),
	/// A cache key for a namespace access
	Na(NamespaceId, String),
	/// A cache key for a namespace access grant
	Ng(NamespaceId, String, String),
	/// A cache key for a database
	DbByName(String, String),
	/// A cache key for a database user
	Du(NamespaceId, DatabaseId, String),
	/// A cache key for a database access
	Da(NamespaceId, DatabaseId, String),
	/// A cache key for a database access grant
	Dg(NamespaceId, DatabaseId, String, String),
	/// A cache key for an api (on a database)
	Ap(NamespaceId, DatabaseId, String),
	/// A cache key for an analyzer (on a database)
	Az(NamespaceId, DatabaseId, String),
	/// A cache key for a bucket (on a database)
	Bu(NamespaceId, DatabaseId, String),
	/// A cache key for a function (on a database)
	Fc(NamespaceId, DatabaseId, String),
	/// A cache key for a model (on a database)
	Ml(NamespaceId, DatabaseId, String, String),
	/// A cache key for a config (on a database)
	Cg(NamespaceId, DatabaseId, String),
	/// A cache key for a parameter (on a database)
	Pa(NamespaceId, DatabaseId, String),
	/// A cache key for a sequence (on a database)
	Sq(NamespaceId, DatabaseId, String),
	/// A cache key for a table
	TbByName(String, String, String),
	/// A cache key for a table by id.
	Tb(NamespaceId, DatabaseId, String),
	/// A cache key for an event (on a table)
	Ev(NamespaceId, DatabaseId, String, String),
	/// A cache key for a fied (on a table)
	Fd(NamespaceId, DatabaseId, String, String),
	/// A cache key for an index (on a table)
	Ix(NamespaceId, DatabaseId, String, String),
	/// A cache key for a record
	Record(NamespaceId, DatabaseId, String, RecordIdKey),
}

impl<'a> From<Lookup<'a>> for Key {
	#[rustfmt::skip]
	fn from(value: Lookup<'a>) -> Self {
		match value {
			//
			Lookup::Nds => Key::Nds,
			Lookup::Rus => Key::Rus,
			Lookup::Ras => Key::Ras,
			Lookup::Rgs(a) => Key::Rgs(a.to_string()),
			Lookup::Nss => Key::Nss,
			Lookup::Nus(a) => Key::Nus(a),
			Lookup::Nas(a) => Key::Nas(a),
			Lookup::Ngs(a, b) => Key::Ngs(a, b.to_string()),
			Lookup::Dbs(a) => Key::Dbs(a),
			Lookup::Dus(a, b) => Key::Dus(a, b),
			Lookup::Das(a, b) => Key::Das(a, b),
			Lookup::Dgs(a, b, c) => Key::Dgs(a, b, c.to_string()),
			Lookup::Aps(a, b) => Key::Aps(a, b),
			Lookup::Azs(a, b) => Key::Azs(a, b),
			Lookup::Bus(a, b) => Key::Bus(a, b),
			Lookup::Fcs(a, b) => Key::Fcs(a, b),
			Lookup::Mls(a, b) => Key::Mls(a, b),
			Lookup::Cgs(a, b) => Key::Cgs(a, b),
			Lookup::Pas(a, b) => Key::Pas(a, b),
			Lookup::Sqs(a, b) => Key::Sqs(a, b),
			Lookup::Tbs(a, b) => Key::Tbs(a, b),
			Lookup::Evs(a, b, c) => Key::Evs(a, b, c.to_string()),
			Lookup::Fds(a, b, c) => Key::Fds(a, b, c.to_string()),
			Lookup::Fts(a, b, c) => Key::Fts(a, b, c.to_string()),
			Lookup::Ixs(a, b, c) => Key::Ixs(a, b, c.to_string()),
			Lookup::Lvs(a, b, c) => Key::Lvs(a, b, c.to_string()),
			//
			Lookup::Nd(a) => Key::Nd(a),
			Lookup::Ru(a) => Key::Ru(a.to_string()),
			Lookup::Ra(a) => Key::Ra(a.to_string()),
			Lookup::Rg(a, b) => Key::Rg(a.to_string(), b.to_string()),
			Lookup::NsByName(a) => Key::NsByName(a.to_string()),
			Lookup::Nu(a, b) => Key::Nu(a, b.to_string()),
			Lookup::Na(a, b) => Key::Na(a, b.to_string()),
			Lookup::Ng(a, b, c) => Key::Ng(a, b.to_string(), c.to_string()),
			Lookup::DbByName(a, b) => Key::DbByName(a.to_string(), b.to_string()),
			Lookup::Du(a, b, c) => Key::Du(a, b, c.to_string()),
			Lookup::Da(a, b, c) => Key::Da(a, b, c.to_string()),
			Lookup::Dg(a, b, c, d) => Key::Dg(a, b, c.to_string(), d.to_string()),
			Lookup::Ap(a, b, c) => Key::Ap(a, b, c.to_string()),
			Lookup::Az(a, b, c) => Key::Az(a, b, c.to_string()),
			Lookup::Bu(a, b, c) => Key::Bu(a, b, c.to_string()),
			Lookup::Fc(a, b, c) => Key::Fc(a, b, c.to_string()),
			Lookup::Ml(a, b, c, d) => Key::Ml(a, b, c.to_string(), d.to_string()),
			Lookup::Cg(a, b, c) => Key::Cg(a, b, c.to_string()),
			Lookup::Pa(a, b, c) => Key::Pa(a, b, c.to_string()),
			Lookup::Sq(a, b,c) => Key::Sq(a, b, c.to_string()),
			Lookup::Tb(a, b, c) => Key::Tb(a, b, c.to_string()),
			Lookup::TbByName(a, b, c) => Key::TbByName(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Ev(a, b, c, d) => Key::Ev(a, b, c.to_string(), d.to_string()),
			Lookup::Fd(a, b, c, d) => Key::Fd(a, b, c.to_string(), d.to_string()),
			Lookup::Ix(a, b, c, d) => Key::Ix(a, b, c.to_string(), d.to_string()),
			Lookup::Record(a, b, c, d) => Key::Record(a, b, c.to_string(), d.to_owned()),
		}
	}
}
