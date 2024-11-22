use super::lookup::Lookup;
use crate::sql::id::Id;
use uuid::Uuid;

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
	Nus(String),
	/// A cache key for namespace accesses
	Nas(String),
	/// A cache key for namespace access grants
	Ngs(String, String),
	/// A cache key for databases
	Dbs(String),
	/// A cache key for database users
	Dus(String, String),
	/// A cache key for database accesses
	Das(String, String),
	/// A cache key for database access grants
	Dgs(String, String, String),
	/// A cache key for analyzers (on a database)
	Azs(String, String),
	/// A cache key for functions (on a database)
	Fcs(String, String),
	/// A cache key for models (on a database)
	Mls(String, String),
	/// A cache key for configs (on a database)
	Cgs(String, String),
	/// A cache key for parameters (on a database)
	Pas(String, String),
	/// A cache key for tables
	Tbs(String, String),
	/// A cache key for events (on a table)
	Evs(String, String, String),
	/// A cache key for fieds (on a table)
	Fds(String, String, String),
	/// A cache key for views (on a table)
	Fts(String, String, String),
	/// A cache key for indexes (on a table)
	Ixs(String, String, String),
	/// A cache key for live queries (on a table)
	Lvs(String, String, String),
	/// A cache key for a node
	Nd(Uuid),
	/// A cache key for a root user
	Ru(String),
	/// A cache key for a root access
	Ra(String),
	/// A cache key for a root access grant
	Rg(String, String),
	/// A cache key for a namespace
	Ns(String),
	/// A cache key for a namespace user
	Nu(String, String),
	/// A cache key for a namespace access
	Na(String, String),
	/// A cache key for a namespace access grant
	Ng(String, String, String),
	/// A cache key for a database
	Db(String, String),
	/// A cache key for a database user
	Du(String, String, String),
	/// A cache key for a database access
	Da(String, String, String),
	/// A cache key for a database access grant
	Dg(String, String, String, String),
	/// A cache key for an analyzer (on a database)
	Az(String, String, String),
	/// A cache key for a function (on a database)
	Fc(String, String, String),
	/// A cache key for a model (on a database)
	Ml(String, String, String, String),
	/// A cache key for a config (on a database)
	Cg(String, String, String),
	/// A cache key for a parameter (on a database)
	Pa(String, String, String),
	/// A cache key for a table
	Tb(String, String, String),
	/// A cache key for an event (on a table)
	Ev(String, String, String, String),
	/// A cache key for a fied (on a table)
	Fd(String, String, String, String),
	/// A cache key for an index (on a table)
	Ix(String, String, String, String),
	/// A cache key for a record
	Record(String, String, String, Id),
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
			Lookup::Nus(a) => Key::Nus(a.to_string()),
			Lookup::Nas(a) => Key::Nas(a.to_string()),
			Lookup::Ngs(a, b) => Key::Ngs(a.to_string(), b.to_string()),
			Lookup::Dbs(a) => Key::Dbs(a.to_string()),
			Lookup::Dus(a, b) => Key::Dus(a.to_string(), b.to_string()),
			Lookup::Das(a, b) => Key::Das(a.to_string(), b.to_string()),
			Lookup::Dgs(a, b, c) => Key::Dgs(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Azs(a, b) => Key::Azs(a.to_string(), b.to_string()),
			Lookup::Fcs(a, b) => Key::Fcs(a.to_string(), b.to_string()),
			Lookup::Mls(a, b) => Key::Mls(a.to_string(), b.to_string()),
			Lookup::Cgs(a, b) => Key::Cgs(a.to_string(), b.to_string()),
			Lookup::Pas(a, b) => Key::Pas(a.to_string(), b.to_string()),
			Lookup::Tbs(a, b) => Key::Tbs(a.to_string(), b.to_string()),
			Lookup::Evs(a, b, c) => Key::Evs(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Fds(a, b, c) => Key::Fds(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Fts(a, b, c) => Key::Fts(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Ixs(a, b, c) => Key::Ixs(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Lvs(a, b, c) => Key::Lvs(a.to_string(), b.to_string(), c.to_string()),
			//
			Lookup::Nd(a) => Key::Nd(a),
			Lookup::Ru(a) => Key::Ru(a.to_string()),
			Lookup::Ra(a) => Key::Ra(a.to_string()),
			Lookup::Rg(a, b) => Key::Rg(a.to_string(), b.to_string()),
			Lookup::Ns(a) => Key::Ns(a.to_string()),
			Lookup::Nu(a, b) => Key::Nu(a.to_string(), b.to_string()),
			Lookup::Na(a, b) => Key::Na(a.to_string(), b.to_string()),
			Lookup::Ng(a, b, c) => Key::Ng(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Db(a, b) => Key::Db(a.to_string(), b.to_string()),
			Lookup::Du(a, b, c) => Key::Du(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Da(a, b, c) => Key::Da(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Dg(a, b, c, d) => Key::Dg(a.to_string(), b.to_string(), c.to_string(), d.to_string()),
			Lookup::Az(a, b, c) => Key::Az(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Fc(a, b, c) => Key::Fc(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Ml(a, b, c, d) => Key::Ml(a.to_string(), b.to_string(), c.to_string(), d.to_string()),
			Lookup::Cg(a, b, c) => Key::Cg(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Pa(a, b, c) => Key::Pa(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Tb(a, b, c) => Key::Tb(a.to_string(), b.to_string(), c.to_string()),
			Lookup::Ev(a, b, c, d) => Key::Ev(a.to_string(), b.to_string(), c.to_string(), d.to_string()),
			Lookup::Fd(a, b, c, d) => Key::Fd(a.to_string(), b.to_string(), c.to_string(), d.to_string()),
			Lookup::Ix(a, b, c, d) => Key::Ix(a.to_string(), b.to_string(), c.to_string(), d.to_string()),
			Lookup::Record(a, b, c, d) => Key::Record(a.to_string(), b.to_string(), c.to_string(), d.to_owned()),
		}
	}
}
