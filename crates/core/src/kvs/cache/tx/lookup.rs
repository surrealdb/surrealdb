use super::key::Key;
use crate::sql::id::Id;
use quick_cache::Equivalent;
use uuid::Uuid;

#[derive(Hash, Eq, PartialEq)]
pub(crate) enum Lookup<'a> {
	/// A cache key for nodes
	Nds,
	/// A cache key for root users
	Rus,
	/// A cache key for root accesses
	Ras,
	/// A cache key for root access grants
	Rgs(&'a str),
	/// A cache key for namespaces
	Nss,
	/// A cache key for namespace users
	Nus(&'a str),
	/// A cache key for namespace accesses
	Nas(&'a str),
	/// A cache key for namespace access grants
	Ngs(&'a str, &'a str),
	/// A cache key for databases
	Dbs(&'a str),
	/// A cache key for database users
	Dus(&'a str, &'a str),
	/// A cache key for database accesses
	Das(&'a str, &'a str),
	/// A cache key for database access grants
	Dgs(&'a str, &'a str, &'a str),
	/// A cache key for analyzers (on a database)
	Azs(&'a str, &'a str),
	/// A cache key for functions (on a database)
	Fcs(&'a str, &'a str),
	/// A cache key for models (on a database)
	Mls(&'a str, &'a str),
	/// A cache key for configs (on a database)
	Cgs(&'a str, &'a str),
	/// A cache key for parameters (on a database)
	Pas(&'a str, &'a str),
	/// A cache key for tables
	Tbs(&'a str, &'a str),
	/// A cache key for events (on a table)
	Evs(&'a str, &'a str, &'a str),
	/// A cache key for fields (on a table)
	Fds(&'a str, &'a str, &'a str),
	/// A cache key for views (on a table)
	Fts(&'a str, &'a str, &'a str),
	/// A cache key for indexes (on a table)
	Ixs(&'a str, &'a str, &'a str),
	/// A cache key for live queries (on a table)
	Lvs(&'a str, &'a str, &'a str),
	/// A cache key for a node
	Nd(Uuid),
	/// A cache key for a root user
	Ru(&'a str),
	/// A cache key for a root access
	Ra(&'a str),
	/// A cache key for a root access grant
	Rg(&'a str, &'a str),
	/// A cache key for a namespace
	Ns(&'a str),
	/// A cache key for a namespace user
	Nu(&'a str, &'a str),
	/// A cache key for a namespace access
	Na(&'a str, &'a str),
	/// A cache key for a namespace access grant
	Ng(&'a str, &'a str, &'a str),
	/// A cache key for a database
	Db(&'a str, &'a str),
	/// A cache key for a database user
	Du(&'a str, &'a str, &'a str),
	/// A cache key for a database access
	Da(&'a str, &'a str, &'a str),
	/// A cache key for a database access grant
	Dg(&'a str, &'a str, &'a str, &'a str),
	/// A cache key for an analyzer (on a database)
	Az(&'a str, &'a str, &'a str),
	/// A cache key for a function (on a database)
	Fc(&'a str, &'a str, &'a str),
	/// A cache key for a model (on a database)
	Ml(&'a str, &'a str, &'a str, &'a str),
	/// A cache key for a config (on a database)
	Cg(&'a str, &'a str, &'a str),
	/// A cache key for a parameter (on a database)
	Pa(&'a str, &'a str, &'a str),
	/// A cache key for a table
	Tb(&'a str, &'a str, &'a str),
	/// A cache key for an event (on a table)
	Ev(&'a str, &'a str, &'a str, &'a str),
	/// A cache key for a field (on a table)
	Fd(&'a str, &'a str, &'a str, &'a str),
	/// A cache key for an index (on a table)
	Ix(&'a str, &'a str, &'a str, &'a str),
	/// A cache key for a record
	Record(&'a str, &'a str, &'a str, &'a Id),
}

impl<'a> Equivalent<Key> for Lookup<'a> {
	#[rustfmt::skip]
	fn equivalent(&self, key: &Key) -> bool {
		match (self, key) {
			//
			(Self::Nds, Key::Nds) => true,
			(Self::Rus, Key::Rus) => true,
			(Self::Ras, Key::Ras) => true,
			(Self::Rgs(la), Key::Rgs(ka)) => la == ka,
			(Self::Nss, Key::Nss) => true,
			(Self::Nus(la), Key::Nus(ka)) => la == ka,
			(Self::Nas(la), Key::Nas(ka)) => la == ka,
			(Self::Ngs(la, lb), Key::Ngs(ka, kb)) => la == ka && lb == kb,
			(Self::Dbs(la), Key::Dbs(ka)) => la == ka,
			(Self::Dus(la, lb), Key::Dus(ka, kb)) => la == ka && lb == kb,
			(Self::Das(la, lb), Key::Das(ka, kb)) => la == ka && lb == kb,
			(Self::Dgs(la, lb, lc), Key::Dgs(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Azs(la, lb), Key::Azs(ka, kb)) => la == ka && lb == kb,
			(Self::Fcs(la, lb), Key::Fcs(ka, kb)) => la == ka && lb == kb,
			(Self::Mls(la, lb), Key::Mls(ka, kb)) => la == ka && lb == kb,
			(Self::Cgs(la, lb), Key::Cgs(ka, kb)) => la == ka && lb == kb,
			(Self::Pas(la, lb), Key::Pas(ka, kb)) => la == ka && lb == kb,
			(Self::Tbs(la, lb), Key::Tbs(ka, kb)) => la == ka && lb == kb,
			(Self::Evs(la, lb, lc), Key::Evs(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Fds(la, lb, lc), Key::Fds(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Fts(la, lb, lc), Key::Fts(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Ixs(la, lb, lc), Key::Ixs(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Lvs(la, lb, lc), Key::Lvs(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			//
			(Self::Nd(la), Key::Nd(ka)) => la == ka,
			(Self::Ru(la), Key::Ru(ka)) => la == ka,
			(Self::Ra(la), Key::Ra(ka)) => la == ka,
			(Self::Rg(la, lb), Key::Rg(ka, kb)) => la == ka && lb == kb,
			(Self::Ns(la), Key::Ns(ka)) => la == ka,
			(Self::Nu(la, lb), Key::Nu(ka, kb)) => la == ka && lb == kb,
			(Self::Na(la, lb), Key::Na(ka, kb)) => la == ka && lb == kb,
			(Self::Ng(la, lb, lc), Key::Ng(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Db(la, lb), Key::Db(ka, kb)) => la == ka && lb == kb,
			(Self::Du(la, lb, lc), Key::Du(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Da(la, lb, lc), Key::Da(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Dg(la, lb, lc, ld), Key::Dg(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Az(la, lb, lc), Key::Az(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Fc(la, lb, lc), Key::Fc(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Ml(la, lb, lc, ld), Key::Ml(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Cg(la, lb, lc), Key::Cg(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Pa(la, lb, lc), Key::Pa(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Tb(la, lb, lc), Key::Tb(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Ev(la, lb, lc, ld), Key::Ev(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fd(la, lb, lc, ld), Key::Fd(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ix(la, lb, lc, ld), Key::Ix(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Record(la, lb, lc, ld), Key::Record(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && *ld == kd,
			//
			_ => false,
		}
	}
}
