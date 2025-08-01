use super::key::Key;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::expr::id::Id;
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
	Nus(NamespaceId),
	/// A cache key for namespace accesses
	Nas(NamespaceId),
	/// A cache key for namespace access grants
	Ngs(NamespaceId, &'a str),
	/// A cache key for databases
	Dbs(NamespaceId),
	/// A cache key for database users
	Dus(NamespaceId, DatabaseId),
	/// A cache key for database accesses
	Das(NamespaceId, DatabaseId),
	/// A cache key for database access grants
	Dgs(NamespaceId, DatabaseId, &'a str),
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
	/// A cache key for sequences (on a database)
	Sqs(NamespaceId, DatabaseId),
	/// A cache key for tables
	Tbs(NamespaceId, DatabaseId),
	/// A cache key for events (on a table)
	Evs(NamespaceId, DatabaseId, &'a str),
	/// A cache key for fields (on a table)
	Fds(NamespaceId, DatabaseId, &'a str),
	/// A cache key for views (on a table)
	Fts(NamespaceId, DatabaseId, &'a str),
	/// A cache key for indexes (on a table)
	Ixs(NamespaceId, DatabaseId, &'a str),
	/// A cache key for live queries (on a table)
	Lvs(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a node
	Nd(Uuid),
	/// A cache key for a root user
	Ru(&'a str),
	/// A cache key for a root access
	Ra(&'a str),
	/// A cache key for a root access grant
	Rg(&'a str, &'a str),
	/// A cache key for a namespace
	NsByName(&'a str),
	/// A cache key for a namespace by id.
	NsById(NamespaceId),
	/// A cache key for a namespace user
	Nu(NamespaceId, &'a str),
	/// A cache key for a namespace access
	Na(NamespaceId, &'a str),
	/// A cache key for a namespace access grant
	Ng(NamespaceId, &'a str, &'a str),
	/// A cache key for a database by id.
	DbById(NamespaceId, DatabaseId),
	/// A cache key for a database by name.
	DbByName(&'a str, &'a str),
	/// A cache key for a database user
	Du(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a database access
	Da(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a database access grant
	Dg(NamespaceId, DatabaseId, &'a str, &'a str),
	/// A cache key for an api (on a database)
	Ap(NamespaceId, DatabaseId, &'a str),
	/// A cache key for an analyzer (on a database)
	Az(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a bucket (on a database)
	Bu(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a function (on a database)
	Fc(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a model (on a database)
	Ml(NamespaceId, DatabaseId, &'a str, &'a str),
	/// A cache key for a config (on a database)
	Cg(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a parameter (on a database)
	Pa(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a sequence (on a database)
	Sq(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a table by id.
	TbById(NamespaceId, DatabaseId, &'a str),
	/// A cache key for a table by name.
	TbByName(&'a str, &'a str, &'a str),
	/// A cache key for an event (on a table)
	Ev(NamespaceId, DatabaseId, &'a str, &'a str),
	/// A cache key for a field (on a table)
	Fd(NamespaceId, DatabaseId, &'a str, &'a str),
	/// A cache key for an index (on a table)
	Ix(NamespaceId, DatabaseId, &'a str, &'a str),
	/// A cache key for a record
	Record(NamespaceId, DatabaseId, &'a str, &'a Id),
}

impl Equivalent<Key> for Lookup<'_> {
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
			(Self::Aps(la, lb), Key::Aps(ka, kb)) => la == ka && lb == kb,
			(Self::Azs(la, lb), Key::Azs(ka, kb)) => la == ka && lb == kb,
			(Self::Bus(la, lb), Key::Bus(ka, kb)) => la == ka && lb == kb,
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
			(Self::NsById(la), Key::NsById(ka)) => la == ka,
			(Self::NsByName(la), Key::NsByName(ka)) => la == ka,
			(Self::Nu(la, lb), Key::Nu(ka, kb)) => la == ka && lb == kb,
			(Self::Na(la, lb), Key::Na(ka, kb)) => la == ka && lb == kb,
			(Self::Ng(la, lb, lc), Key::Ng(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::DbById(la, lb), Key::DbById(ka, kb)) => la == ka && lb == kb,
			(Self::DbByName(la, lb), Key::DbByName(ka, kb)) => la == ka && lb == kb,
			(Self::Du(la, lb, lc), Key::Du(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Da(la, lb, lc), Key::Da(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Dg(la, lb, lc, ld), Key::Dg(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ap(la, lb, lc), Key::Ap(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Az(la, lb, lc), Key::Az(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Bu(la, lb, lc), Key::Bu(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Fc(la, lb, lc), Key::Fc(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Ml(la, lb, lc, ld), Key::Ml(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Cg(la, lb, lc), Key::Cg(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Pa(la, lb, lc), Key::Pa(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Sq(la, lb, lc), Key::Sq(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::TbById(la, lb, lc), Key::TbById(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::TbByName(la, lb, lc), Key::TbByName(ka, kb, kc)) => la == ka && lb == kb && lc == kc,
			(Self::Ev(la, lb, lc, ld), Key::Ev(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Fd(la, lb, lc, ld), Key::Fd(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Ix(la, lb, lc, ld), Key::Ix(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && ld == kd,
			(Self::Record(la, lb, lc, ld), Key::Record(ka, kb, kc, kd)) => la == ka && lb == kb && lc == kc && *ld == kd,
			//
			_ => false,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use rstest::rstest;

	// TODO: STU: Add tests for the code above.
}
