use quick_cache::Weighter;

use super::entry::Entry;
use super::key::Key;

#[derive(Clone)]
pub(crate) struct Weight;

impl Weighter<Key, Entry> for Weight {
	fn weight(&self, key: &Key, _val: &Entry) -> u64 {
		match key {
			Key::DbByName(_, _)
			| Key::Dbs(_)
			| Key::Nds
			| Key::NsByName(_)
			| Key::Nss
			| Key::Tb(_, _, _)
			| Key::TbByName(_, _, _)
			| Key::Nd(_)
			| Key::Tbs(_, _) => 1,

			Key::Ap(_, _, _)
			| Key::Aps(_, _)
			| Key::Az(_, _, _)
			| Key::Azs(_, _)
			| Key::Bu(_, _, _)
			| Key::Bus(_, _)
			| Key::Cg(_, _, _)
			| Key::Cgs(_, _)
			| Key::Da(_, _, _)
			| Key::Das(_, _)
			| Key::Dg(_, _, _, _)
			| Key::Dgs(_, _, _)
			| Key::Du(_, _, _)
			| Key::Dus(_, _)
			| Key::Ev(_, _, _, _)
			| Key::Evs(_, _, _)
			| Key::Fc(_, _, _)
			| Key::Fcs(_, _)
			| Key::Fd(_, _, _, _)
			| Key::Fds(_, _, _)
			| Key::Fts(_, _, _)
			| Key::Ix(_, _, _, _)
			| Key::Ixs(_, _, _)
			| Key::Lvs(_, _, _)
			| Key::Md(_, _, _)
			| Key::Mds(_, _)
			| Key::Ml(_, _, _, _)
			| Key::Mls(_, _)
			| Key::Na(_, _)
			| Key::Nas(_)
			| Key::Ng(_, _, _)
			| Key::Ngs(_, _)
			| Key::Nu(_, _)
			| Key::Nus(_)
			| Key::Pa(_, _, _)
			| Key::Pas(_, _)
			| Key::Ra(_)
			| Key::Ras
			| Key::Rcg(_)
			| Key::Rg(_, _)
			| Key::Rgs(_)
			| Key::Ru(_)
			| Key::Rus
			| Key::Sq(_, _, _)
			| Key::Sqs(_, _) => 2,
		}
	}
}
