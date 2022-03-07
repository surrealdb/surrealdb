use super::Transaction;
use crate::err::Error;
use crate::kvs::Key;
use crate::kvs::Val;
use crate::sql;
use sql::statements::DefineDatabaseStatement;
use sql::statements::DefineEventStatement;
use sql::statements::DefineFieldStatement;
use sql::statements::DefineIndexStatement;
use sql::statements::DefineLoginStatement;
use sql::statements::DefineNamespaceStatement;
use sql::statements::DefineScopeStatement;
use sql::statements::DefineTableStatement;
use sql::statements::DefineTokenStatement;
use sql::statements::LiveStatement;

pub trait Convert<T> {
	fn convert(self) -> T;
}

impl<T> Convert<Vec<T>> for Vec<(Key, Val)>
where
	T: From<Val>,
{
	fn convert(self) -> Vec<T> {
		self.into_iter().map(|(_, v)| v.into()).collect()
	}
}

impl Transaction {
	// Get all namespaces
	pub async fn all_ns(&mut self) -> Result<Vec<DefineNamespaceStatement>, Error> {
		let beg = crate::key::ns::new(crate::key::PREFIX);
		let end = crate::key::ns::new(crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all namespace logins
	pub async fn all_nl(&mut self, ns: &str) -> Result<Vec<DefineLoginStatement>, Error> {
		let beg = crate::key::nl::new(ns, crate::key::PREFIX);
		let end = crate::key::nl::new(ns, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all namespace tokens
	pub async fn all_nt(&mut self, ns: &str) -> Result<Vec<DefineTokenStatement>, Error> {
		let beg = crate::key::nt::new(ns, crate::key::PREFIX);
		let end = crate::key::nt::new(ns, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all databases
	pub async fn all_db(&mut self, ns: &str) -> Result<Vec<DefineDatabaseStatement>, Error> {
		let beg = crate::key::db::new(ns, crate::key::PREFIX);
		let end = crate::key::db::new(ns, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all database logins
	pub async fn all_dl(&mut self, ns: &str, db: &str) -> Result<Vec<DefineLoginStatement>, Error> {
		let beg = crate::key::dl::new(ns, db, crate::key::PREFIX);
		let end = crate::key::dl::new(ns, db, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all database tokens
	pub async fn all_dt(&mut self, ns: &str, db: &str) -> Result<Vec<DefineTokenStatement>, Error> {
		let beg = crate::key::dt::new(ns, db, crate::key::PREFIX);
		let end = crate::key::dt::new(ns, db, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all scopes
	pub async fn all_sc(&mut self, ns: &str, db: &str) -> Result<Vec<DefineScopeStatement>, Error> {
		let beg = crate::key::sc::new(ns, db, crate::key::PREFIX);
		let end = crate::key::sc::new(ns, db, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all scope tokens
	pub async fn all_st(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
	) -> Result<Vec<DefineTokenStatement>, Error> {
		let beg = crate::key::st::new(ns, db, sc, crate::key::PREFIX);
		let end = crate::key::st::new(ns, db, sc, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all tables
	pub async fn all_tb(&mut self, ns: &str, db: &str) -> Result<Vec<DefineTableStatement>, Error> {
		let beg = crate::key::tb::new(ns, db, crate::key::PREFIX);
		let end = crate::key::tb::new(ns, db, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all events
	pub async fn all_ev(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Vec<DefineEventStatement>, Error> {
		let beg = crate::key::ev::new(ns, db, tb, crate::key::PREFIX);
		let end = crate::key::ev::new(ns, db, tb, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all fields
	pub async fn all_fd(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Vec<DefineFieldStatement>, Error> {
		let beg = crate::key::fd::new(ns, db, tb, crate::key::PREFIX);
		let end = crate::key::fd::new(ns, db, tb, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all fields
	pub async fn all_ix(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Vec<DefineIndexStatement>, Error> {
		let beg = crate::key::ix::new(ns, db, tb, crate::key::PREFIX);
		let end = crate::key::ix::new(ns, db, tb, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all views
	pub async fn all_ft(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Vec<DefineTableStatement>, Error> {
		let beg = crate::key::ft::new(ns, db, tb, crate::key::PREFIX);
		let end = crate::key::ft::new(ns, db, tb, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
	// Get all lives
	pub async fn all_lv(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<Vec<LiveStatement>, Error> {
		let beg = crate::key::lv::new(ns, db, tb, crate::key::PREFIX);
		let end = crate::key::lv::new(ns, db, tb, crate::key::SUFFIX);
		let val = self.getr(beg..end, u32::MAX).await?;
		Ok(val.convert())
	}
}

impl Transaction {
	// Get a namespace
	pub async fn get_ns(&mut self, ns: &str) -> Result<DefineNamespaceStatement, Error> {
		let key = crate::key::ns::new(ns);
		let val = self.get(key).await?.ok_or(Error::NsNotFound)?;
		Ok(val.into())
	}
	// Get a namespace login
	pub async fn get_nl(&mut self, ns: &str, nl: &str) -> Result<DefineLoginStatement, Error> {
		let key = crate::key::nl::new(ns, nl);
		let val = self.get(key).await?.ok_or(Error::NlNotFound)?;
		Ok(val.into())
	}
	// Get a namespace token
	pub async fn get_nt(&mut self, ns: &str, nt: &str) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::nt::new(ns, nt);
		let val = self.get(key).await?.ok_or(Error::NtNotFound)?;
		Ok(val.into())
	}
	// Get a database
	pub async fn get_db(&mut self, ns: &str, db: &str) -> Result<DefineDatabaseStatement, Error> {
		let key = crate::key::db::new(ns, db);
		let val = self.get(key).await?.ok_or(Error::DbNotFound)?;
		Ok(val.into())
	}
	// Get a database login
	pub async fn get_dl(
		&mut self,
		ns: &str,
		db: &str,
		dl: &str,
	) -> Result<DefineLoginStatement, Error> {
		let key = crate::key::dl::new(ns, db, dl);
		let val = self.get(key).await?.ok_or(Error::DlNotFound)?;
		Ok(val.into())
	}
	// Get a database token
	pub async fn get_dt(
		&mut self,
		ns: &str,
		db: &str,
		dt: &str,
	) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::dt::new(ns, db, dt);
		let val = self.get(key).await?.ok_or(Error::DtNotFound)?;
		Ok(val.into())
	}
	// Get a scope
	pub async fn get_sc(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
	) -> Result<DefineScopeStatement, Error> {
		let key = crate::key::sc::new(ns, db, sc);
		let val = self.get(key).await?.ok_or(Error::ScNotFound)?;
		Ok(val.into())
	}
	// Get a scope token
	pub async fn get_st(
		&mut self,
		ns: &str,
		db: &str,
		sc: &str,
		st: &str,
	) -> Result<DefineTokenStatement, Error> {
		let key = crate::key::st::new(ns, db, sc, st);
		let val = self.get(key).await?.ok_or(Error::StNotFound)?;
		Ok(val.into())
	}
	// Get a table
	pub async fn get_tb(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
	) -> Result<DefineTableStatement, Error> {
		let key = crate::key::tb::new(ns, db, tb);
		let val = self.get(key).await?.ok_or(Error::TbNotFound)?;
		Ok(val.into())
	}
}

impl Transaction {
	// Get all namespaces
	pub async fn add_ns(&mut self, ns: &str) -> Result<(), Error> {
		let key = crate::key::ns::new(ns);
		self.put(
			key,
			DefineNamespaceStatement {
				name: ns.to_owned(),
			},
		)
		.await
	}
	// Get all namespace logins
	pub async fn add_db(&mut self, ns: &str, db: &str) -> Result<(), Error> {
		let key = crate::key::db::new(ns, db);
		self.put(
			key,
			DefineDatabaseStatement {
				name: db.to_owned(),
			},
		)
		.await
	}
	// Get all namespace tokens
	pub async fn add_tb(&mut self, ns: &str, db: &str, tb: &str) -> Result<(), Error> {
		let key = crate::key::tb::new(ns, db, tb);
		self.put(
			key,
			DefineTableStatement {
				name: tb.to_owned(),
				..DefineTableStatement::default()
			},
		)
		.await
	}
}
