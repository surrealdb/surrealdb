//! Records the defaults of the release that created a datastore.
//!
//! A datastore records the defaults its databases take when a statement names none
//! ([`StoreDefaults`]). A datastore this release creates holds no entry and its databases take
//! [`StoreDefaults::CURRENT`]; one an earlier release created holds none either, and its
//! databases were all created under that release's defaults. This migration records those
//! ([`StoreDefaults::LEGACY`]), so a database created after the upgrade takes the level its
//! siblings stand at until an operator raises the default with `ALTER SYSTEM COMPATIBILITY 4`.
//!
//! # Which nodes read what it writes
//!
//! No earlier release reads the entry, so a node on one ignores it.
//!
//! # Re-entrancy
//!
//! The entry is written only where none stands, in the transaction that reads it, so running
//! again leaves an entry already written, by this migration or by an operator, as it is.

use anyhow::Result;

use crate::key::schema::StoreDefaultsKey;
use crate::kvs::TransactionType;
use crate::kvs::ds::Datastore;
use crate::kvs::version::StoreDefaults;

/// Records [`StoreDefaults::LEGACY`] on a datastore that holds no defaults.
pub(super) async fn record_the_defaults_of_an_earlier_release(ds: &Datastore) -> Result<()> {
	let txn = ds.transaction(TransactionType::Write).await?;
	if catch!(txn, txn.get_key(&StoreDefaultsKey {}, None).await).is_none() {
		catch!(txn, txn.set_key(&StoreDefaultsKey {}, &StoreDefaults::LEGACY).await);
	}
	txn.commit().await
}
