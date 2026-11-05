//! Isolation levels a transaction can be opened at, and the optional backend
//! capability that offers more than the default.
//!
//! Every backend's ordinary transaction is [`Isolation::Snapshot`]. A backend
//! that can also provide [`Isolation::Serializable`] publishes an
//! [`IsolationLevelsHandle`] through [`crate::TransactionBuilder::extension`]; a
//! backend that publishes none is opened as before and refuses `Serializable`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use common::future::BoxFut;

use crate::TransactionType;
use crate::api::Transactable;
use crate::err::Result;

/// How much of a transaction's reads join its conflict set at commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Isolation {
	/// The conflict set is the transaction's writes: two transactions that
	/// write the same key cannot both commit.
	Snapshot,
	/// The conflict set is the transaction's writes and every key and range it
	/// read, which also rules out write skew.
	Serializable,
}

impl fmt::Display for Isolation {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(match self {
			Self::Snapshot => "SNAPSHOT",
			Self::Serializable => "SERIALIZABLE",
		})
	}
}

/// A backend that opens transactions at a stated isolation level.
///
/// `open` is called only for a level in [`Self::supported`], and the
/// transaction it returns holds that level for its whole life.
pub trait IsolationLevels: Any + Send + Sync {
	/// The levels the backend opens transactions at. Includes
	/// [`Isolation::Snapshot`].
	fn supported(&self) -> &'static [Isolation];

	/// Opens a backend transaction at `isolation`, returning it and whether it
	/// is local to the process, as for [`crate::TransactionBuilder::new_transaction`].
	fn open(
		&self,
		write: TransactionType,
		isolation: Isolation,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>>;
}

/// The [`crate::TransactionBuilder::extension`] key under which a backend
/// publishes its [`IsolationLevels`] implementation. The newtype is what makes
/// the capability recoverable: `Arc<dyn Any>` downcasts only to a sized type.
pub struct IsolationLevelsHandle(pub Arc<dyn IsolationLevels>);
