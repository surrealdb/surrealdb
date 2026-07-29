//! Failures raised when a catalog definition is absent or already present.
//!
//! The catalog is the set of definitions the database is described by, so most
//! of its failures are about a definition's presence: a lookup needed one that
//! is not there, or a definition wanted a name that is already taken. Three
//! are about a definition's shape instead - an analyzer a live index still
//! depends on, a method repeated across the `FOR` clauses of one `DEFINE API`,
//! and an index whose stored format predates what this build can read.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{AlreadyExistsError, Error as TypesError, NotFoundError};

use crate::val::TableName;

/// A failure in the catalog layer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
	/// The requested namespace does not exist
	#[error("The namespace '{name}' does not exist")]
	NsNotFound {
		name: String,
	},

	/// The requested database does not exist
	#[error("The database '{name}' does not exist")]
	DbNotFound {
		name: String,
	},

	/// The requested event does not exist
	#[error("The event '{name}' does not exist")]
	EvNotFound {
		name: String,
	},

	/// The requested function does not exist
	#[error("The function '{name}' does not exist")]
	FcNotFound {
		name: String,
	},

	/// The requested module does not exist
	#[error("The module '{name}' does not exist")]
	MdNotFound {
		name: String,
	},

	/// The requested field does not exist
	#[error("The field '{name}' does not exist")]
	FdNotFound {
		name: String,
	},

	/// The requested model does not exist
	#[error("The model 'ml::{name}' does not exist")]
	MlNotFound {
		name: String,
	},

	/// The cluster node does not exist
	#[error("The node '{uuid}' does not exist")]
	NdNotFound {
		uuid: String,
	},

	/// The requested param does not exist
	#[error("The param '${name}' does not exist")]
	PaNotFound {
		name: String,
	},

	/// The requested sequence does not exist
	#[error("The sequence '{name}' does not exist")]
	SeqNotFound {
		name: String,
	},

	/// The requested config does not exist
	#[error("The config for {name} does not exist")]
	CgNotFound {
		name: String,
	},

	/// The requested table does not exist
	#[error("The table '{name}' does not exist")]
	TbNotFound {
		name: TableName,
	},

	/// The requested api does not exist
	#[error("The api '{value}' does not exist")]
	ApNotFound {
		value: String,
	},

	/// The requested analyzer does not exist
	#[error("The analyzer '{name}' does not exist")]
	AzNotFound {
		name: String,
	},

	/// The requested bucket does not exist
	#[error("The bucket '{name}' does not exist")]
	BuNotFound {
		name: String,
	},

	/// The requested index does not exist
	#[error("The index '{name}' does not exist")]
	IxNotFound {
		name: String,
	},

	/// The requested root user does not exist
	#[error("The root user '{name}' does not exist")]
	UserRootNotFound {
		name: String,
	},

	/// The requested namespace user does not exist
	#[error("The user '{name}' does not exist in the namespace '{ns}'")]
	UserNsNotFound {
		name: String,
		ns: String,
	},

	/// The requested database user does not exist
	#[error("The user '{name}' does not exist in the database '{db}'")]
	UserDbNotFound {
		name: String,
		ns: String,
		db: String,
	},

	/// The requested root access method does not exist
	#[error("The root access method '{ac}' does not exist")]
	AccessRootNotFound {
		ac: String,
	},

	/// The requested root access grant does not exist
	#[error("The root access grant '{gr}' does not exist for '{ac}'")]
	AccessGrantRootNotFound {
		ac: String,
		gr: String,
	},

	/// The requested namespace access method does not exist
	#[error("The access method '{ac}' does not exist in the namespace '{ns}'")]
	AccessNsNotFound {
		ac: String,
		ns: String,
	},

	/// The requested namespace access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the namespace '{ns}'")]
	AccessGrantNsNotFound {
		ac: String,
		gr: String,
		ns: String,
	},

	/// The requested database access method does not exist
	#[error("The access method '{ac}' does not exist in the database '{db}'")]
	AccessDbNotFound {
		ac: String,
		ns: String,
		db: String,
	},

	/// The requested database access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the database '{db}'")]
	AccessGrantDbNotFound {
		ac: String,
		gr: String,
		ns: String,
		db: String,
	},

	/// The requested api already exists
	#[error("The api '{value}' already exists")]
	ApAlreadyExists {
		value: String,
	},

	/// The requested analyzer already exists
	#[error("The analyzer '{name}' already exists")]
	AzAlreadyExists {
		name: String,
	},

	/// The requested bucket already exists
	#[error("The bucket '{value}' already exists")]
	BuAlreadyExists {
		value: String,
	},

	/// The requested database already exists
	#[error("The database '{name}' already exists")]
	DbAlreadyExists {
		name: String,
	},

	/// The requested event already exists
	#[error("The event '{name}' already exists")]
	EvAlreadyExists {
		name: String,
	},

	/// The requested field already exists
	#[error("The field '{name}' already exists")]
	FdAlreadyExists {
		name: String,
	},

	/// The requested function already exists
	#[error("The function '{name}' already exists")]
	FcAlreadyExists {
		name: String,
	},

	/// The requested module already exists
	#[error("The module '{name}' already exists")]
	MdAlreadyExists {
		name: String,
	},

	/// The requested index already exists
	#[error("The index '{name}' already exists")]
	IxAlreadyExists {
		name: String,
	},

	/// The requested model already exists
	#[error("The model '{name}' already exists")]
	MlAlreadyExists {
		name: String,
	},

	/// The requested namespace already exists
	#[error("The namespace '{name}' already exists")]
	NsAlreadyExists {
		name: String,
	},

	/// The requested param already exists
	#[error("The param '${name}' already exists")]
	PaAlreadyExists {
		name: String,
	},

	/// The requested config already exists
	#[error("The config for {name} already exists")]
	CgAlreadyExists {
		name: String,
	},

	/// The requested sequence already exists
	#[error("The sequence '{name}' already exists")]
	SeqAlreadyExists {
		name: String,
	},

	/// The requested table already exists
	#[error("The table '{name}' already exists")]
	TbAlreadyExists {
		name: String,
	},

	/// The requested user already exists
	#[error("The root user '{name}' already exists")]
	UserRootAlreadyExists {
		name: String,
	},

	/// The requested namespace user already exists
	#[error("The user '{name}' already exists in the namespace '{ns}'")]
	UserNsAlreadyExists {
		name: String,
		ns: String,
	},

	/// The requested database user already exists
	#[error("The user '{name}' already exists in the database '{db}'")]
	UserDbAlreadyExists {
		name: String,
		ns: String,
		db: String,
	},

	/// The requested root access method already exists
	#[error("The root access method '{ac}' already exists")]
	AccessRootAlreadyExists {
		ac: String,
	},

	/// The requested namespace access method already exists
	#[error("The access method '{ac}' already exists in the namespace '{ns}'")]
	AccessNsAlreadyExists {
		ac: String,
		ns: String,
	},

	/// The requested database access method already exists
	#[error("The access method '{ac}' already exists in the database '{db}'")]
	AccessDbAlreadyExists {
		ac: String,
		ns: String,
		db: String,
	},

	/// The analyzer cannot be removed because it is referenced by an index
	#[error(
		"The analyzer '{name}' is in use by index '{index}' on table '{table}' and cannot be removed"
	)]
	AzInUse {
		name: String,
		table: String,
		index: String,
	},

	/// The same method appears in more than one `FOR` clause on a single
	/// `DEFINE API` statement
	#[error("The method '{method}' is defined in more than one FOR clause on api '{value}'")]
	ApMethodDuplicate {
		value: String,
		method: String,
	},

	/// An index was built by an older version whose on-disk format is no longer
	/// readable, and it must be rebuilt before it can be queried.
	#[error(
		"The index `{index}` on table `{table}` was built with an out-of-date on-disk format \
		 (Expected: {expected}, Actual: {actual}). Run `REBUILD INDEX {index} ON {table}` before querying it."
	)]
	IndexRebuildRequired {
		index: String,
		table: String,
		expected: u16,
		actual: u16,
	},
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			// Typed on the wire. Clients branch on these details, so these six
			// arms are the part of this mapper that is a compatibility surface.
			Error::NsNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Namespace {
					name,
				},
			),
			Error::DbNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Database {
					name,
				},
			),
			Error::TbNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Table {
					name: name.into_string(),
				},
			),
			Error::NsAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Namespace {
					name,
				},
			),
			Error::DbAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Database {
					name,
				},
			),
			Error::TbAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Table {
					name,
				},
			),

			// Untyped on the wire. Most of these could carry a `NotFound` or
			// `AlreadyExists` kind and none does, so giving one a kind is a
			// deliberate change to what clients receive rather than something
			// this mapper assumes. A kind chosen from the variant's name would
			// be actively wrong here: it would re-type every `AlreadyExists`
			// below as not-found, along with `AzInUse`, `ApMethodDuplicate` and
			// `IndexRebuildRequired`, which are neither.
			Error::EvNotFound {
				..
			}
			| Error::FcNotFound {
				..
			}
			| Error::MdNotFound {
				..
			}
			| Error::FdNotFound {
				..
			}
			| Error::MlNotFound {
				..
			}
			| Error::NdNotFound {
				..
			}
			| Error::PaNotFound {
				..
			}
			| Error::SeqNotFound {
				..
			}
			| Error::CgNotFound {
				..
			}
			| Error::ApNotFound {
				..
			}
			| Error::AzNotFound {
				..
			}
			| Error::BuNotFound {
				..
			}
			| Error::IxNotFound {
				..
			}
			| Error::UserRootNotFound {
				..
			}
			| Error::UserNsNotFound {
				..
			}
			| Error::UserDbNotFound {
				..
			}
			| Error::AccessRootNotFound {
				..
			}
			| Error::AccessGrantRootNotFound {
				..
			}
			| Error::AccessNsNotFound {
				..
			}
			| Error::AccessGrantNsNotFound {
				..
			}
			| Error::AccessDbNotFound {
				..
			}
			| Error::AccessGrantDbNotFound {
				..
			}
			| Error::ApAlreadyExists {
				..
			}
			| Error::AzAlreadyExists {
				..
			}
			| Error::BuAlreadyExists {
				..
			}
			| Error::EvAlreadyExists {
				..
			}
			| Error::FdAlreadyExists {
				..
			}
			| Error::FcAlreadyExists {
				..
			}
			| Error::MdAlreadyExists {
				..
			}
			| Error::IxAlreadyExists {
				..
			}
			| Error::MlAlreadyExists {
				..
			}
			| Error::PaAlreadyExists {
				..
			}
			| Error::CgAlreadyExists {
				..
			}
			| Error::SeqAlreadyExists {
				..
			}
			| Error::UserRootAlreadyExists {
				..
			}
			| Error::UserNsAlreadyExists {
				..
			}
			| Error::UserDbAlreadyExists {
				..
			}
			| Error::AccessRootAlreadyExists {
				..
			}
			| Error::AccessNsAlreadyExists {
				..
			}
			| Error::AccessDbAlreadyExists {
				..
			}
			| Error::AzInUse {
				..
			}
			| Error::ApMethodDuplicate {
				..
			}
			| Error::IndexRebuildRequired {
				..
			} => internal_todo(message),
		}
	}
}
