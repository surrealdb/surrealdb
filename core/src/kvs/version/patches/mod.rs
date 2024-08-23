use super::Version;
use crate::{err::Error, kvs::Datastore};

mod record_id_uuid;

pub trait VersionPatch {
	async fn apply(ds: Datastore, version: Version) -> Result<(), Error>;
}
