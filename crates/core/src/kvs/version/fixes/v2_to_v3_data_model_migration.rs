use std::sync::Arc;

use crate::{
	err::Error,
	kvs::{util, Transaction},
	mdl::namespace::Namespace,
	sql::statements::DefineNamespaceStatement,
};

/// Migrate the internal data model from v2 to v3.
///
/// The v2 data model stored the AST in the database, while the v3 data model begins the
/// migration to a data model separated from the AST.
pub async fn v2_to_v3_data_model_migration(tx: Arc<Transaction>) -> Result<(), Error> {
	migrate_namespace_data_model(&tx).await?;

	Ok(())
}

/// Migrate the namespace data model from v2 to v3.
async fn migrate_namespace_data_model(tx: &Arc<Transaction>) -> Result<(), Error> {
	let beg = crate::key::root::ns::prefix();
	let end = crate::key::root::ns::suffix();
	let ast_ns_values = tx.getr(beg..end, None).await?;
	let ast_ns_statements: Arc<[DefineNamespaceStatement]> =
		util::deserialize_cache(ast_ns_values.iter().map(|x| x.1.as_slice()))?;
	for ns_statement in ast_ns_statements.iter() {
		let ns = Namespace::from(ns_statement);

		let key = crate::key::root::ns::new(ns.name.as_str());
		tx.set(key, revision::to_vec(&ns)?, None).await?;
	}

	Ok(())
}
