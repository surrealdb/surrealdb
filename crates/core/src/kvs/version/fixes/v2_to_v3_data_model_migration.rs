use std::sync::Arc;

use crate::{
	err::Error,
	kvs::{util, Transaction},
	cat::namespace::Namespace,
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
	for ns_statement in ast_ns_statements.into_iter() {
		// If the namespace ID is not set, generate a new one.
		// This is a workaround for the fact that the ID was mistakenly being thrown away in v2.
		let ns_statement = if ns_statement.id.is_none() {
			let mut ns_statement = ns_statement.clone();
			ns_statement.id = Some(tx.lock().await.get_next_ns_id().await?.into_inner());
			ns_statement
		} else {
			ns_statement.clone()
		};

		let ns = Namespace::try_from_statement(tx, &ns_statement).await?;

		let key = crate::key::root::ns::new(ns.name.as_str());
		tx.set(key, revision::to_vec(&ns)?, None).await?;
	}

	Ok(())
}
