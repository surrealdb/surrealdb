/// Create a live query in the database that is tied with all required entries
async fn a_live_query(
	tx: &mut Transaction,
	cl: Uuid,
	namespace: &str,
	database: &str,
	table: &str,
	lq: Uuid,
) -> Result<(), Error> {
	// TODO Create a statement and write that because that is source of truth
	let lq_key = crate::key::node::lq::new(cl, lq, namespace, database);
	tx.put(&lq_key, table).await?;

	let lv_key = crate::key::table::lq::new(namespace, database, table, lq);
	let lv_val = LiveStatement {
		id: sql::Uuid::from(lq),
		node: cl,
		expr: Fields::default(),
		what: Value::from(table),
		cond: None,
		fetch: None,
		archived: None,
	};
	tx.put(&lv_key, lv_val).await
}
