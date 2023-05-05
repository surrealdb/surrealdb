use crate::key::lq::Lq;
use crate::key::lv::Lv;

async fn a_live_query(
	tx: &mut Transaction,
	cl: Uuid,
	namespace: &str,
	database: &str,
	table: &str,
	lq: Uuid,
) -> Result<(), Error> {
	let lq_key = Lq::new(cl, namespace, database, lq);
	tx.put(&lq_key, table).await?;

	let lv_key = Lv::new(namespace, database, table, lq);
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
