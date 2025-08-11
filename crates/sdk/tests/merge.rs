mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn merge_record() -> Result<()> {
	let sql = "
		UPSERT person:test SET name.initials = 'TMH', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
		UPSERT person:test MERGE {
			name: {
				title: 'Mr',
				initials: NONE,
				suffix: ['BSc', 'MSc'],
			}
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: {
					initials: 'TMH',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					suffix: ['BSc', 'MSc'],
				}
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
