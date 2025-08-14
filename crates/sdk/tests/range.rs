mod helpers;
use surrealdb::Result;

use crate::helpers::Test;

#[tokio::test]
async fn select_start_limit_fetch() -> Result<()> {
	let sql = "
		CREATE tag:rs SET name = 'Rust';
		CREATE tag:go SET name = 'Golang';
		CREATE tag:js SET name = 'JavaScript';
		CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
		CREATE person:jaime SET tags = [tag:js];
		SELECT * FROM person LIMIT 1 FETCH tags;
		SELECT * FROM person START 1 LIMIT 1 FETCH tags;
		SELECT * FROM person START 1 LIMIT 1 FETCH tags EXPLAIN FULL;
	";
	let mut t = Test::new(sql).await?;
	t.expect_size(8)?;
	//
	t.expect_val(
		"[
			{
				id: tag:rs,
				name: 'Rust'
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: tag:go,
				name: 'Golang'
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: tag:js,
				name: 'JavaScript'
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: person:tobie,
				tags: [tag:rs, tag:go, tag:js]
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: person:jaime,
				tags: [tag:js]
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: person:jaime,
				tags: [
					{
						id: tag:js,
						name: 'JavaScript'
					}
				]
			}
		]",
	)?;
	//
	t.expect_val(
		"[
			{
				id: person:tobie,
				tags: [
					{
						id: tag:rs,
						name: 'Rust'
					},
					{
						id: tag:go,
						name: 'Golang'
					},
					{
						id: tag:js,
						name: 'JavaScript'
					}
				]
			}
		]",
	)?;
	//
	t.expect_val(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'person'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
				},
				{
					detail: {
						CancelOnLimit: 1,
						SkipStart: 1
					},
					operation: 'StartLimitStrategy'
				},
				{
					detail: {
						count: 1
					},
					operation: 'Fetch'
				}
			]",
	)?;
	Ok(())
}
