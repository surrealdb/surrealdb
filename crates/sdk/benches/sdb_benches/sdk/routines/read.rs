use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_core::val::RecordIdKey;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::sdb_benches::sdk::Record;

pub struct Read {
	runtime: &'static Runtime,
	table_name: String,
}

impl Read {
	pub fn new(runtime: &'static Runtime) -> Self {
		Self {
			runtime,
			table_name: format!("table_{}", RecordIdKey::rand()),
		}
	}
}

impl super::Routine for Read {
	fn setup(&self, client: &'static Surreal<Any>, num_ops: usize) {
		self.runtime.block_on(async {
			client.query(format!("DEFINE TABLE {}", self.table_name)).await.unwrap();
			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for task_id in 0..num_ops {
				let table_name = self.table_name.clone();

				tasks.spawn(async move {
					let _: Option<Record> = client
						.create((table_name, task_id as i64))
						.content(Record {
							field: RecordIdKey::rand(),
						})
						.await
						.expect("[setup] create record failed")
						.expect("[setup] the create operation returned None");
				});
			}

			while let Some(task) = tasks.join_next().await {
				task.unwrap();
			}
		});
	}

	fn run(&self, client: &'static Surreal<Any>, num_ops: usize) {
		self.runtime.block_on(async {
			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for task_id in 0..num_ops {
				let table_name = self.table_name.clone();

				tasks.spawn(async move {
					let _: Option<Record> = criterion::black_box(
						client
							.select((table_name, task_id as i64))
							.await
							.expect("[run] select operation failed")
							.expect("[run] the select operation returned None"),
					);
				});
			}

			while let Some(task) = tasks.join_next().await {
				task.unwrap();
			}
		});
	}

	fn cleanup(&self, client: &'static Surreal<Any>, _num_ops: usize) {
		self.runtime.block_on(async {
			client
				.query(format!("REMOVE table {}", self.table_name))
				.await
				.expect("[cleanup] remove table failed")
				.check()
				.unwrap();
		});
	}
}
