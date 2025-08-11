use surrealdb::Surreal;
use surrealdb::engine::any::Any;
use surrealdb_core::val::RecordIdKey;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::sdb_benches::sdk::Record;

pub struct Create {
	runtime: &'static Runtime,
	table_name: String,
}

impl Create {
	pub fn new(runtime: &'static Runtime) -> Self {
		Self {
			runtime,
			table_name: format!("table_{}", RecordIdKey::rand()),
		}
	}
}

impl super::Routine for Create {
	fn setup(&self, _client: &'static Surreal<Any>, _num_ops: usize) {}

	fn run(&self, client: &'static Surreal<Any>, num_ops: usize) {
		self.runtime.block_on(async {
			let data = Record {
				field: RecordIdKey::rand(),
			};

			client.query(format!("DEFINE TABLE {}", self.table_name)).await.unwrap();

			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for _ in 0..num_ops {
				let table_name = self.table_name.clone();
				let data = data.clone();

				tasks.spawn(async move {
					let res: Option<Record> = criterion::black_box(
						client
							.create(table_name)
							.content(data)
							.await
							.expect("[run] record creation failed"),
					);

					res.expect("[run] record creation should return a result");
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
				.query(format!("REMOVE TABLE {}", self.table_name))
				.await
				.expect("[cleanup] remove table failed");
		});
	}
}
