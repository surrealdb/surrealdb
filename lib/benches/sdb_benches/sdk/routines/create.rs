use surrealdb::{engine::any::Any, sql::Id, Surreal};
use tokio::{runtime::Runtime, task::JoinSet};

use crate::sdb_benches::sdk::Record;

pub struct Create {
	runtime: &'static Runtime,
	table_name: String,
}

impl Create {
	pub fn new(runtime: &'static Runtime) -> Self {
		Self {
			runtime,
			table_name: format!("table_{}", Id::rand().to_raw()),
		}
	}
}

impl super::Routine for Create {
	fn setup(&self, _client: &'static Surreal<Any>, _num_ops: usize) {}

	fn run(&self, client: &'static Surreal<Any>, num_ops: usize) {
		self.runtime.block_on(async {
			let data = Record {
				field: Id::rand(),
			};

			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for _ in 0..num_ops {
				let table_name = self.table_name.clone();
				let data = data.clone();

				tasks.spawn(async move {
					let res: Vec<Record> = criterion::black_box(
						client
							.create(table_name)
							.content(data)
							.await
							.expect("[run] record creation failed"),
					);

					assert_eq!(
						res.len(),
						1,
						"[run] expected record creation to return 1 record, got {}",
						res.len()
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
				.query(format!("REMOVE TABLE {}", self.table_name))
				.await
				.expect("[cleanup] remove table failed");
		});
	}
}
