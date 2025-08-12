use std::sync::Arc;

use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::val::RecordIdKey;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

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
	fn setup(&self, ds: Arc<Datastore>, session: Session, num_ops: usize) {
		self.runtime.block_on(async {
			// Create table
			let mut res = ds
				.execute(format!("DEFINE TABLE {}", &self.table_name).as_str(), &session, None)
				.await
				.expect("[setup] define table failed");
			let _ = res.remove(0).output().expect("[setup] the create operation returned no value");

			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for task_id in 0..num_ops {
				let ds = ds.clone();
				let session = session.clone();
				let table_name = self.table_name.clone();

				tasks.spawn_on(
					async move {
						let mut res = ds
							.execute(
								format!(
									"CREATE {}:{} SET field = '{}'",
									&table_name,
									task_id,
									RecordIdKey::rand()
								)
								.as_str(),
								&session,
								None,
							)
							.await
							.expect("[setup] create record failed");
						let res = res
							.remove(0)
							.output()
							.expect("[setup] the create operation returned no value");
						if res.is_nullish() {
							panic!("[setup] Record not found");
						}
					},
					self.runtime.handle(),
				);
			}

			while let Some(task) = tasks.join_next().await {
				task.unwrap();
			}
		});
	}

	fn run(&self, ds: Arc<Datastore>, session: Session, num_ops: usize) {
		self.runtime.block_on(async {
			// Spawn one task for each operation
			let mut tasks = JoinSet::default();
			for task_id in 0..num_ops {
				let ds = ds.clone();
				let session = session.clone();
				let table_name = self.table_name.clone();

				tasks.spawn_on(
					async move {
						let mut res = criterion::black_box(
							ds.execute(
								format!(
									"SELECT * FROM {}:{} WHERE field = '{}'",
									&table_name,
									task_id,
									RecordIdKey::rand()
								)
								.as_str(),
								&session,
								None,
							)
							.await
							.expect("[run] select operation failed"),
						);
						let res = res
							.remove(0)
							.output()
							.expect("[run] the select operation returned no value");
						if res.is_nullish() {
							panic!("[run] Record not found");
						}
					},
					self.runtime.handle(),
				);
			}

			while let Some(task) = tasks.join_next().await {
				task.unwrap();
			}
		});
	}

	fn cleanup(&self, ds: Arc<Datastore>, session: Session, _num_ops: usize) {
		self.runtime.block_on(async {
			let mut res = ds
				.execute(format!("REMOVE TABLE {}", self.table_name).as_str(), &session, None)
				.await
				.expect("[cleanup] remove table failed");
			let _ =
				res.remove(0).output().expect("[cleanup] the remove operation returned no value");
		});
	}
}
