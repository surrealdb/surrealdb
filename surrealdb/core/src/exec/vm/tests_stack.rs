//! A statement runs on the native stack of the thread that polls it, and an
//! unoptimised build keeps every future an `.await` builds in the frame that
//! awaits it. The statements a client sends most — create, select, update and
//! delete of a record, a delete that cascades to the edges of its record —
//! run on a thread of the 2 MiB stack a test or a spawned thread gets.

use crate::exec::operators::test_util::TestDb;

const STACK: usize = 2 << 20;

/// `statements` run in turn as the owner on a thread with a stack of `STACK`
/// bytes, over a datastore set up with `setup` elsewhere.
fn on_a_small_stack(setup: &'static str, statements: &'static [&'static str]) {
	let runtime =
		|| tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	let ds = std::thread::spawn(move || {
		runtime().block_on(async { std::sync::Arc::clone(TestDb::new(setup).await.datastore()) })
	})
	.join()
	.expect("set up");
	std::thread::Builder::new()
		.stack_size(STACK)
		.spawn(move || {
			runtime().block_on(async {
				for sql in statements {
					for response in ds.execute(sql, &TestDb::owner(), None).await.expect("runs") {
						response.result.unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
					}
				}
			})
		})
		.expect("a thread")
		.join()
		.expect("the statements ran within the stack");
}

#[test]
fn a_record_is_created_read_updated_and_deleted_on_a_small_stack() {
	on_a_small_stack(
		"",
		&[
			"CREATE user:john",
			"SELECT * FROM user:john",
			"UPDATE user:john SET name = 'John'",
			"DELETE user:john RETURN BEFORE",
			"DELETE user:jane",
		],
	);
}

#[test]
fn a_delete_cascading_to_edges_runs_on_a_small_stack() {
	on_a_small_stack(
		"CREATE user:john, user:jane; RELATE user:john->knows->user:jane;",
		&["DELETE user:john RETURN BEFORE", "SELECT * FROM knows"],
	);
}
