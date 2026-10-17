//! A statement runs on the native stack of the thread that polls it, and an
//! unoptimised build keeps every future an `.await` builds in the frame that
//! awaits it. The statements a client sends most — create, select, update and
//! delete of a record, a delete that cascades to the edges of its record —
//! run on a thread of the 2 MiB stack a test or a spawned thread gets. So does
//! a cycle of links through COMPUTED fields, whose depth only the
//! computation-depth budget bounds.

use crate::exec::operators::test_util::TestDb;

const STACK: usize = 2 << 20;

/// `statements` run in turn as the owner on a thread with a stack of `STACK`
/// bytes, over a datastore set up with `setup` elsewhere: the answer of each
/// statement, its error rendered.
fn answers_on_a_small_stack(
	setup: &'static str,
	statements: &'static [&'static str],
) -> Vec<Result<(), String>> {
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
				let mut answers = Vec::new();
				for sql in statements {
					for response in ds.execute(sql, &TestDb::owner(), None).await.expect("runs") {
						answers.push(response.result.map(|_| ()).map_err(|e| e.to_string()));
					}
				}
				answers
			})
		})
		.expect("a thread")
		.join()
		.expect("the statements ran within the stack")
}

/// `statements` succeed in turn on a thread with a stack of `STACK` bytes
/// ([`answers_on_a_small_stack`]).
fn on_a_small_stack(setup: &'static str, statements: &'static [&'static str]) {
	for (at, answer) in answers_on_a_small_stack(setup, statements).into_iter().enumerate() {
		answer.unwrap_or_else(|e| panic!("statement {at} failed: {e}"));
	}
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

/// Each read through a cycle of links under a COMPUTED field — `.*` or a field
/// read, over a set, an array or a single link — ends in the computation-depth
/// error on a small stack: every level of the cycle is a nested evaluation of
/// the statement's driver, so the depth the budget admits costs the stack of
/// one level.
#[test]
fn a_cycle_of_links_through_computed_fields_ends_in_the_depth_error_on_a_small_stack() {
	let answers = answers_on_a_small_stack(
		"DEFINE FIELD friends ON person TYPE set<record<person>> DEFAULT <set>[];
		DEFINE FIELD docs ON person COMPUTED friends.*;
		CREATE person:1 SET friends = <set>[person:2] RETURN NONE;
		CREATE person:2 SET friends = <set>[person:1] RETURN NONE;
		DEFINE FIELD friends ON animal TYPE array<record<animal>> DEFAULT [];
		DEFINE FIELD docs ON animal COMPUTED friends.*;
		CREATE animal:1 SET friends = [animal:2] RETURN NONE;
		CREATE animal:2 SET friends = [animal:1] RETURN NONE;
		DEFINE FIELD me ON node COMPUTED self.*;
		CREATE node:1 SET self = node:1 RETURN NONE;
		DEFINE FIELD friends ON pet TYPE set<record<pet>> DEFAULT <set>[];
		DEFINE FIELD docs ON pet COMPUTED friends.docs;
		CREATE pet:1 SET friends = <set>[pet:2] RETURN NONE;
		CREATE pet:2 SET friends = <set>[pet:1] RETURN NONE;
		DEFINE FIELD friend ON link TYPE option<record<link>>;
		DEFINE FIELD doc ON link COMPUTED friend.doc;
		CREATE link:1 SET friend = link:2 RETURN NONE;
		CREATE link:2 SET friend = link:1 RETURN NONE;",
		&[
			"SELECT id, docs FROM person:1",
			"SELECT id, docs FROM animal:1",
			"SELECT id, me FROM node:1",
			"SELECT id, docs FROM pet:1",
			"SELECT id, doc FROM link:1",
		],
	);
	assert_eq!(answers.len(), 5);
	for (at, answer) in answers.into_iter().enumerate() {
		let error = answer.expect_err("a cycle of links ends in an error");
		assert!(
			error.contains("excessive computation depth"),
			"statement {at} failed with another error: {error}"
		);
	}
}

/// A cycle of links through a COMPUTED field that selects from an array of
/// links, which a fan-out evaluates, ends in the computation-depth error on a
/// small stack and in time: under the statement's driver the fan-out runs its
/// rows one at a time, so each level of the cycle is one nested evaluation and
/// the first row to fail ends the rows beside it.
#[test]
fn a_cycle_of_links_through_an_overlapped_fan_out_ends_in_the_depth_error_on_a_small_stack() {
	use crate::exec::operators::link_window::tests::{eaches, reset_fills};

	let ds = std::thread::spawn(|| {
		tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.expect("a runtime")
			.block_on(async {
				std::sync::Arc::clone(
					TestDb::new(
						"DEFINE FIELD friends ON crowd TYPE array<record<crowd>> DEFAULT [];
						DEFINE FIELD docs ON crowd COMPUTED { LET $f = friends; RETURN (SELECT * FROM $f); };
						CREATE crowd:1 SET friends = [crowd:2, crowd:2, crowd:2, crowd:2, crowd:2] RETURN NONE;
						CREATE crowd:2 SET friends = [crowd:1, crowd:1, crowd:1, crowd:1, crowd:1] RETURN NONE;",
					)
					.await
					.datastore(),
				)
			})
	})
	.join()
	.expect("set up");
	// Bounded, so a cycle whose rows run every sibling at every level fails the
	// test rather than spinning it.
	let (done_tx, done_rx) = std::sync::mpsc::channel();
	std::thread::Builder::new()
		.stack_size(STACK)
		.spawn(move || {
			let answered = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("a runtime")
				.block_on(async {
					reset_fills();
					let mut responses = ds
						.execute("SELECT id, docs FROM crowd:1", &TestDb::owner(), None)
						.await
						.expect("runs");
					(responses.remove(0).result.map(|_| ()).map_err(|e| e.to_string()), eaches())
				});
			done_tx.send(answered).ok();
		})
		.expect("a thread");
	let (answer, overlapped) = done_rx
		.recv_timeout(std::time::Duration::from_secs(60))
		.expect("the statement ends within the stack and the time");
	assert!(overlapped > 0, "the cycle never reached the overlapping fan-out");
	let error = answer.expect_err("a cycle of links ends in an error");
	assert!(error.contains("excessive computation depth"), "another error: {error}");
}
