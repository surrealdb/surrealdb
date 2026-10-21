/**
[env]
imports = ["reproductions/unique_index_conflict_hides_the_stored_record_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A UNIQUE-index conflict on a CREATE must not name the stored record or its
	indexed value: a caller with CREATE but not SELECT could otherwise probe for
	a value and, on a conflict, learn the hidden row's id and value
	(GHSA-cff7-q66j-4hc8). The conflict is still reported. On the unfixed code the
	error named `users:victim` and the stored value.

	The redaction must hold wherever a nested write stringifies the error before
	it reaches the caller. A top-level CREATE streams its write and is redacted at
	the stream exit. A CREATE inside a FOR loop or block is a grouped write, run
	through `run_write` without the stream exit, so it must be redacted on that
	path too. A conflict raised by a synchronous event's THEN write is folded into
	the event's failure string, and one raised by an ON DELETE reference repair is
	folded into the referential-failure string, so each must be redacted before it
	is stringified; otherwise the grouped, event and repair forms still leak the
	hidden record and value.

	Event nesting must not defeat it. A CREATE on `chain_a` fires an event that
	writes `chain_b`, whose event raises the conflict, so the error is stringified
	two event frames deep. Each event frame suppresses the read check, so the
	redaction must read the caller's recorded status rather than the innermost
	frame; otherwise the nested form still leaks the hidden record and value.

	A `DEFINE API` chain runs its handler with permissions off and returns a
	failed handler's error text as the response body. The chain carries the
	caller's own read-check status, so a conflict its write raises is redacted
	for a permission-checked caller, both directly and from an event the
	handler's write fires.
"""

[[test.results]]
error = "Database index `email_idx` already contains a conflicting value in another record"

[[test.results]]
error = "Database index `email_idx` already contains a conflicting value in another record"

[[test.results]]
error = "Error while processing event conflict: Database index `email_idx` already contains a conflicting value in another record"

[[test.results]]
error = "An error occurred while updating references for `tag:fav`: Database index `email_idx` already contains a conflicting value in another record"

[[test.results]]
error = "Error while processing event cascade_a: Error while processing event cascade_b: Database index `email_idx` already contains a conflicting value in another record"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Database index `email_idx` already contains a conflicting value in another record', headers: {}, status: 500 }"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'Error while processing event cascade_a: Error while processing event cascade_b: Database index `email_idx` already contains a conflicting value in another record', headers: {}, status: 500 }"
*/

CREATE users:probe SET email = 'secret@example.com';

FOR $i IN [1] {
	CREATE users:probe_grouped SET email = 'secret@example.com';
};

CREATE trigger:probe;

DELETE tag:fav;

CREATE chain_a:probe;

api::invoke("/register", { method: "post", body: { email: 'secret@example.com' } });

api::invoke("/chain", { method: "post" });
