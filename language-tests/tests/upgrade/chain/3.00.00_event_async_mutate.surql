/**
[test]
reason = """
Chain mutation for event_async.surql: every walked version writes one source row
through the events an earlier version defined, firing the sync event inside the
write transaction and enqueuing the async one for out-of-band processing.

The row carries a generated id, so no hop collides with the row a previous hop
wrote, and the write itself is the only statement here: an error in a mutation
aborts the walk, and the async event's own failures never reach the writer that
queued it."""
run = false
*/
CREATE chain_event_async_src SET hop = $hop, wrote_at = $version;
