/**
[test]
reason = """
Chain setup for event_async.surql: defines the source table, the two log tables
and the pair of events on the first walked version.

`[env] imports` is not replayed in chain mode, so this recreates the schema the
pairwise sibling's import writes. Both event bodies create a record with a
generated id in a SCHEMALESS table with no index, assertion or permission on it,
so neither body can fail: the async event never enters the retry path, whose
attempts are made out-of-band, invisible to the version that queued the event,
and whose exhaustion drops the queued entry with nothing but a log line."""
run = false
*/
DEFINE TABLE chain_event_async_src SCHEMALESS;
DEFINE TABLE chain_event_async_synclog SCHEMALESS;
DEFINE TABLE chain_event_async_asynclog SCHEMALESS;
DEFINE EVENT chain_event_async_sync_evt ON chain_event_async_src WHEN true THEN (
	CREATE chain_event_async_synclog SET src_hop = $after.hop
);
DEFINE EVENT chain_event_async_async_evt ON chain_event_async_src ASYNC RETRY 2 MAXDEPTH 3 WHEN true THEN (
	CREATE chain_event_async_asynclog SET src_hop = $after.hop
);
