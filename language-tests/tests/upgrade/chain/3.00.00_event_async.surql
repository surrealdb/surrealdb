/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain with a pair of events —
one sync, one ASYNC RETRY 2 MAXDEPTH 3 — defined on it by the first walked
version. Every later version writes a source row through both: the sync event
runs inside that write, and the async event is persisted to the durable event
queue and drained out-of-band, by whichever binary is running when the
event-processing lease next comes free.

A pairwise hop shows only that the newer binary can still parse the definition
the older one stored. This shows the definition still *fires*: every release in
turn enqueues through an async event an older release defined, and a queue entry
one release wrote is drained, executed and logged by a later one, with nothing
lost, duplicated or replayed as the store passes from binary to binary."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_event_async_setup.surql"
mutate = "upgrade/chain/3.00.00_event_async_mutate.surql"

# Both event definitions survive every release intact, the async one keeping its
# retry and depth budget verbatim.
[[test.results]]
match = '''
    string::contains($result.events.chain_event_async_sync_evt, "DEFINE EVENT chain_event_async_sync_evt ON chain_event_async_src")
    && !string::contains($result.events.chain_event_async_sync_evt, "ASYNC")
    && string::contains($result.events.chain_event_async_async_evt, "DEFINE EVENT chain_event_async_async_evt ON chain_event_async_src ASYNC RETRY 2 MAXDEPTH 3")
'''

# One source row per version walked so far, and one sync log row for each of
# them: both writes commit in the same transaction, so both are exact.
[[test.results]]
match = '''count($result) == $hop && $result == array::distinct($result) && array::max($result) == $hop'''

[[test.results]]
match = '''count($result) == $hop && $result == array::distinct($result) && array::max($result) == $hop'''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# A floor rather than `== $hop`: an async event is queued by the write and run
# by a background pass, so the row for the hop currently running may not exist
# yet. Everything an *earlier* version queued must be there, since the wait above
# outlasts the lease that keeps a freshly started binary from draining. The rest
# is exact — never more rows than source rows, never a duplicate, never a hop
# number no source row carries.
[[test.results]]
match = '''
    count($result) >= $hop - 1
    && count($result) <= $hop
    && $result == array::distinct($result)
    && array::all($result, |$src_hop| $src_hop >= 1 && $src_hop <= $hop)
'''

*/
INFO FOR TABLE chain_event_async_src;
SELECT VALUE hop FROM chain_event_async_src ORDER BY hop;
-- The sync event runs inside the write transaction that triggers it, so its log
-- is complete the moment the source row is visible.
SELECT VALUE src_hop FROM chain_event_async_synclog ORDER BY src_hop;
-- The async event is only enqueued by that write. A binary that has just opened
-- the store cannot drain the queue until the lease held by the node that last
-- ran a pass expires, so the backlog an earlier version left behind arrives some
-- way into this hop. Wait for it in bounded steps, each its own statement: the
-- reads of a single statement all come from one snapshot and would never observe
-- the drain, however long it slept.
IF count(SELECT id FROM chain_event_async_asynclog) < (count(SELECT id FROM chain_event_async_src) - 1) { SLEEP 5s };
IF count(SELECT id FROM chain_event_async_asynclog) < (count(SELECT id FROM chain_event_async_src) - 1) { SLEEP 5s };
IF count(SELECT id FROM chain_event_async_asynclog) < (count(SELECT id FROM chain_event_async_src) - 1) { SLEEP 5s };
IF count(SELECT id FROM chain_event_async_asynclog) < (count(SELECT id FROM chain_event_async_src) - 1) { SLEEP 5s };
IF count(SELECT id FROM chain_event_async_asynclog) < (count(SELECT id FROM chain_event_async_src) - 1) { SLEEP 5s };
SELECT VALUE src_hop FROM chain_event_async_asynclog ORDER BY src_hop;
