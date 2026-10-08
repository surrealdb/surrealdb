/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A plain `DEFINE TABLE … AS SELECT` (no GROUP) wrote its row with `tx.set_record` and stopped there, running none of the view table's own lifecycle: no events, no live queries, no change feed, no second-order views and no index maintenance. Its own `DEFINE TABLE` backfill did call `run_triggers`, so a view table was maintained once at definition and never again. The grouped case was fixed earlier (see codex_e72dc843); this pins the same contract for the ungrouped one."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 1, id: target:1 }]"

# A view row's stored `data` carries no `id` — it is spliced back in from the
# storage key on read — so `$after` is the projection alone, exactly as it is
# for a grouped view's events.
[[test.results]]
value = "[{ ev: 'CREATE', id: log:seen, seen: { field: 1 } }]"

[[test.results]]
match = "array::any($result, |$entry| array::any($entry.changes, |$c| $c.update.field == 1))"

*/

DEFINE TABLE target SCHEMALESS;
DEFINE TABLE mirror CHANGEFEED 1h AS SELECT field FROM target;

-- An event on the view table. The view row is a record on a real table, so
-- a write that maintains it is a write the table's events observe.
DEFINE EVENT saw ON mirror THEN {
	CREATE log:seen SET ev = $event, seen = $after;
};

CREATE target:1 SET field = 1;

SELECT * FROM log;

-- And the change feed sees the same write.
SHOW CHANGES FOR TABLE mirror SINCE 1;
