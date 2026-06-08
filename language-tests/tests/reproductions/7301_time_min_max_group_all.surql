/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "time::min / time::max with GROUP ALL returned the chrono MAX_UTC / MIN_UTC sentinels (a real far-future / far-past datetime) for an empty group, instead of NONE. Aggregators continue to silently skip per-row values that don't match the expected scalar type (arrays, NONE, etc.) — that behaviour matches the math::* aggregators and is intentional — but the empty-group sentinel now matches the scalar `time::min`/`time::max` functions, which return NONE. Per-row scalar Datetime aggregation (e.g. `time::min(time_field)`) is exercised by `aggregate_new_executor.surql`. The legacy compute-only path errors on non-Datetime values, so this test is scoped to the streaming executor."
issue = 7301

[[test.results]]
value = "NONE"

[[test.results]]
value = "d'2024-06-15T08:00:00Z'"

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

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[d'2024-01-01T00:00:00Z']"

[[test.results]]
value = "[d'2024-12-31T23:59:59Z']"

*/

-- Per-row argument is an array of datetimes. The non-grouped scalar call
-- works (the scalar `time::min` accepts an array). Under GROUP ALL the
-- aggregator treats the per-row value as a scalar Datetime, so the array
-- is skipped; with no values accumulated, finalize must return NONE
-- (previously returned the MAX_UTC sentinel — bug #7301).
LET $single = { a: [d'2024-06-15T08:00:00Z'] };

SELECT VALUE time::min(a) AS a FROM ONLY $single;
SELECT VALUE time::min(a) AS a FROM ONLY $single GROUP ALL;
SELECT VALUE time::max(a) AS a FROM ONLY $single GROUP ALL;

-- Empty source with GROUP ALL: same NONE expectation, no MAX_UTC sentinel.
LET $empty = { a: [] };

SELECT VALUE time::min(a) AS a FROM ONLY $empty GROUP ALL;
SELECT VALUE time::max(a) AS a FROM ONLY $empty GROUP ALL;

-- Multi-row table where every row's argument is an array (so all rows are
-- skipped by the aggregator). The result must be a single group with NONE,
-- not the MAX_UTC sentinel.
{
    CREATE event7301:1 SET a = [d'2024-06-15T08:00:00Z'];
    CREATE event7301:2 SET a = [d'2024-01-01T00:00:00Z'];
    CREATE event7301:3 SET a = [d'2024-12-31T23:59:59Z'];
    RETURN "OK";
};

SELECT VALUE time::min(a) FROM event7301 GROUP ALL;
SELECT VALUE time::max(a) FROM event7301 GROUP ALL;

-- Mixed-validity rows: the silent-skip semantics we deliberately kept must
-- ignore non-Datetime per-row values (arrays, NONE) while still aggregating
-- the rows that do have a scalar Datetime. A future change to error or to
-- flatten arrays would break this contract — that's the whole point of the
-- test.
{
    CREATE event7301mixed:1 SET t = d'2024-06-15T08:00:00Z';
    CREATE event7301mixed:2 SET t = [d'2099-01-01T00:00:00Z'];   -- array, skipped
    CREATE event7301mixed:3 SET t = d'2024-01-01T00:00:00Z';
    CREATE event7301mixed:4 SET t = NONE;                         -- skipped
    CREATE event7301mixed:5 SET t = d'2024-12-31T23:59:59Z';
    RETURN "OK";
};

SELECT VALUE time::min(t) FROM event7301mixed GROUP ALL;
SELECT VALUE time::max(t) FROM event7301mixed GROUP ALL;
