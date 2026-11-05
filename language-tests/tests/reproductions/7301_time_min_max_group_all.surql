/**
[env]

[test]
reason = """
`time::min` / `time::max` with `GROUP ALL` returned the chrono MAX_UTC /
MIN_UTC sentinels — real far-future / far-past datetimes — for a group that
accumulated nothing, instead of NONE. The sentinel is indistinguishable from
a stored datetime, which is what made it a bug rather than a cosmetic one.

An aggregate folds one value per row, so this test also fixes what happens to
a row whose value is not of the kind being folded. Three cases, and the
distinction between them is whether the group has an answer at all:

- `NONE` is absent rather than wrong and is always skipped. A group of only absent values folded
  nothing, and reports NONE — the original bug, guarded here over both a single object and a
  table so the sentinel cannot come back.
- A wrong-kind value is skipped when the group folded at least one usable value, so one malformed
  row does not discard the rest. The mixed group below asserts that, ignoring both an array and a
  `NONE` while still reporting the min and max of the three real datetimes.
- A wrong-kind value **raises** when the group folded nothing, because that is exactly when a
  sentinel or an identity would be returned as though it were an answer. The error is the same one
  the scalar form raises for that argument.

The last case is why an all-array group is an error here and not NONE: the
query is ill-typed for an aggregate, and saying so beats inventing a value.
Note the scalar `time::min` does accept an array — asserted below — because it
receives the whole array as its argument, where the aggregate receives one
element per row.
"""
issue = 7301

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
value = "NONE"

[[test.results]]
value = "d'2024-06-15T08:00:00Z'"

[[test.results]]
error = "Incorrect arguments for function time::min(). Argument 1 was the wrong type. Expected `datetime` but found `[d'2024-06-15T08:00:00Z']`"

[[test.results]]
error = "Incorrect arguments for function time::max(). Argument 1 was the wrong type. Expected `datetime` but found `[d'2024-06-15T08:00:00Z']`"

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Incorrect arguments for function time::min(). Argument 1 was the wrong type. Expected `datetime` but found `[d'2024-06-15T08:00:00Z']`"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[d'2024-01-01T00:00:00Z']"

[[test.results]]
value = "[d'2024-12-31T23:59:59Z']"
*/

-- Genuinely empty groups: nothing to fold and nothing rejected. These are the
-- original bug — the result must be NONE, never a MAX_UTC / MIN_UTC sentinel.
LET $none = { a: NONE };
SELECT VALUE time::min(a) AS a FROM ONLY $none GROUP ALL;
SELECT VALUE time::max(a) AS a FROM ONLY $none GROUP ALL;

{
    CREATE event7301none:1 SET t = NONE;
    CREATE event7301none:2 SET t = NONE;
    RETURN "OK";
};
SELECT VALUE time::min(t) FROM event7301none GROUP ALL;
SELECT VALUE time::max(t) FROM event7301none GROUP ALL;

-- The scalar form takes the whole array as its argument, so it still answers.
LET $single = { a: [d'2024-06-15T08:00:00Z'] };
SELECT VALUE time::min(a) AS a FROM ONLY $single;

-- As an aggregate the per-row value is the array itself, which is not a
-- datetime. Nothing else folded, so this raises rather than reporting NONE.
SELECT VALUE time::min(a) AS a FROM ONLY $single GROUP ALL;
SELECT VALUE time::max(a) AS a FROM ONLY $single GROUP ALL;

{
    CREATE event7301:1 SET a = [d'2024-06-15T08:00:00Z'];
    CREATE event7301:2 SET a = [d'2024-01-01T00:00:00Z'];
    RETURN "OK";
};
SELECT VALUE time::min(a) FROM event7301 GROUP ALL;

-- Mixed rows: the array and the NONE are skipped because three usable
-- datetimes folded, so the group still reports a real min and max.
{
    CREATE event7301mixed:1 SET t = d'2024-06-15T08:00:00Z';
    CREATE event7301mixed:2 SET t = [d'2099-01-01T00:00:00Z'];
    CREATE event7301mixed:3 SET t = d'2024-01-01T00:00:00Z';
    CREATE event7301mixed:4 SET t = NONE;
    CREATE event7301mixed:5 SET t = d'2024-12-31T23:59:59Z';
    RETURN "OK";
};
SELECT VALUE time::min(t) FROM event7301mixed GROUP ALL;
SELECT VALUE time::max(t) FROM event7301mixed GROUP ALL;
