/**
[env]
imports = ["upgrade/define/3.00.00_table_view_aggregate_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
An aggregated view's rows keep the keys the binary that wrote them gave them
(`org_totals:['a']`), and a key range of the current binary has to find them
there. A range that found nothing would read the same as a view with no matching
group, and a scan in its place returns the same rows, so the read assertions in
3.00.00_table_view_aggregate.surql cannot tell a range that reads the older
writer's keys from one that does not. This asserts the plan, and then writes, so
the current binary maintains the rows the older one wrote and adds rows of its
own beside them, and one range reads both.

It lives here rather than in the sibling because `EXPLAIN` output is not stable
across the 3.x line. Every assertion holds whichever binary wrote the import,
because the ordinary language-test runner replays this file too and then the
import is written by this checkout. Only the group column and the count are read:
the type of the sum is the older writer's.
"""

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: org_totals")'''
error = false

[[test.results]]
value = "[{ n: 2, org: 'a' }]"

[[test.results]]
value = "[{ n: 2, org: 'a' }]"

[[test.results]]
value = "[{ n: 2, org: 'a' }, { n: 2, org: 'b' }]"

[[test.results]]
value = "[{ amount: 100, id: txn:5, org: 'a' }]"

[[test.results]]
value = "[{ n: 3, org: 'a' }]"

[[test.results]]
value = "[{ n: 3, org: 'a' }]"

[[test.results]]
value = "[{ amount: 1, id: txn:6, org: 'c' }]"

[[test.results]]
match = '''string::contains($result, "KeyRangeScan") AND string::contains($result, "table: org_totals")'''
error = false

[[test.results]]
value = "[{ n: 2, org: 'b' }, { n: 1, org: 'c' }]"

[[test.results]]
value = "[{ n: 2, org: 'b' }, { n: 1, org: 'c' }]"

*/
-- The view's rows were written by the binary that ran the import, under keys of
-- the form `org_totals:['a']`; a range of this binary's reads them.
EXPLAIN SELECT org, n FROM org_totals WHERE org = 'a';
SELECT org, n FROM org_totals WHERE org = 'a';
SELECT org, n FROM org_totals WITH NOINDEX WHERE org = 'a';
SELECT org, n FROM org_totals WHERE org IN ['a', 'b'] ORDER BY org;

-- A write of this binary maintains a row of the older writer, and a range reads it.
CREATE txn:5 SET org = 'a', amount = 100;
SELECT org, n FROM org_totals WHERE org = 'a';
SELECT org, n FROM org_totals WITH NOINDEX WHERE org = 'a';

-- A group this binary creates sits among the older writer's, and one range reads both.
CREATE txn:6 SET org = 'c', amount = 1;
EXPLAIN SELECT org, n FROM org_totals WHERE org > 'a' ORDER BY org;
SELECT org, n FROM org_totals WHERE org > 'a' ORDER BY org;
SELECT org, n FROM org_totals WITH NOINDEX WHERE org > 'a' ORDER BY org;
