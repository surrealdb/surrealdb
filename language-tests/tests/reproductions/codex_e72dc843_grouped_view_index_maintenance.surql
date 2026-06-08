/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true

[test]
reason = "Grouped DEFINE TABLE … AS SELECT … GROUP BY … writes view records via `tx.set_record` rather than the regular document pipeline. Codex finding e72dc843 reports that changefeed entries (and live-query notifications) are not emitted for the view table because the view trigger only ran a subset of the document lifecycle. After the fix `run_triggers` calls `process_table_lives` and `process_changefeeds`, so `SHOW CHANGES` for the view table includes the aggregated update."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dept: 'Engineering', id: employee:e1 }]"

[[test.results]]
value = "[{ dept: 'Engineering', id: employee:e2 }]"

[[test.results]]
value = "[{ dept: 'Sales', id: employee:s1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dept: 'Sales', id: employee:e1 }]"

[[test.results]]
value = "[{ count: 1, dept: 'Engineering', id: department:['Engineering'] }, { count: 2, dept: 'Sales', id: department:['Sales'] }]"

[[test.results]]
match = "array::any($result, |$entry| array::any($entry.changes, |$c| $c.update.count == 2))"

*/

DEFINE TABLE employee SCHEMAFULL CHANGEFEED 1h;
DEFINE FIELD dept ON employee TYPE string;
CREATE employee:e1 SET dept = 'Engineering';
CREATE employee:e2 SET dept = 'Engineering';
CREATE employee:s1 SET dept = 'Sales';
DEFINE TABLE department TYPE NORMAL CHANGEFEED 1h AS
	SELECT dept, count() AS count
	FROM employee
	GROUP BY dept;

-- Move one engineer to Sales. The grouped view triggers an aggregated
-- update on both `department:['Engineering']` and `department:['Sales']`.
UPDATE employee:e1 SET dept = 'Sales';

-- Sanity: the view rows reflect the move.
SELECT * FROM department ORDER BY dept;

-- The bug: without the fix, the view trigger skipped `process_changefeeds`,
-- so SHOW CHANGES against the view table emitted no entries even though
-- the materialised records changed. Asking for changes since versionstamp
-- 0 returns every committed change on that table.
SHOW CHANGES FOR TABLE department SINCE 1;
