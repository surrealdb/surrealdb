/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY on a type::field() alias must produce globally correct ordering across pages"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ createdOn: d'2026-01-04T00:00:00Z' }, { createdOn: d'2026-01-03T00:00:00Z' }]"

[[test.results]]
value = "[{ createdOn: d'2026-01-02T00:00:00Z' }, { createdOn: d'2026-01-01T00:00:00Z' }]"
*/

{
    CREATE scan:1 SET kind = "scan", createdOn = d'2026-01-01T00:00:00Z';
    CREATE scan:2 SET kind = "scan", createdOn = d'2026-01-02T00:00:00Z';
    CREATE scan:3 SET kind = "scan", createdOn = d'2026-01-03T00:00:00Z';
    CREATE scan:4 SET kind = "scan", createdOn = d'2026-01-04T00:00:00Z';
    CREATE scan:5 SET kind = "other", createdOn = d'2026-01-05T00:00:00Z';
    RETURN "OK";
};

LET $field1 = "kind";
LET $literal1 = "scan";
LET $select2 = "createdOn";
LET $table = "scan";

-- Page 1: should return the 2 newest scans (4th, 3rd)
SELECT type::field($select2) AS `createdOn`
    FROM type::table($table)
    WHERE (type::field($field1) = $literal1)
    ORDER BY `createdOn` DESC LIMIT 2 START 0;

-- Page 2: should return the 2 oldest scans (2nd, 1st)
SELECT type::field($select2) AS `createdOn`
    FROM type::table($table)
    WHERE (type::field($field1) = $literal1)
    ORDER BY `createdOn` DESC LIMIT 2 START 2;
