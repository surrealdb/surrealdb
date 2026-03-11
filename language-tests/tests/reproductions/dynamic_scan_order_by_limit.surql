/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY + LIMIT returns incorrect results with DynamicScan sources (type::table, $param)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[d'2060-01-01T00:00:00Z', d'2060-01-01T00:00:01Z', d'2060-01-01T00:00:02Z', d'2060-01-01T00:00:03Z', d'2060-01-01T00:00:04Z']"

[[test.results]]
value = "[d'2060-01-01T00:00:00Z', d'2060-01-01T00:00:01Z', d'2060-01-01T00:00:02Z', d'2060-01-01T00:00:03Z', d'2060-01-01T00:00:04Z']"

[[test.results]]
value = "[d'2060-01-01T00:00:00Z', d'2060-01-01T00:00:01Z', d'2060-01-01T00:00:02Z', d'2060-01-01T00:00:03Z', d'2060-01-01T00:00:04Z']"
*/

LET $time = d"2060-01-01T00:00:00Z";

FOR $i IN 0..10 {
    CREATE scan SET createdOn = $time + duration::from_secs($i), kind = "scan";
};

LET $table = type::table("scan");

-- DynamicScan via type::table(): must return first 5 rows in createdOn order
(SELECT createdOn FROM type::table("scan") ORDER BY createdOn LIMIT 5).createdOn;

-- $param (Value::Table, resolved to Expr::Table at plan time): must return first 5 rows in createdOn order
(SELECT createdOn FROM $table ORDER BY createdOn LIMIT 5).createdOn;

-- Literal table (control): must return first 5 rows in createdOn order
(SELECT createdOn FROM scan ORDER BY createdOn LIMIT 5).createdOn;
