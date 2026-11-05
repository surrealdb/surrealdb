/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Filtering and mapping an array of record ids with lambdas reads no record, and returns what the WHERE filter over the same array returns; SELECT over the result reads only the selected records."
issue = 4645

[[test.results]]
value = "[{ id: space:1, users: [user:1, user:2, user:3, user:4, user:5, user:6, user:7, user:8, user:9, user:10, user:11, user:12, user:13, user:14, user:15, user:16, user:17, user:18, user:19] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[user:3, user:19]"

[[test.results]]
value = "[[user:3, user:19]]"

[[test.results]]
value = "[[user:3, user:19]]"

[[test.results]]
value = "[{ id: user:3 }, { id: user:19 }]"

[[test.results]]
value = "[{ id: user:3 }, { id: user:19 }]"
*/

CREATE space:1 SET users = (CREATE |user:1..20| RETURN VALUE id);
LET $s = SELECT *, users FROM space:1;
LET $a = ($s.users.filter(|$f| record::id($f) IN [3, 19]).map(|$r| type::record(record::table($r), $r)))[0];
RETURN $a;
RETURN $s.users[WHERE $this.id() IN [3, 19]];
RETURN $s.users.filter(|$f| $f.id() IN [3, 19]);
SELECT * FROM $a;
SELECT * FROM (($s.users.filter(|$f| record::id($f) IN [3, 19]).map(|$r| type::record(record::table($r), $r)))[0]);
