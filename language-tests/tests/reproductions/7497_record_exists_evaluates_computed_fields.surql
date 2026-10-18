/**
[env]
timeout = 5000

[test]
reason = "record::exists and .exists() must answer from the key, without evaluating the target's COMPUTED fields"
issue = 7497

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: a:1, id: b:1 }]"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD boom ON a COMPUTED <int>"not a number";
CREATE a:1 RETURN NONE;
SELECT id FROM a;
RETURN a:1.exists();
RETURN record::exists(a:1);
RETURN (SELECT VALUE id FROM ONLY a:1) != NONE;
DEFINE TABLE b SCHEMAFULL;
DEFINE FIELD a ON b TYPE record<a> ASSERT $value.exists();
CREATE b:1 SET a = a:1;
RETURN record::exists(a:2);
RETURN record::exists(nowhere:1);
RETURN a:1.exists();
RETURN record::exists(a:{ id: 1 });
RETURN record::exists(a:{ id: NONE });
