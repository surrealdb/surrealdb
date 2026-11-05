/**
[env]
namespace = true
database = true

[test]
reason = "UPSERT ONLY over a table whose WHERE matches nothing writes the one record it guarantees and returns that record alone, the get-or-create idiom."

[[test.results]]
value = "1"

[[test.results]]
value = "'a'"

[[test.results]]
value = "'a'"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

*/

UPSERT ONLY u SET c = 1 WHERE false RETURN VALUE c;
UPSERT ONLY user SET email = 'a' WHERE email = 'a' RETURN VALUE email;
UPSERT ONLY user SET email = 'a' WHERE email = 'a' RETURN VALUE email;
RETURN count(SELECT * FROM user);
UPSERT ONLY v SET c = 1 WHERE false RETURN NONE;
RETURN count(SELECT * FROM v);
