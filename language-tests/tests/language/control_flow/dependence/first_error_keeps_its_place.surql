/**
[test]
reason = "A block's first failing statement is the error it reports, and the statements after it do not run"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
error = "Database record `a:1` already exists"

[[test.results]]
error = "The table 'b' does not exist"

[[test.results]]
error = "An error occurred: first"

[[test.results]]
error = "The table 'd' does not exist"

[[test.results]]
error = "An error occurred: stop"

[[test.results]]
error = "The table 'f' does not exist"

[env]
namespace = "test"
database = "test"

*/

CREATE a:1;
{ CREATE b:1 SET v = 1; CREATE a:1; THROW 'later'; };
SELECT * FROM b;
{ THROW 'first'; CREATE d:1; };
SELECT * FROM d;
FOR $i IN [1, 2] { CREATE f SET i = $i; IF $i = 2 { THROW 'stop' }; };
SELECT count() FROM f GROUP ALL;
