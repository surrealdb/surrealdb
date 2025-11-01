/**
[test]

[[test.results]]
error = "Couldn't write to a read only transaction"

[[test.results]]
error = "The table 'foo' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
(||{
	CREATE foo;
})();
SELECT * FROM foo;


DEFINE TABLE foo;
SELECT * FROM foo;
