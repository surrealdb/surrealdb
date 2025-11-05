/**
[test]

[[test.results]]
error = "Couldn't write to a read only transaction"

[[test.results]]
value = "[]"

*/
(||{
	CREATE foo;
})();
SELECT * FROM foo;
