/**
[env]

[test]
reason = """A grouped SELECT whose projection reads $parent as the source of a nested \
SELECT is refused with the GROUP BY row-scope error, the same as a $parent read in any \
other position of the projection."""

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"

[[test.results]]
error = "Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"

[[test.results]]
error = "Invalid query: Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to"
*/

{
	CREATE a:1 SET v = 1, g = 1;
	CREATE a:2 SET v = 2, g = 1;
	CREATE a:3 SET v = 3, g = 2;
	CREATE a:4 SET v = 4, g = 2;
	RETURN "OK";
};

SELECT (SELECT id FROM $parent) AS p, g, math::mean(v) FROM a GROUP BY g;

SELECT (SELECT count() FROM $parent GROUP ALL) AS c, g FROM a GROUP BY g;

SELECT (SELECT id FROM $parent) AS p, count() FROM a GROUP ALL;
