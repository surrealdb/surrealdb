/**
[test]
reason = "Test ORDER BY multiple columns with NUMERIC modifier"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, version: 'v1.0', build: 'build2' }, { id: item:2, version: 'v1.0', build: 'build10' }, { id: item:3, version: 'v2.0', build: 'build1' }, { id: item:4, version: 'v10.0', build: 'build5' }]"
*/

-- Setup: Create records with version strings containing numbers
{
	CREATE item:1 SET version = "v1.0", build = "build2";
	CREATE item:2 SET version = "v1.0", build = "build10";
	CREATE item:3 SET version = "v2.0", build = "build1";
	CREATE item:4 SET version = "v10.0", build = "build5";
	RETURN "OK";
};

-- Test: Order by version and build with NUMERIC for natural sorting
SELECT * FROM item ORDER BY version NUMERIC ASC, build NUMERIC ASC;
