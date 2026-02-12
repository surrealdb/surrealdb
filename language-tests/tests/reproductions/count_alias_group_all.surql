/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "COUNT() with AS alias and GROUP ALL should use the alias as the output field name"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ a: 3, b: 3 }]"
*/

-- Setup: create test records
CREATE product:a SET name = "A", category = "One";
CREATE product:b SET name = "B", category = "One";
CREATE product:c SET name = "C", category = "Two";

-- Test 1: COUNT() with AS alias should use the alias
SELECT count() AS c FROM product GROUP ALL;

-- Test 2: COUNT() without alias should default to "count"
SELECT count() FROM product GROUP ALL;

-- Test 3: Multiple COUNT() fields with different aliases
SELECT count() AS a, count() AS b FROM product GROUP ALL;
