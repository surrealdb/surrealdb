/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	`tags.*` over an `array<string>` field yields the array, so a cast of it
	to `string` renders the array as a string rather than being elided as a
	cast to the value's own kind, a cast to `int` fails, and a function
	declared over `array<string>` accepts it unchanged. Both engines agree
	on every strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, nums: [1, 2], tags: ['a', 'b'] }]"

[[test.results]]
value = """[{ s: "['a', 'b']" }]"""

[[test.results]]
error = "Could not cast into `int` using input `[1, 2]`"

[[test.results]]
value = "[{ f: 'a' }]"

[[test.results]]
value = "[{ a: ['a', 'b'] }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD tags ON t TYPE array<string>;
DEFINE FIELD nums ON t TYPE array<int>;
DEFINE FUNCTION fn::first($xs: array<string>) { RETURN $xs[0]; };
CREATE t:1 SET tags = ['a', 'b'], nums = [1, 2];
SELECT <string> tags.* AS s FROM t;
SELECT <int> nums.* AS n FROM t;
SELECT fn::first(tags.*) AS f FROM t;
SELECT <array<string>> tags.* AS a FROM t;
