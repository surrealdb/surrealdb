/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Converting the members of a literal kind keeps the rules around it: a member kind that admits a value but whose conversion refuses it (a `set<int, 2>` given `[1, 1]`) makes the literal not match, so a union falls through to its next arm instead of panicking; a cast converts members by the cast rules, so `<[int]> ['1']` is `[1]` as `<array<int>> ['1']` is; and a record id key admitted by a literal arm of a union stays as written.
"""
issue = 6697

[[test.results]]
error = "Tried to set `$a`, but couldn't coerce value: Expected `[set<int, 2> | string]` but found `[[1, 1]]`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[u:['x', 1f], true]]"

*/
LET $a: [set<int, 2> | string] = [[1, 1]];
$a;
RETURN <[int]> ['1'];
RETURN <array<int>> ['1'];
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD id ON u TYPE [string, int] | string;
CREATE u:['x', 1.0] RETURN VALUE [id, type::is_float(id[1])];
