/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An `id[N]` definition past the end of an array key, or with a negative index, names no place in the key and checks nothing, as for any other field; one within the key is still held to its type"
issue = 5395

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: huge:[1, 'a'] }]"

[[test.results]]
error = "Couldn't coerce value for field `id[0]` of `huge:['b', 'a']`: Expected `int` but found `'b'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: negative:[1] }]"
*/

DEFINE TABLE huge SCHEMAFULL;
DEFINE FIELD id ON huge TYPE array;
DEFINE FIELD id[0] ON huge TYPE int;
DEFINE FIELD id[1000000000] ON huge TYPE string;
CREATE huge:[1, 'a'];
CREATE huge:['b', 'a'];

DEFINE TABLE negative SCHEMAFULL;
DEFINE FIELD id ON negative TYPE array;
DEFINE FIELD id[-1] ON negative TYPE string;
CREATE negative:[1];
