/**
[test]
reason = "An UPSERT whose WHERE matches no record inserts one, and a schema error from that insert fails the statement rather than answering an empty array."
issue = 6816

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$error = /^Couldn't coerce value for field `email` of `user:.*`: Expected `string` but found `NONE`$/"
error = true

[[test.results]]
error = "Couldn't coerce value for field `email` of `user:x`: Expected `string` but found `NONE`"

[[test.results]]
match = "$error = /^Couldn't coerce value for field `email` of `user:.*`: Expected `string` but found `1`$/"
error = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ email: 'a', id: user:id }]"
skip-record-id-key = true

[[test.results]]
value = "[{ email: 'a', id: user:id }]"
skip-record-id-key = true

*/

DEFINE TABLE user SCHEMAFULL;
DEFINE FIELD email ON user TYPE string;
UPSERT user CONTENT {} WHERE id = user:id;
UPSERT user:x CONTENT {};
UPSERT user SET email = 1 WHERE email = 'nope';
SELECT * FROM user;
UPSERT user CONTENT { email: 'a' } WHERE email = 'nope';
SELECT * FROM user;
