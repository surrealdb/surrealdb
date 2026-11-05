/**
[test]
reason = "`*` in a field path stands for every value of an object and every element of an array, and a field defined in it needs `*` defined first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `emails.*.address`: the field `emails.*` it is nested in is not defined. Define `emails.*` first"

[[test.results]]
error = "Couldn't coerce value for field `book.*.title` of `user:1`: Expected `none | string` but found `9`"

[[test.results]]
value = "[{ book: { dune: { title: 'Dune' } }, id: user:2 }]"

[[test.results]]
value = "[{ id: user:3, tags: [{ value: 'bla' }] }]"

[[test.results]]
error = "Couldn't coerce value for field `tags.*.value` of `user:4`: Expected `none | string` but found `9`"
*/

DEFINE FIELD book ON TABLE user TYPE option<object>;
DEFINE FIELD book.* ON TABLE user TYPE object;
DEFINE FIELD book.*.title ON TABLE user TYPE option<string>;
DEFINE FIELD tags ON TABLE user TYPE option<array<object>>;
DEFINE FIELD tags.*.value ON TABLE user TYPE option<string>;
DEFINE FIELD emails ON TABLE user TYPE option<object>;
DEFINE FIELD emails.*.address ON TABLE user TYPE string;

CREATE user:1 SET book = { dune: { title: 9 } };
CREATE user:2 SET book = { dune: { title: 'Dune' } };
CREATE user:3 SET tags = [{ value: 'bla' }];
CREATE user:4 SET tags = [{ value: 9 }];
