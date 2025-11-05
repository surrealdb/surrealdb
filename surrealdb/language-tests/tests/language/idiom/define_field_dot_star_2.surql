
/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `emails.address` of `user:1`: Expected `string` but found `9`"

[[test.results]]
value = """
[
	{
		emails: {
			address: 'me@me.com'
		},
		id: user:2
	}
]
"""


[[test.results]]
value = """
[
	{
		emails: {
			address: 'me@me.com'
		},
		id: user:3,
		tags: [
			{
				value: 'bla'
			}
		]
	}
]
"""

*/


DEFINE FIELD emails.address ON TABLE user TYPE string;
-- Previously `emails.*` would be considered the same as `emails`
-- Resulting in two conflicting types for the same field. But now
-- `emails.*`, for objects, targets all values when `emails` is an object
-- and all entries when `emails` is an array.
DEFINE FIELD emails.*.address ON TABLE user TYPE option<number>;
DEFINE FIELD tags.*.value ON TABLE user TYPE option<string>;

CREATE user:1 SET emails.address = 9;
CREATE user:2 SET emails.address = "me@me.com";
create user:3 set tags = [{ value: 'bla' }], emails.address = "me@me.com";

