/**
[test]

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
error = "Cannot use the `range` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `function` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `file` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `geometry` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `none` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `null` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `bool` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `bytes` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `datetime` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `decimal` type on the `id` field, as that's not a valid record id key."

[[test.results]]
error = "Cannot use the `duration` type on the `id` field, as that's not a valid record id key."

*/

// Valid
// Nested inside arrays and objects, any type is allowed
DEFINE FIELD OVERWRITE id ON a;
DEFINE FIELD OVERWRITE id ON a TYPE any;
DEFINE FIELD OVERWRITE id ON a TYPE number;
DEFINE FIELD OVERWRITE id ON a TYPE int;
DEFINE FIELD OVERWRITE id ON a TYPE string;
DEFINE FIELD OVERWRITE id ON a TYPE uuid;
DEFINE FIELD OVERWRITE id ON a TYPE array;
DEFINE FIELD OVERWRITE id ON a TYPE array<int>;
DEFINE FIELD OVERWRITE id ON a TYPE array<datetime>;
DEFINE FIELD OVERWRITE id ON a TYPE set;
DEFINE FIELD OVERWRITE id ON a TYPE set<int>;
DEFINE FIELD OVERWRITE id ON a TYPE set<datetime>;
DEFINE FIELD OVERWRITE id ON a TYPE object;

DEFINE FIELD OVERWRITE id ON a TYPE 123;
DEFINE FIELD OVERWRITE id ON a TYPE 'string';
DEFINE FIELD OVERWRITE id ON a TYPE [uuid, 'string', datetime];
DEFINE FIELD OVERWRITE id ON a TYPE { uuid: 'string', datetime: datetime };

DEFINE FIELD OVERWRITE id ON a TYPE string | int;

// Invalid
DEFINE FIELD OVERWRITE id ON a TYPE range;
DEFINE FIELD OVERWRITE id ON a TYPE function;
DEFINE FIELD OVERWRITE id ON a TYPE file;
DEFINE FIELD OVERWRITE id ON a TYPE geometry;
DEFINE FIELD OVERWRITE id ON a TYPE none;
DEFINE FIELD OVERWRITE id ON a TYPE null;
DEFINE FIELD OVERWRITE id ON a TYPE bool;
DEFINE FIELD OVERWRITE id ON a TYPE bytes;
DEFINE FIELD OVERWRITE id ON a TYPE datetime;
DEFINE FIELD OVERWRITE id ON a TYPE decimal;
DEFINE FIELD OVERWRITE id ON a TYPE duration;