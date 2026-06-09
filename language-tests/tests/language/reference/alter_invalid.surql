/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE number`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `nested.ref`. Specify a referencing field at the root level instead."

*/
DEFINE FIELD ref ON parent TYPE number;
ALTER FIELD ref ON parent REFERENCE;
DEFINE FIELD nested.ref ON parent TYPE record;
ALTER FIELD nested.ref ON parent REFERENCE;
