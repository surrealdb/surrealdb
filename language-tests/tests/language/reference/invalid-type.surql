/**
[test]

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE number`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE number | record`. Specify only a `record` type, or a type containing only records, instead."

*/
DEFINE FIELD ref ON test TYPE number REFERENCE;
DEFINE FIELD ref ON test TYPE number | record REFERENCE;