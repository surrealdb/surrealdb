/**
[test]

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `invalid.nested`. Specify a referencing field at the root level instead."

*/
DEFINE FIELD invalid.nested ON test TYPE record REFERENCE;