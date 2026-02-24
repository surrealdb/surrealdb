/**
[test]

[[test.results]]
error = "Cannot use the `COMPUTED` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `VALUE` keyword with `COMPUTED`."

[[test.results]]
error = "Cannot use the `ASSERT` keyword with `COMPUTED`."

[[test.results]]
error = "Cannot use the `DEFAULT` keyword with `COMPUTED`."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `COMPUTED`."

[[test.results]]
error = "Cannot use the `READONLY` keyword with `COMPUTED`."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'i' - Field: 'f'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'i' - Field: 'f'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'i' - Field: 'foo'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'i' - Field: 'foo'"

*/
DEFINE FIELD id ON a COMPUTED 1;
DEFINE FIELD a ON a VALUE 1 COMPUTED 1;
DEFINE FIELD a ON a ASSERT 1 COMPUTED 1;
DEFINE FIELD a ON a DEFAULT 1 COMPUTED 1;
DEFINE FIELD a ON a REFERENCE COMPUTED 1;
DEFINE FIELD a ON a READONLY COMPUTED 1;
DEFINE INDEX i ON a FIELDS f;
DEFINE FIELD f ON a COMPUTED 1;

DEFINE FIELD f on b COMPUTED 1;
DEFINE INDEX i ON b FIELDS f;

DEFINE FIELD foo ON c COMPUTED { bar: 1 };
DEFINE INDEX i ON c FIELDS foo.bar;

DEFINE INDEX i ON d FIELDS foo.bar;
DEFINE FIELD foo ON d COMPUTED { bar: 1 };
