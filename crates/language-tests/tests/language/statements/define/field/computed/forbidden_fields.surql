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

*/
DEFINE FIELD id ON a COMPUTED 1;
DEFINE FIELD a ON a VALUE 1 COMPUTED 1;
DEFINE FIELD a ON a ASSERT 1 COMPUTED 1;
DEFINE FIELD a ON a DEFAULT 1 COMPUTED 1;
DEFINE FIELD a ON a REFERENCE COMPUTED 1;
DEFINE FIELD a ON a READONLY COMPUTED 1;