/**
[env]
imports = ["upgrade/value/none_null_array_import.surql"]

[test]
upgrade = true
# After 2.2.1 their was a behavior change which allowed NONE to be stored again.
importing-version = ">=2.2.1"

[[test.results]]
value = "[NONE, NULL]"

*/
SELECT VALUE data FROM ONLY test:values;
