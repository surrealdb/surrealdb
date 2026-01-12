/**
[env]
imports = ["upgrade/value/none_null_array_import.surql"]

[test]
upgrade = true
# In 2.1.0 there was a behavior change which removed NONE from stored arrays.
importing-version = "2.1.*"

[[test.results]]
value = "[NULL]"

*/
SELECT VALUE data FROM ONLY test:values;
