/**
[env]
imports = ["upgrade/value/none_null_array_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "[NONE, NULL]"

*/
SELECT VALUE data FROM ONLY test:values;
