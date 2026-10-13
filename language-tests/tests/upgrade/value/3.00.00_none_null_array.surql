/**
[env]
imports = ["upgrade/value/3.00.00_none_null_array_import.surql"]

[test]
upgrade = true

[[test.results]]
value = "[NONE, NULL]"

*/
SELECT VALUE data FROM ONLY test:values;
