/**
[test]
upgrade = true

[[test.results]]
value = "[{ field1: 1, field2: 'b', id: test:1 }]"

[env]
imports = ["upgrade/basic_data_import.surql"]
*/

SELECT * FROM test;
