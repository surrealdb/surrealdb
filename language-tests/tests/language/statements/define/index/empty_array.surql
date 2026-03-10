/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ arr: [], id: indexTest:eypwfv5hxl4png4geq5p }]"
skip-record-id-key = true

[[test.results]]
value = "NONE"

*/

DEFINE TABLE indexTest;
INSERT INTO indexTest { arr: [] };
DEFINE INDEX idx_arr ON TABLE indexTest COLUMNS arr;
