/**
[env]
imports = ["upgrade/value/set_import.surql"]

[test]
upgrade = true
reason = "Closes the upgrade-tests coverage gap for `Value::Set` after the rev-2 wire-format adoption — the cross-binary harness now exercises every structure whose `#[revisioned]` attribute moved from rev-1 to rev-2 in this PR (Array, Object, Set, Value). The import (run by the FROM binary, pre-rev-2) writes three Set shapes — small, single-element trailing-comma form, and 12-element large — so the rev-1 wire bytes flow through the multi-revision walker on the TO binary's rev-2-aware decoder."

[[test.results]]
value = "{1, 2, 3}"

[[test.results]]
value = "{42,}"

[[test.results]]
value = "{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}"

*/

SELECT VALUE small FROM ONLY test:set_values;
SELECT VALUE single FROM ONLY test:set_values;
SELECT VALUE big FROM ONLY test:set_values;
