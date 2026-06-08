/**
[env]
imports = ["upgrade/cross_version_carryover_import.surql"]

[test]
upgrade = true
reason = "Canary: confirms the upgrade harness genuinely shares state between FROM and TO versions. The import (run by the FROM binary) creates marker:guard with a known payload; the test (run by the TO binary against the same data dir) must read it back. If the runner ever regresses to running both phases against fresh stores, this test fails with 'table marker does not exist' — verified empirically."

[[test.results]]
value = "[{ id: marker:guard, payload: 3735928559, written_at_version: 'from-version-wrote-this' }]"

*/
SELECT * FROM marker;
