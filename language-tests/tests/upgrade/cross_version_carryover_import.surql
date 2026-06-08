/**
[test]

[[test.results]]
value = "[{ id: marker:guard, written_at_version: 'from-version-wrote-this', payload: 3735928559 }]"

*/
-- This whole record is written by the FROM-version process. If the test file
-- can read it back via the TO-version process, the two processes really are
-- sharing the same on-disk store.
CREATE marker:guard SET written_at_version = 'from-version-wrote-this', payload = 3735928559;
