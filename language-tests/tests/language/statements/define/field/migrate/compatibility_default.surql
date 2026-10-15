/**
[env]
clean = true

[test]
reason = "ALTER SYSTEM COMPATIBILITY sets the level a database created later takes when it names none, and leaves every existing database at its own; a database created implicitly takes it too, and a COMPATIBILITY clause overrides it."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, databases: { later: 'DEFINE DATABASE later COMPATIBILITY 3', pinned: 'DEFINE DATABASE pinned COMPATIBILITY 4', test: 'DEFINE DATABASE test COMPATIBILITY 4' }, users: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ accesses: {  }, databases: { later: 'DEFINE DATABASE later COMPATIBILITY 3', latest: 'DEFINE DATABASE latest COMPATIBILITY 4', pinned: 'DEFINE DATABASE pinned COMPATIBILITY 4', test: 'DEFINE DATABASE test COMPATIBILITY 4' }, users: {  } }"
*/
ALTER SYSTEM COMPATIBILITY 3;
DEFINE DATABASE later;
DEFINE DATABASE pinned COMPATIBILITY 4;
INFO FOR NS;
ALTER SYSTEM COMPATIBILITY 4;
DEFINE DATABASE latest;
DEFINE DATABASE OVERWRITE later;
INFO FOR NS;
