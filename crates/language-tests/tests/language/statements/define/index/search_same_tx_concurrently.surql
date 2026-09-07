/**
[env]
# Extra timeout because this test sleeps to wait for the background build.
timeout = 4000

[test]
issue = 7234
reason = "DEFINE ANALYZER and a SEARCH index in the same BEGIN/COMMIT transaction must not leave the index in an `error` state because the background build cannot see the not-yet-committed analyzer. Variant: CONCURRENTLY (no DEFER)."

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = """
$result.building.status == 'ready'
"""
*/

CREATE |accounts:5| CONTENT { firstName: rand::string(5), lastName: rand::string(5), emailAddress: rand::string(5) } RETURN NONE;

BEGIN;
DEFINE ANALYZER accounts_adminSearch TOKENIZERS blank FILTERS lowercase, ascii, edgengram(1, 50);
DEFINE INDEX _adminSearch ON accounts FIELDS firstName, lastName, emailAddress SEARCH ANALYZER accounts_adminSearch BM25(1.2, 0.75) CONCURRENTLY;
COMMIT;

SLEEP 1s;
INFO FOR INDEX _adminSearch ON accounts;
