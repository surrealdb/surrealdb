/**
[test]
reason = "A write an event runs that conflicts with a UNIQUE index fails the firing statement with the index error, for CREATE, UPDATE and UPSERT, and through an event fired by an event's write."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: audit:0, k: 1 }]"

[[test.results]]
value = "[{ id: audit:1, k: 2 }]"

[[test.results]]
error = "Database index `ak` already contains 1, with record `audit:0`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Database index `ak` already contains 1, with record `audit:0`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Database index `ak` already contains 1, with record `audit:0`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Database index `ak` already contains 1, with record `audit:0`"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event ev: Error while processing event forward: Database index `ak` already contains 1, with record `audit:0`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: audit:0, k: 1 }, { id: audit:1, k: 2 }]"

*/

DEFINE TABLE audit;
DEFINE INDEX ak ON audit FIELDS k UNIQUE;
CREATE audit:0 SET k = 1;
CREATE audit:1 SET k = 2;

-- The write as a statement of its own.
CREATE audit:9 SET k = 1;

-- CREATE in an event.
DEFINE TABLE created;
DEFINE EVENT ev ON created WHEN $event = "CREATE" THEN (CREATE audit:9 SET k = 1);
CREATE created:1;
SELECT * FROM created;

-- UPDATE in an event.
DEFINE TABLE updated;
DEFINE EVENT ev ON updated WHEN $event = "CREATE" THEN (UPDATE audit:1 SET k = 1);
CREATE updated:1;
SELECT * FROM updated;

-- UPSERT in an event.
DEFINE TABLE upserted;
DEFINE EVENT ev ON upserted WHEN $event = "CREATE" THEN (UPSERT audit:9 SET k = 1);
CREATE upserted:1;
SELECT * FROM upserted;

-- An event whose write fires another event, whose write conflicts.
DEFINE TABLE relay;
DEFINE EVENT forward ON relay WHEN $event = "CREATE" THEN (CREATE audit:9 SET k = 1);
DEFINE TABLE nested;
DEFINE EVENT ev ON nested WHEN $event = "CREATE" THEN (CREATE relay:1);
CREATE nested:1;
SELECT * FROM nested;
SELECT * FROM relay;

-- No write of a failed statement remains.
SELECT * FROM audit ORDER BY id;
