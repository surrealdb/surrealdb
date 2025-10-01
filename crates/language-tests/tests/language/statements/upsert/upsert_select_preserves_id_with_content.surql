/**
[test]
reason = "Test UPSERT with SELECT and content modification - demonstrates current behavior where new IDs are generated (this is the bug being fixed)"
wip = true # Made wip after fix was rolled back when it was found to exasterbate an other bug.

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: document:test, name: 'original', value: 100 }]"

[[test.results]]
value = "[{ id: document:test, name: 'updated', value: 200 }]"

[[test.results]]
value = "[{ id: document:test, name: 'original', value: 100 }]"

[[test.results]]
value = "[{ id: document:test, name: 'original', value: 100 }]"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE TABLE document SCHEMAFULL;
DEFINE FIELD name ON document TYPE string;
DEFINE FIELD value ON document TYPE number;

-- Store the document in a variable
LET $doc = { id: document:test, name: 'original', value: 100 };

-- First UPSERT: using a variable with modified content (should preserve ID but currently generates new ID)
LET $newDoc = (SELECT * FROM ONLY $doc);
UPSERT document CONTENT $newDoc;

-- Second UPSERT: using inline SELECT with modified content (should also preserve ID but currently generates new ID)
UPSERT document CONTENT (SELECT * FROM ONLY $doc.extend({ name: 'updated', value: 200 }));

-- UPSERT with a CONTENT clause that uses a WHERE clause to update the document
UPSERT document CONTENT $doc WHERE id = document:test;

-- Verify the original document still exists (the bug is that new documents were created instead of updating the original)
SELECT * FROM document;

-- Count how many documents were created (should be 3: original + 2 new ones due to the bug)
SELECT count() FROM document GROUP ALL;
