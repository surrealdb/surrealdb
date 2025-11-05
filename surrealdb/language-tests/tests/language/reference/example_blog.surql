/**
[test]

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
value = "NONE"

[[test.results]]
value = "[{ id: tag:surrealdb }, { id: tag:multimodel }]"

[[test.results]]
value = "[{ authored: [], id: person:micha }]"

[[test.results]]
value = "[{ author: person:micha, id: post:whysurrealdb, tags: [tag:surrealdb, tag:multimodel] }]"

[[test.results]]
value = "[{ authored: [post:whysurrealdb], id: person:micha }]"

[[test.results]]
value = "[{ author: person:micha, id: post:whysurrealdb, tags: [tag:surrealdb, tag:multimodel] }]"

[[test.results]]
value = "[{ id: tag:multimodel, posts: [post:whysurrealdb] }, { id: tag:surrealdb, posts: [post:whysurrealdb] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:micha, id: post:whysurrealdb, tags: [tag:surrealdb] }]"

[[test.results]]
error = "Cannot delete `person:micha` as it is referenced by `post:whysurrealdb` with an ON DELETE REJECT clause"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:micha, id: post:whysurrealdb, tags: [] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[env.capabilities]
allow-experimental = ["record_references"]
*/

DEFINE TABLE tag;

DEFINE TABLE person;
DEFINE FIELD authored ON person COMPUTED <~post;

DEFINE TABLE post;
DEFINE FIELD author ON post TYPE record<person>
    REFERENCE ON DELETE REJECT;

DEFINE FIELD tags ON post TYPE array<record<tag>>
    REFERENCE ON DELETE UNSET;

CREATE tag:surrealdb, tag:multimodel;
CREATE person:micha;
CREATE post:whysurrealdb SET author = person:micha, tags = [tag:surrealdb, tag:multimodel];

SELECT * FROM person;
SELECT * FROM post;
SELECT *, <~post AS posts FROM tag;

DELETE tag:multimodel;
SELECT * FROM post;

DELETE person:micha;

DEFINE FIELD OVERWRITE tags ON post TYPE array<record<tag>>
    REFERENCE ON DELETE THEN (UPDATE ONLY $this SET tags -= $reference);

DELETE tag:surrealdb;
SELECT * FROM post;

DEFINE FIELD OVERWRITE author ON post TYPE record<person>
    REFERENCE ON DELETE CASCADE;

DELETE person:micha;
SELECT * FROM post;
