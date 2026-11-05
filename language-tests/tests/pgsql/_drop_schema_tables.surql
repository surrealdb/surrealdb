/**
[test]
run = false
reason = "Setup only: two tables linked by a reference and a view over one of them, each with a row, for the tests that drop the PostgreSQL schema."
*/

DEFINE TABLE dsc_author SCHEMAFULL;
DEFINE FIELD name ON dsc_author TYPE string;

DEFINE TABLE dsc_post SCHEMAFULL;
DEFINE FIELD author_id ON dsc_post TYPE record<dsc_author> REFERENCE;
DEFINE FIELD title ON dsc_post TYPE string;

DEFINE TABLE dsc_titles AS SELECT id, title FROM dsc_post;

CREATE dsc_author:1 SET name = 'Ada';
CREATE dsc_post:1 SET author_id = dsc_author:1, title = 'first';
