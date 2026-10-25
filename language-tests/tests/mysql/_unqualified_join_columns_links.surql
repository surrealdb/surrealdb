/**
[test]
run = false
reason = "Setup only: a schemaless table of users and a table of posts whose owner column is declared as a link to a user, for the unqualified_join_columns_links tests of the MySQL dialect."
*/

DEFINE TABLE ujc_user SCHEMALESS;
CREATE ujc_user:1 SET name = 'Eve' RETURN NONE;
CREATE ujc_user:2 SET name = 'Sam' RETURN NONE;
CREATE ujc_user:3 SET name = 'Ann' RETURN NONE;

DEFINE TABLE ujc_post SCHEMAFULL;
DEFINE FIELD title ON ujc_post TYPE string;
DEFINE FIELD owner ON ujc_post TYPE record<ujc_user>;
CREATE ujc_post:1 SET title = 'p1', owner = ujc_user:1 RETURN NONE;
CREATE ujc_post:2 SET title = 'p2', owner = ujc_user:1 RETURN NONE;
CREATE ujc_post:3 SET title = 'p3', owner = ujc_user:2 RETURN NONE;
