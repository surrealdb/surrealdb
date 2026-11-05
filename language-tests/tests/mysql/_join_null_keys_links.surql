/**
[test]
run = false
reason = "The tables behind the join_null_keys_links tests: posts and reviews whose owner column is a link to a user and is missing on some rows"
*/

DEFINE TABLE joinnull_user SCHEMALESS;
CREATE joinnull_user:1 SET name = 'ann' RETURN NONE;
CREATE joinnull_user:2 SET name = 'bob' RETURN NONE;

DEFINE TABLE joinnull_post SCHEMAFULL;
DEFINE FIELD title ON joinnull_post TYPE string;
DEFINE FIELD owner ON joinnull_post TYPE option<record<joinnull_user>>;
CREATE joinnull_post:1 SET title = 'p1', owner = joinnull_user:1 RETURN NONE;
CREATE joinnull_post:2 SET title = 'p2', owner = joinnull_user:2 RETURN NONE;
CREATE joinnull_post:3 SET title = 'p3' RETURN NONE;
CREATE joinnull_post:4 SET title = 'p4' RETURN NONE;

DEFINE TABLE joinnull_review SCHEMAFULL;
DEFINE FIELD note ON joinnull_review TYPE string;
DEFINE FIELD owner ON joinnull_review TYPE option<record<joinnull_user>>;
CREATE joinnull_review:1 SET note = 'n1', owner = joinnull_user:1 RETURN NONE;
CREATE joinnull_review:2 SET note = 'n2' RETURN NONE;
CREATE joinnull_review:3 SET note = 'n3' RETURN NONE;
