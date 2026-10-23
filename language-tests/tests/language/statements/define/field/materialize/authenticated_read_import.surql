/**
[test]
reason = "Setup for authenticated_read.surql: a record user who may select and create posts of their own and read authors, one of whom the user may not read, and a post the user may not read among the ones an author lists."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE author SCHEMAFULL
	PERMISSIONS FOR select WHERE visible = true, FOR create, update, delete NONE;
DEFINE FIELD visible ON author TYPE bool DEFAULT true;
DEFINE FIELD posts ON author TYPE array<record<post>> DEFAULT [] READONLY;
DEFINE FIELD secret_posts ON author TYPE array<record<post>> DEFAULT [] READONLY
	PERMISSIONS FOR select NONE;
DEFINE TABLE post SCHEMAFULL
	PERMISSIONS FOR select WHERE owner = $auth.id, FOR create, update, delete WHERE owner = $auth.id;
DEFINE FIELD owner ON post TYPE record<person>;
DEFINE FIELD author ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.posts;
DEFINE FIELD hidden ON post TYPE record<author> REFERENCE MATERIALIZE INTO author.secret_posts;

CREATE person:bob, person:eve;
CREATE author:open SET visible = true;
CREATE author:closed SET visible = false;
CREATE post:1 SET owner = person:bob, author = author:open, hidden = author:open;
CREATE post:2 SET owner = person:eve, author = author:open, hidden = author:open;
CREATE post:3 SET owner = person:eve, author = author:closed, hidden = author:closed;
