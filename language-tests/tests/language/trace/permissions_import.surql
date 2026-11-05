/**
[test]
reason = "Setup for the trace permission tests: a graph a record user reads only part of. pal:h is hidden, blocks cannot be read, the out field of follows is hidden, the owner field of secret is hidden, and drafts cannot be read."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid ) DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE pal SCHEMALESS PERMISSIONS FOR select WHERE hidden != true;
DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE TABLE blocks TYPE RELATION SCHEMALESS PERMISSIONS FOR select NONE;
DEFINE TABLE follows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD OVERWRITE out ON follows TYPE record<pal> PERMISSIONS FOR select NONE;
DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON secret PERMISSIONS FOR select NONE;
DEFINE TABLE note SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD author ON note TYPE option<record<pal>> REFERENCE;
DEFINE TABLE draft SCHEMALESS PERMISSIONS FOR select NONE;
DEFINE FIELD author ON draft TYPE option<record<pal>> REFERENCE;

CREATE pal:a SET name = 'Ann';
CREATE pal:b SET name = 'Bob';
CREATE pal:h SET name = 'Hidden', hidden = true;
RELATE pal:a->knows:ab->pal:b;
RELATE pal:a->knows:ah->pal:h;
RELATE pal:a->blocks:ab->pal:b;
RELATE pal:a->follows:ab->pal:b;
RELATE pal:a->follows:ah->pal:h;
CREATE secret:1 SET owner = pal:a;
CREATE note:1 SET author = pal:a, friend = pal:h;
CREATE note:2 SET author = pal:h;
CREATE draft:1 SET author = pal:a;
