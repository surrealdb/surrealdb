/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Setup for 7127_limit_with_indexed_filter_and_traversal_residual: a record user owning one project whose memories are readable only through a graph-traversal permission"
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR SESSION 1h;
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD email ON user TYPE string;
DEFINE TABLE project SCHEMAFULL PERMISSIONS FOR select WHERE <-owns<-user CONTAINS $auth.id;
DEFINE FIELD name ON project TYPE string;
DEFINE TABLE memory SCHEMAFULL PERMISSIONS FOR select WHERE <-has_memory<-project<-owns<-user CONTAINS $auth.id;
DEFINE FIELD content ON memory TYPE string;
DEFINE FIELD archived_at ON memory TYPE option<datetime> DEFAULT NONE;
DEFINE INDEX memory_archived_idx ON memory FIELDS archived_at;
DEFINE TABLE has_memory TYPE RELATION FROM project TO memory ENFORCED PERMISSIONS FOR select WHERE in<-owns<-user CONTAINS $auth.id;
DEFINE TABLE owns TYPE RELATION FROM user TO project ENFORCED PERMISSIONS FULL;
CREATE user:u SET email = "a@b.c";
CREATE user:other SET email = "o@b.c";
CREATE project:p SET name = "P";
CREATE project:q SET name = "Q";
RELATE user:u->owns->project:p;
RELATE user:other->owns->project:q;
FOR $i IN 1..13 { LET $m = (CREATE type::record('memory', $i) SET content = "m"); RELATE project:p->has_memory->$m; };
FOR $i IN 20..23 { LET $m = (CREATE type::record('memory', $i) SET content = "a", archived_at = time::now()); RELATE project:p->has_memory->$m; };
FOR $i IN 30..60 { LET $m = (CREATE type::record('memory', $i) SET content = "x"); RELATE project:q->has_memory->$m; };
