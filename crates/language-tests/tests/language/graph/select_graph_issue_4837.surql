/**
[test]
issue = 4837
wip = true

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[flavour:Strawberries, flavour:Chocolate, flavour:Vanilla]"

[[test.results]]
value = "[flavour:Strawberries, flavour:Chocolate, flavour:Vanilla]"

[[test.results]]
value = "[flavour:Strawberries, flavour:Chocolate, flavour:Vanilla]"

*/

BEGIN;
-- TABLE: child

DEFINE TABLE child TYPE ANY SCHEMALESS PERMISSIONS NONE;

-- TABLE: parent

DEFINE TABLE parent TYPE ANY SCHEMALESS PERMISSIONS NONE;

-- RELATE: has_parent

DEFINE TABLE has_parent TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD in ON has_parent TYPE record<child> PERMISSIONS FULL;
DEFINE FIELD out ON has_parent TYPE record<parent> PERMISSIONS FULL;

-- RELATE: loves

DEFINE TABLE loves TYPE ANY SCHEMALESS PERMISSIONS NONE;

DEFINE FIELD in ON loves TYPE record<child> PERMISSIONS FULL;
DEFINE FIELD out ON loves TYPE record<flavour> PERMISSIONS FULL;

-- TABLE: flavour

DEFINE TABLE flavour TYPE ANY SCHEMALESS PERMISSIONS NONE;

-- Inserting records

CREATE parent:John;

CREATE child:Mike;
CREATE child:Jessica;

CREATE flavour:Chocolate;
CREATE flavour:Strawberries;
CREATE flavour:Vanilla;

RELATE child:Mike->has_parent->parent:John;
RELATE child:Jessica->has_parent->parent:John;

RELATE child:Mike->loves->flavour:Chocolate;
RELATE child:Mike->loves->flavour:Strawberries;

RELATE child:Jessica->loves->flavour:Vanilla;

RETURN "OK";
COMMIT;

--- Test queries

LET $parent = parent:John;

#New output format
SELECT VALUE out.id FROM $parent<-has_parent.in->loves;

#New output format
SELECT VALUE out.id FROM (<record> parent:John)<-has_parent.in->loves;

#Same output format as in 1.x
SELECT VALUE out.id FROM parent:John<-has_parent.in->loves;
