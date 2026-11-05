/**
[test]
run = false
reason = "The tables the truncate_failure_keeps_counter and truncate_editor_keeps_rows tests remove rows from: tables as the SQL front ends define them, with a generated key column drawn three times (one whose rows an event refuses to delete), and one without a generated column"
*/

DEFINE TABLE trunc_guarded TYPE NORMAL SCHEMAFULL PERMISSIONS FULL;
DEFINE SEQUENCE `trunc_guarded.id` BATCH 1000 START 1;
DEFINE FIELD id ON trunc_guarded TYPE int DEFAULT sequence::nextval('trunc_guarded.id') PERMISSIONS FULL;
DEFINE FIELD name ON trunc_guarded TYPE none | null | string PERMISSIONS FULL;
DEFINE INDEX trunc_guarded_id_pk ON trunc_guarded FIELDS id UNIQUE;
DEFINE EVENT trunc_guarded_rows_stay ON trunc_guarded WHEN $event = 'DELETE' THEN { THROW 'rows stay' };

DEFINE TABLE trunc_editor_gen TYPE NORMAL SCHEMAFULL PERMISSIONS FULL;
DEFINE SEQUENCE `trunc_editor_gen.id` BATCH 1000 START 1;
DEFINE FIELD id ON trunc_editor_gen TYPE int DEFAULT sequence::nextval('trunc_editor_gen.id') PERMISSIONS FULL;
DEFINE FIELD name ON trunc_editor_gen TYPE none | null | string PERMISSIONS FULL;
DEFINE INDEX trunc_editor_gen_id_pk ON trunc_editor_gen FIELDS id UNIQUE;

DEFINE TABLE trunc_editor_plain TYPE NORMAL SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD id ON trunc_editor_plain TYPE int PERMISSIONS FULL;
DEFINE FIELD name ON trunc_editor_plain TYPE none | null | string PERMISSIONS FULL;
DEFINE INDEX trunc_editor_plain_id_pk ON trunc_editor_plain FIELDS id UNIQUE;

CREATE trunc_guarded:1 SET name = 'g' RETURN NONE;
CREATE trunc_guarded:2 SET name = 'g' RETURN NONE;
CREATE trunc_guarded:3 SET name = 'g' RETURN NONE;
CREATE trunc_editor_gen:1 SET name = 'e' RETURN NONE;
CREATE trunc_editor_gen:2 SET name = 'e' RETURN NONE;
CREATE trunc_editor_gen:3 SET name = 'e' RETURN NONE;
CREATE trunc_editor_plain:1 SET name = 'p' RETURN NONE;
CREATE trunc_editor_plain:2 SET name = 'p' RETURN NONE;

RETURN [sequence::nextval('trunc_guarded.id'), sequence::nextval('trunc_guarded.id'), sequence::nextval('trunc_guarded.id')];
RETURN [sequence::nextval('trunc_editor_gen.id'), sequence::nextval('trunc_editor_gen.id'), sequence::nextval('trunc_editor_gen.id')];
