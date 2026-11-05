-- A string field narrowed to a literal union, widened to string, and wrapped in
-- option. A second field holding NULL and NONE (TYPE any stores both) is narrowed
-- to option of a literal union with a NONE fallback, and then to the bare union
-- with a value fallback. A NULL is not a literal and takes the fallback; a NONE
-- row is absent, and must not read as the NULL row or the reverse. Probes whether
-- a union widened to its base type is accepted, since it is a widening.
DEFINE TABLE lit SCHEMAFULL;
DEFINE FIELD k ON lit TYPE string;
DEFINE FIELD o ON lit TYPE any;
CREATE lit:1 SET k = 'a', o = 'a';
CREATE lit:2 SET k = 'b', o = NULL;
CREATE lit:3 SET k = 'c', o = 'c';
CREATE lit:4 SET k = '', o = NONE;
CREATE lit:5 SET k = 'a', o = 'z';
CREATE lit:6 SET k = 'A', o = 1;
SELECT VALUE k FROM lit ORDER BY id;
SELECT VALUE o FROM lit ORDER BY id;
-- @upgrade
ALTER FIELD k ON lit TYPE 'a' | 'b' MIGRATE EXISTING ROWS AS k_lit USING $value;
ALTER FIELD k ON lit TYPE 'a' | 'b' MIGRATE EXISTING ROWS AS k_lit USING $value FALLBACK 'a';
SELECT VALUE k FROM lit ORDER BY id;
-- @drain
(INFO FOR TABLE lit).migrations.guaranteed;
DEFINE INDEX by_k ON lit FIELDS k;
SELECT VALUE k FROM lit WHERE k = 'a' ORDER BY id;
SELECT VALUE k FROM lit WHERE k > 'a' ORDER BY id;
-- @check
-- The index is removed before the union is widened.
REMOVE INDEX by_k ON lit;
ALTER FIELD k ON lit TYPE string;
ALTER FIELD k ON lit TYPE string MIGRATE EXISTING ROWS AS k_str USING $value FALLBACK 'x';
ALTER FIELD k ON lit TYPE option<'a' | 'b'>;
CREATE lit:7 SET k = 'b', o = 'q';
SELECT VALUE k FROM lit ORDER BY id;
-- @check
-- The NULL and NONE field narrowed to an option of a literal union. The NULL row
-- takes the NONE fallback; the NONE row stays absent under the fallback NONE.
ALTER FIELD o ON lit TYPE option<'x' | 'y'> MIGRATE EXISTING ROWS AS o_opt USING $value;
ALTER FIELD o ON lit TYPE option<'x' | 'y'> MIGRATE EXISTING ROWS AS o_opt USING $value FALLBACK NONE;
SELECT VALUE o FROM lit ORDER BY id;
SELECT VALUE o FROM ONLY lit:2;
SELECT VALUE o FROM ONLY lit:4;
-- @drain
SELECT VALUE o FROM lit ORDER BY id;
(INFO FOR TABLE lit).migrations.generation;
-- Removing the option: a NONE row needs a value of the union to pass.
ALTER FIELD o ON lit TYPE 'x' | 'y' MIGRATE EXISTING ROWS AS o_req USING $value FALLBACK 'x';
SELECT VALUE o FROM lit ORDER BY id;
SELECT VALUE o FROM ONLY lit:4;
(INFO FOR TABLE lit).migrations.generation;
-- @check
-- Adding null to the union of the field is a widening; rows may now hold null.
ALTER FIELD o ON lit TYPE 'x' | 'y' | null;
CREATE lit:8 SET k = 'a', o = null;
CREATE lit:9 SET k = 'a', o = 'w';
SELECT VALUE o FROM lit ORDER BY id;
-- @drain
SELECT VALUE o FROM lit ORDER BY id;
-- @check
