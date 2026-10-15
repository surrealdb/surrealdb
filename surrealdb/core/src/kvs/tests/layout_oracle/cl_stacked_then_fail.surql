-- Stacked migrations decided in one transaction the caller owns: two migrations of one field and one of
-- another, a migration under a name the transaction already holds, and failing writes and definitions
-- between them. The caller commits what each statement wrote before its failure. The stacked steps
-- apply in the order they were minted, the failing statements leave no step behind, and each row reads
-- as every step applied to its stored value. A later transaction, after the job, is judged on the
-- definitions it finds, not on the ones the earlier failures would have left.
DEFINE TABLE st2 SCHEMAFULL;
DEFINE FIELD f ON st2 TYPE string;
DEFINE FIELD g ON st2 TYPE string;
CREATE st2:1 SET f = '10', g = '1' RETURN NONE;
CREATE st2:2 SET f = '20', g = '2' RETURN NONE;
CREATE st2:3 SET f = 'x', g = '3' RETURN NONE;
-- @upgrade
-- @client
ALTER FIELD f ON st2 TYPE int MIGRATE EXISTING ROWS AS f1 USING <int>$value FALLBACK 0;
ALTER FIELD g ON st2 TYPE int MIGRATE EXISTING ROWS AS g1 USING <int>$value FALLBACK 0;
ALTER FIELD f ON st2 TYPE int MIGRATE EXISTING ROWS AS f2 USING $value * 2 FALLBACK 0;
ALTER FIELD f ON st2 TYPE int MIGRATE EXISTING ROWS AS f1 USING <int>$value + 1 FALLBACK 0;
CREATE st2:4 SET f = 4, g = 4 RETURN NONE;
CREATE st2:5 SET f = 'bad', g = 5 RETURN NONE;
UPDATE st2:1 SET g = 'x' RETURN NONE;
-- @end
SELECT * FROM st2 ORDER BY id;
(INFO FOR TABLE st2).migrations;
-- @check
-- @drain
SELECT * FROM st2 ORDER BY id;
(INFO FOR TABLE st2).migrations;
-- After the job has finished, a refused float migration of the converted field and a valid one commit
-- together with a write of the new type.
-- @client
ALTER FIELD f ON st2 TYPE float MIGRATE EXISTING ROWS AS f_float USING <float>$value FALLBACK 'x';
ALTER FIELD f ON st2 TYPE float MIGRATE EXISTING ROWS AS f_float USING <float>$value / 4 FALLBACK 0.0;
CREATE st2:6 SET f = 0.5, g = 6 RETURN NONE;
CREATE st2:7 SET f = 'seven', g = 7 RETURN NONE;
-- @end
SELECT * FROM st2 ORDER BY id;
SELECT VALUE f FROM st2 ORDER BY id;
(INFO FOR TABLE st2).migrations;
-- @check
-- @drain
SELECT * FROM st2 ORDER BY id;
(INFO FOR TABLE st2).migrations;
-- @check
