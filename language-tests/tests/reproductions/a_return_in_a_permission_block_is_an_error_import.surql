/**
[test]
reason = "Setup for a_return_in_a_permission_block_is_an_error.surql: tables whose permissions, computed fields and field values are blocks that RETURN, beside a table no record user may read."
run = false
*/
-- The record users' table exists: the permissions read `$auth.id`, and a
-- record id naming an undefined table is an error rather than `NONE`.
DEFINE TABLE user SCHEMALESS;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( type::record($rid) ) DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE private SCHEMALESS PERMISSIONS NONE;
CREATE private:1 SET s = 'SECRET';
DEFINE TABLE gated4 SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN private:1.s };
CREATE gated4:1 SET f = 1;
DEFINE TABLE gated5 SCHEMALESS PERMISSIONS FOR select WHERE { RETURN private:1.s };
CREATE gated5:1 SET f = 1;
DEFINE TABLE gated6 SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN true };
CREATE gated6:1 SET f = 1;
DEFINE TABLE gated7 SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; IF true { RETURN 'x' }; false };
CREATE gated7:1 SET f = 1;
DEFINE TABLE gated8 SCHEMALESS PERMISSIONS FOR select WHERE (SELECT VALUE { LET $u = 1; RETURN true } FROM ONLY [1] LIMIT 1);
CREATE gated8:1 SET f = 1;
DEFINE TABLE gated9 SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; FOR $i IN [1] { BREAK }; true };
CREATE gated9:1 SET f = 1;
DEFINE TABLE gatedA SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN false };
CREATE gatedA:1 SET f = 1;
DEFINE TABLE comp SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD c ON comp COMPUTED { LET $a = 1; RETURN $a + 1 };
DEFINE FIELD d ON comp COMPUTED { LET $a = 1; IF true { RETURN 'early' }; 'late' };
CREATE comp:1 SET f = 1;
DEFINE TABLE fq SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD s ON fq PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN false };
DEFINE FIELD t ON fq PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN private:1.s };
CREATE fq:1 SET s = 'S', t = 'T', f = 1;
DEFINE TABLE fv SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD v ON fv VALUE { LET $a = 2; RETURN $a * 3 };
DEFINE TABLE dv SCHEMALESS PERMISSIONS FOR select WHERE c = 2;
DEFINE FIELD c ON dv COMPUTED { LET $a = 1; RETURN $a + 1 };
CREATE dv:1 SET f = 1;
DEFINE TABLE dp SCHEMALESS PERMISSIONS FOR select WHERE c = 2;
DEFINE FIELD c ON dp COMPUTED 1 + 1;
CREATE dp:1 SET f = 1;
DEFINE TABLE wg SCHEMALESS PERMISSIONS FOR select FULL, FOR create WHERE { LET $u = $auth.id; RETURN true };
DEFINE TABLE wf SCHEMALESS PERMISSIONS FOR select FULL, FOR create WHERE { LET $u = $auth.id; RETURN false };
DEFINE TABLE wu SCHEMALESS PERMISSIONS FOR select WHERE { LET $u = $auth.id; RETURN true }, FOR update FULL;
CREATE wu:1 SET f = 1;
