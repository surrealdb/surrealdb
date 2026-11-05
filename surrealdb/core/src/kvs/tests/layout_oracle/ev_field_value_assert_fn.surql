-- A field's VALUE and ASSERT may call user functions. The migration rewrites the rows it
-- reaches with its transform alone, so VALUE is not run over them; a later write runs VALUE
-- and ASSERT with the bodies current at that write, on the row as it reads (migrated when it
-- is still behind its step). A row that violates a later ASSERT is not rewritten by the job.
DEFINE FUNCTION fn::tag($v: any) { RETURN <int>$v + 1 };
DEFINE FUNCTION fn::ok($v: any) { RETURN $v >= 0 };
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD n ON tb TYPE string;
DEFINE FIELD note ON tb TYPE option<string>;
CREATE tb:1 SET n = '5';
CREATE tb:2 SET n = '-3';
-- @upgrade
DEFINE FIELD OVERWRITE n ON tb TYPE int VALUE fn::tag($value) ASSERT fn::ok($value) MIGRATE EXISTING ROWS AS m USING <int>$value FALLBACK 0;
CREATE tb:3 SET n = 7;
SELECT * FROM tb ORDER BY id;
DEFINE FUNCTION OVERWRITE fn::tag($v: any) { RETURN <int>$v * 10 };
CREATE tb:4 SET n = 2;
UPDATE tb:1 SET note = 'a';
SELECT * FROM tb ORDER BY id;
-- @drain
SELECT * FROM tb ORDER BY id;
DEFINE FUNCTION OVERWRITE fn::ok($v: any) { RETURN $v > 25 };
UPDATE tb:2 SET note = 'b';
UPDATE tb:4 SET note = 'c';
SELECT * FROM tb ORDER BY id;
-- @check
