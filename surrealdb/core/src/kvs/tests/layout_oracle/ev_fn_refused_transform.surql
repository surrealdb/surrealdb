-- A transform may not call a user-defined function, and a FALLBACK may not be one: each
-- migration is refused when it is defined, so no row is ever read through a body that is
-- redefined later, and the migration name a refused statement gave stays free. A builtin
-- transform defined afterwards reads the rows the same way whatever fn::conv's body is.
DEFINE FUNCTION fn::conv($v: any) { RETURN <int>$v + 1 };
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD n ON tb TYPE string;
CREATE tb:1 SET n = '1';
CREATE tb:2 SET n = 'x';
CREATE tb:3 SET n = '3';
-- @upgrade
CREATE tb:4 SET n = '4';
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING fn::conv($value) FALLBACK 0;
DEFINE FIELD OVERWRITE n ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING fn::conv($value) FALLBACK 0;
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING <int>$value FALLBACK fn::conv(0);
DEFINE FUNCTION OVERWRITE fn::conv($v: any) { RETURN <int>$v * 100 };
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING <int>$value FALLBACK -1;
SELECT * FROM tb ORDER BY id;
SELECT VALUE type::is_int(n) FROM tb ORDER BY id;
DEFINE FUNCTION OVERWRITE fn::conv($v: any) { RETURN <int>$v + 1000 };
CREATE tb:5 SET n = 5;
-- @drain
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- @check
