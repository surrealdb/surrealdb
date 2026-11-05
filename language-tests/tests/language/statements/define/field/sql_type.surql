/**
[test]
reason = "DEFINE FIELD ... SQL TYPE <dialect> '<text>': the clause is accepted in any position of the field clauses, keywords are case-insensitive, it is printed with the definition in INFO, and it does not change the field's kind or what a write accepts"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { m: "DEFINE FIELD m ON t TYPE decimal SQL TYPE MYSQL 'DECIMAL(10,2)' PERMISSIONS FULL", n: "DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)' PERMISSIONS FULL", s: "DEFINE FIELD s ON t TYPE string SQL TYPE DYNAMODB 'S' READONLY COMMENT 'c' PERMISSIONS FULL", sql: "DEFINE FIELD sql ON t TYPE string SQL TYPE POSTGRESQL 'varchar(20)' DEFAULT 'a' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""

[[test.results]]
value = "[{ id: t:1, m: 2.25dec, n: 1.5dec, s: 'x', sql: 'y' }]"

[[test.results]]
value = "1.5dec"

[[test.results]]
error = "Couldn't coerce value for field `n` of `t:2`: Expected `decimal` but found `'not a number'`"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)';
define field m on t sql type mysql 'DECIMAL(10,2)' type decimal;
DEFINE FIELD OVERWRITE s ON t SQL TYPE DYNAMODB 'S' TYPE string READONLY COMMENT "c";
DEFINE FIELD IF NOT EXISTS sql ON t TYPE string DEFAULT 'a' SQL TYPE POSTGRESQL 'varchar(20)';
INFO FOR TABLE t;
CREATE t:1 SET n = 1.5dec, m = 2.25dec, s = 'x', sql = 'y';
SELECT VALUE n FROM ONLY t:1;
CREATE t:2 SET n = 'not a number', m = 1.0dec, s = 'x';
DEFINE FIELD sql_type ON t TYPE int;
