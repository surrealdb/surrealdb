-- A migration of a field of a table whose id names its components, then a drain. The rows of the table
-- read the migrated values before and after the job reaches them, and the components stay readable as
-- fields of each row.
DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [city: string, time: datetime];
DEFINE FIELD temp ON reading TYPE string;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = '11' RETURN NONE;
CREATE reading:['Rome', d'2024-01-01T00:00:00Z'] SET temp = '9' RETURN NONE;
-- @upgrade
ALTER FIELD temp ON reading TYPE int MIGRATE EXISTING ROWS AS temp_x10 USING (<int>$value) * 10 FALLBACK -1;
SELECT VALUE temp FROM reading ORDER BY id;
-- @drain
SELECT VALUE temp FROM reading ORDER BY id;
SELECT VALUE city FROM reading ORDER BY id;
-- @check
