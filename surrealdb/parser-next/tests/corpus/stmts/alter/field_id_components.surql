ALTER FIELD id ON reading TYPE [city: string, time: datetime];
ALTER FIELD IF EXISTS id ON reading TYPE [city: string, time: datetime,];
ALTER FIELD id ON reading TYPE [string, datetime];
ALTER FIELD id ON reading DROP TYPE;
ALTER FIELD id ON reading TYPE [a: string] TYPE [string];
ALTER FIELD id ON reading TYPE [a: string] DROP TYPE;
ALTER FIELD id ON reading DROP TYPE TYPE [a: int, b: datetime];
ALTER FIELD id ON reading TYPE [`my city`: string, time: datetime] COMMENT "c";
ALTER FIELD value ON reading TYPE string;
