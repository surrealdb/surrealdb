DEFINE FIELD f ON t TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;
DEFINE FIELD f ON t TYPE string MIGRATE EXISTING ROWS AS fill USING $value ?? 'none';
DEFINE FIELD f ON t MIGRATE EXISTING ROWS AS `two words` USING $value COMMENT "c" PERMISSIONS FULL;
DEFINE FIELD OVERWRITE f ON t TYPE int MIGRATE existing rows as m using (SELECT VALUE 1 FROM ONLY t:1) fallback -1;
-- The clause's keywords stay identifiers everywhere else.
DEFINE FIELD using ON rows TYPE int MIGRATE EXISTING ROWS AS fallback USING using FALLBACK existing;
