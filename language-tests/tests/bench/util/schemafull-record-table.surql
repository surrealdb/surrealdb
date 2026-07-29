-- Schema-rich `record` table for write-path benches: typed fields with
-- DEFAULT / VALUE / ASSERT / field permissions, an event, and a composite
-- index, so per-record definition processing (type coercion, clause
-- evaluation, event triggering, index maintenance) shows up in the
-- measurement instead of benching a bare schemaless insert.
DEFINE TABLE record SCHEMAFULL PERMISSIONS FOR select, create, update, delete WHERE age < 200;
DEFINE FIELD text ON record TYPE string;
DEFINE FIELD name ON record TYPE string ASSERT string::len($value) > 0;
DEFINE FIELD age ON record TYPE int DEFAULT 0;
DEFINE FIELD city ON record TYPE string PERMISSIONS FOR select WHERE age < 150;
DEFINE FIELD score ON record TYPE float VALUE math::round($value * 100) / 100;
DEFINE FIELD active ON record TYPE bool DEFAULT true;
DEFINE FIELD created_at ON record TYPE datetime;
DEFINE FIELD status ON record TYPE string ASSERT $value IN ['draft', 'published', 'archived'];
DEFINE FIELD tags ON record TYPE array<string>;
DEFINE FIELD geography ON record FLEXIBLE TYPE object;
DEFINE EVENT audit ON record WHEN $event = 'CREATE' THEN { RETURN NONE };
DEFINE INDEX record_name_age ON record FIELDS name, age;
