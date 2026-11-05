/**
[test]
run = false
reason = "Setup for cast_to_text: a table with a field of each family whose text form is not its stored kind's, so a request that holds no DDL lowers against their declared types"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE v SCHEMAFULL;
DEFINE FIELD id ON v TYPE int;
DEFINE FIELD d ON v TYPE option<date> SQL TYPE POSTGRESQL 'date';
DEFINE FIELD t ON v TYPE option<time> SQL TYPE POSTGRESQL 'time';
DEFINE FIELD ts ON v TYPE option<timestamp> SQL TYPE POSTGRESQL 'timestamp';
DEFINE FIELD tz ON v TYPE option<datetime> SQL TYPE POSTGRESQL 'timestamptz';
DEFINE FIELD iv ON v TYPE option<interval> SQL TYPE POSTGRESQL 'interval';
DEFINE FIELD n ON v TYPE option<decimal> SQL TYPE POSTGRESQL 'numeric(10,2)';
DEFINE FIELD big ON v TYPE option<bigdecimal> SQL TYPE POSTGRESQL 'numeric(40,5)';
DEFINE FIELD m ON v TYPE option<mood>;
DEFINE FIELD r ON v TYPE option<range> SQL TYPE POSTGRESQL 'int4range';
DEFINE FIELD a ON v TYPE option<string> SQL TYPE POSTGRESQL 'inet';
DEFINE FIELD b ON v TYPE option<bool> SQL TYPE POSTGRESQL 'boolean';

CREATE v:1 SET d = pd'2026-10-27', t = pt'12:30:05.5', ts = ts'2026-10-27T12:30:00.25', tz = d'2026-10-27T12:30:00Z', iv = ivl'1 year 2 mons 3 days 04:05:06', n = 1.50dec, big = <bigdecimal> '12345678901234567890123456789012345.12345', m = 'ok', r = 1i32..6i32, a = '192.168.1.5', b = true RETURN NONE;
CREATE v:2 SET iv = ivl'-1 days +00:00:05', n = -0.50dec, big = <bigdecimal> '0.00000', m = 'sad', r = 0i32..0i32, a = '10.0.0.0/8', b = false RETURN NONE;
