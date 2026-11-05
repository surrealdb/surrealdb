/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_not_null_domain.surql"]

[test]
reason = "A domain's base type is reported in pg_type.typbasetype and information_schema.domains as a column declared with that type is: its zone, array dimensions, float precision, network type, json flavour, or the user enum it names. A composite attribute of an enum has the enum's OID in pg_attribute, and a column of a NOT NULL domain over json is not nullable"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ typbasetype: 869, typname: 'd_inet' }, { typbasetype: 114, typname: 'd_json' }, { typbasetype: 114, typname: 'd_json_nn' }, { typbasetype: 700, typname: 'd_real' }, { typbasetype: 1184, typname: 'd_tstz' }, { typbasetype: 1015, typname: 'd_vcarr' }]"

[[test.results]]
value = "[{ over_mood: true }]"

[[test.results]]
value = "[{ data_type: 'inet', domain_name: 'd_inet', udt_name: 'inet' }, { data_type: 'json', domain_name: 'd_json', udt_name: 'json' }, { data_type: 'json', domain_name: 'd_json_nn', udt_name: 'json' }, { data_type: 'USER-DEFINED', domain_name: 'd_mood', udt_name: 'mood' }, { data_type: 'real', domain_name: 'd_real', udt_name: 'float4' }, { data_type: 'timestamp with time zone', domain_name: 'd_tstz', udt_name: 'timestamptz' }, { data_type: 'ARRAY', domain_name: 'd_vcarr', udt_name: '_varchar' }]"

[[test.results]]
value = "[{ attname: 'label', is_mood: false }, { attname: 'm', is_mood: true }]"

[[test.results]]
value = "[{ column_name: 'body', is_nullable: 'NO' }]"

[[test.results]]
value = "[{ attname: 'body', attnotnull: true }]"

*/

CREATE TYPE mood AS ENUM ('sad', 'happy');

CREATE DOMAIN d_tstz AS timestamp(3) with time zone;

CREATE DOMAIN d_vcarr AS varchar(10)[];

CREATE DOMAIN d_real AS float(10);

CREATE DOMAIN d_inet AS inet;

CREATE DOMAIN d_json AS json;

CREATE DOMAIN d_mood AS mood;

CREATE TYPE feeling AS (label text, m mood);

SELECT t.typname, t.typbasetype FROM pg_type t WHERE t.typtype = 'd' AND t.typname <> 'd_mood' ORDER BY t.typname;

SELECT t.typbasetype = (SELECT oid FROM pg_type WHERE typname = 'mood') AS over_mood FROM pg_type t WHERE t.typname = 'd_mood';

SELECT domain_name, data_type, udt_name FROM information_schema.domains ORDER BY domain_name;

SELECT a.attname, a.atttypid = (SELECT oid FROM pg_type WHERE typname = 'mood') AS is_mood FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'feeling' ORDER BY a.attnum;

SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'doc' AND column_name = 'body';

SELECT a.attname, a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'doc' AND a.attname = 'body';
