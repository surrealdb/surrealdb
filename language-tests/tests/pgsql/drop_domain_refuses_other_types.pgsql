/**
[env]
namespace = true
database = true

[test]
reason = "DROP DOMAIN refuses an enum and a composite type, and DROP TYPE refuses a domain, leaving each type in place; each is removed by the statement for its own kind"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The type 'mood' is not a domain; remove it with DROP TYPE"

[[test.results]]
error = "The type 'pair' is not a domain; remove it with DROP TYPE"

[[test.results]]
error = "The type 'd' is a domain; remove it with DROP DOMAIN"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ typname: 'd' }, { typname: 'mood' }, { typname: 'pair' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

CREATE TYPE mood AS ENUM ('sad', 'happy');

CREATE TYPE pair AS (a int);

CREATE DOMAIN d AS int;

DROP DOMAIN mood;

DROP DOMAIN pair;

DROP TYPE d;

DROP DOMAIN IF EXISTS missing;

DROP TYPE IF EXISTS missing;

SELECT typname FROM pg_type WHERE typname IN ('mood', 'pair', 'd') ORDER BY typname;

DROP TYPE mood;

DROP TYPE pair;

DROP DOMAIN d;

SELECT typname FROM pg_type WHERE typname IN ('mood', 'pair', 'd') ORDER BY typname;
