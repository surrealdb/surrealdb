/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "$parent->table correlated subquery returns empty results"
issue = 7133

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
value = "[{ id: bug_user:alice, name: 'alice' }]"

[[test.results]]
value = "[{ code: 'en', id: bug_locale:en }]"

[[test.results]]
value = "[{ code: 'zh', id: bug_locale:zh }]"

[[test.results]]
value = "[{ id: bug_speak:placeholder, in: bug_user:alice, is_default: false, out: bug_locale:en }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: bug_speak:placeholder, in: bug_user:alice, is_default: true, out: bug_locale:zh }]"
skip-record-id-key = true

[[test.results]]
value = "[{ locales: [{ code: 'en' }, { code: 'zh' }] }]"

[[test.results]]
value = "[{ code: 'en' }, { code: 'zh' }]"

[[test.results]]
value = "[{ code: 'en' }, { code: 'zh' }]"
*/

DEFINE TABLE bug_user SCHEMAFULL;
DEFINE FIELD name ON bug_user TYPE string;

DEFINE TABLE bug_locale SCHEMAFULL;
DEFINE FIELD code ON bug_locale TYPE string;

DEFINE TABLE bug_speak SCHEMAFULL TYPE RELATION IN bug_user OUT bug_locale;
DEFINE FIELD is_default ON bug_speak TYPE option<bool>;
DEFINE INDEX bug_speak_idx ON bug_speak FIELDS in, out UNIQUE;

CREATE bug_user:alice SET name = 'alice';
CREATE bug_locale:en SET code = 'en';
CREATE bug_locale:zh SET code = 'zh';

RELATE bug_user:alice -> bug_speak -> bug_locale:en CONTENT {
    in: bug_user:alice, out: bug_locale:en, is_default: false
};
RELATE bug_user:alice -> bug_speak -> bug_locale:zh CONTENT {
    in: bug_user:alice, out: bug_locale:zh, is_default: true
};

-- Correlated subquery: $parent->relation must resolve to outer row
SELECT (
    SELECT out.code AS code FROM $parent->bug_speak ORDER BY code
) AS locales
FROM bug_user:alice;

-- Baseline: direct query on relation table
SELECT out.code AS code FROM bug_speak WHERE in = bug_user:alice ORDER BY code;

-- Baseline: graph traversal outside subquery
SELECT out.code AS code FROM bug_user:alice->bug_speak ORDER BY code;
