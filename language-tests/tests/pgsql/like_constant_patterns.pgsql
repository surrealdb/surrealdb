/**
[env]
namespace = true
database = true

[test]
reason = "LIKE over a pattern and a subject that are both literals: % and _ are wildcards, a backslash makes the next character literal, a line break is a character, case matters for LIKE and not for ILIKE, and NULL gives NULL"

[[test.results]]
value = "[{ backslash_then_wildcard: true, literal_backslash: true, literal_percent: true, literal_underscore: true, other_escape: true, wild_percent: false, wild_underscore: false }]"

[[test.results]]
value = "[{ a_line_break_is_not_ignored: false, percent_spans_lines: true, underscore_spans_lines: true }]"

[[test.results]]
value = "[{ null_negated: NULL, null_pattern: NULL, null_subject: NULL }]"

[[test.results]]
value = "[{ ilike_is_not: true, like_is_case_sensitive: false, negated: true }]"
*/

SELECT
	'50%' LIKE '50\%' AS literal_percent,
	'500' LIKE '50\%' AS wild_percent,
	'a_b' LIKE 'a\_b' AS literal_underscore,
	'axb' LIKE 'a\_b' AS wild_underscore,
	'a\b' LIKE 'a\\b' AS literal_backslash,
	'a\xyz' LIKE 'a\\%' AS backslash_then_wildcard,
	'axb' LIKE 'a\xb' AS other_escape;
SELECT
	E'a\nb' LIKE 'a%b' AS percent_spans_lines,
	E'a\nb' LIKE 'a_b' AS underscore_spans_lines,
	E'a\n' LIKE 'a' AS a_line_break_is_not_ignored;
SELECT
	NULL LIKE 'a' AS null_subject,
	'a' LIKE NULL AS null_pattern,
	NULL NOT LIKE 'a' AS null_negated;
SELECT
	'abc' LIKE 'ABC' AS like_is_case_sensitive,
	'abc' ILIKE 'ABC' AS ilike_is_not,
	'abc' NOT LIKE 'ABC' AS negated;
