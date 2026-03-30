/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY should work correctly when querying edge/relation tables with fields from linked records"
issue = 7188

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ creationDate: d'2026-01-09T00:00:00Z' }, { creationDate: d'2026-01-08T00:00:00Z' }, { creationDate: d'2026-01-07T00:00:00Z' }, { creationDate: d'2026-01-06T00:00:00Z' }, { creationDate: d'2026-01-05T00:00:00Z' }, { creationDate: d'2026-01-04T00:00:00Z' }, { creationDate: d'2026-01-03T00:00:00Z' }, { creationDate: d'2026-01-02T00:00:00Z' }, { creationDate: d'2026-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ creationDate: d'2026-01-01T00:00:00Z' }, { creationDate: d'2026-01-02T00:00:00Z' }, { creationDate: d'2026-01-03T00:00:00Z' }, { creationDate: d'2026-01-04T00:00:00Z' }, { creationDate: d'2026-01-05T00:00:00Z' }, { creationDate: d'2026-01-06T00:00:00Z' }, { creationDate: d'2026-01-07T00:00:00Z' }, { creationDate: d'2026-01-08T00:00:00Z' }, { creationDate: d'2026-01-09T00:00:00Z' }]"

[[test.results]]
value = "[{ creationDate: d'2026-01-09T00:00:00Z' }, { creationDate: d'2026-01-08T00:00:00Z' }, { creationDate: d'2026-01-07T00:00:00Z' }, { creationDate: d'2026-01-06T00:00:00Z' }, { creationDate: d'2026-01-05T00:00:00Z' }, { creationDate: d'2026-01-04T00:00:00Z' }, { creationDate: d'2026-01-03T00:00:00Z' }, { creationDate: d'2026-01-02T00:00:00Z' }, { creationDate: d'2026-01-01T00:00:00Z' }]"
*/

-- Setup: Create records and edges
{
	CREATE media_text_test:0 SET creationDate = d'2025-01-01T00:00:00.000Z';
	CREATE media_text_test:1 SET creationDate = d'2026-01-01T00:00:00.000Z';
	CREATE media_text_test:2 SET creationDate = d'2026-01-02T00:00:00.000Z';
	CREATE media_text_test:3 SET creationDate = d'2026-01-03T00:00:00.000Z';
	CREATE media_text_test:4 SET creationDate = d'2026-01-04T00:00:00.000Z';
	CREATE media_text_test:5 SET creationDate = d'2026-01-05T00:00:00.000Z';
	CREATE media_text_test:6 SET creationDate = d'2026-01-06T00:00:00.000Z';
	CREATE media_text_test:7 SET creationDate = d'2026-01-07T00:00:00.000Z';
	CREATE media_text_test:8 SET creationDate = d'2026-01-08T00:00:00.000Z';
	CREATE media_text_test:9 SET creationDate = d'2026-01-09T00:00:00.000Z';

	RELATE media_text_test:1->is_comment_of->media_text_test:0;
	RELATE media_text_test:2->is_comment_of->media_text_test:0;
	RELATE media_text_test:3->is_comment_of->media_text_test:0;
	RELATE media_text_test:4->is_comment_of->media_text_test:0;
	RELATE media_text_test:5->is_comment_of->media_text_test:0;
	RELATE media_text_test:6->is_comment_of->media_text_test:0;
	RELATE media_text_test:7->is_comment_of->media_text_test:0;
	RELATE media_text_test:8->is_comment_of->media_text_test:0;
	RELATE media_text_test:9->is_comment_of->media_text_test:0;

	RETURN "OK";
};

-- Test 1: ORDER BY alias DESC on edge table with in.field
SELECT
	in.creationDate as creationDate
FROM is_comment_of
WHERE out = media_text_test:0
ORDER BY creationDate DESC;

-- Test 2: ORDER BY alias ASC (should differ from DESC)
SELECT
	in.creationDate as creationDate
FROM is_comment_of
WHERE out = media_text_test:0
ORDER BY creationDate ASC;

-- Test 3: All edges should be returned (9 results), correctly ordered
SELECT
	in.creationDate as creationDate
FROM is_comment_of
WHERE out = media_text_test:0
ORDER BY creationDate DESC;
