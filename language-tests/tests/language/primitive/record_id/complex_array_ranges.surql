/**
[env]
namespace = true
database = true

[test]
reason = "Tests range queries with complex record IDs containing mixed-type arrays (strings + UUIDs)"

[[test.results]]
value = "[{ id: image:['apple', u'00000000-1111-2222-3333-444444444444'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'22222222-3333-4444-5555-666666666666'] }]"

[[test.results]]
value = "[{ id: image:['banana', u'00000000-1111-2222-3333-444444444444'] }]"

[[test.results]]
value = "[{ id: image:['banana', u'11111111-2222-3333-4444-555555555555'] }]"

[[test.results]]
value = "[{ id: image:['cherry', u'00000000-1111-2222-3333-444444444444'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['apple', u'22222222-3333-4444-5555-666666666666'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['apple', u'22222222-3333-4444-5555-666666666666'] }, { id: image:['banana', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['banana', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['cherry', u'00000000-1111-2222-3333-444444444444'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['apple', u'22222222-3333-4444-5555-666666666666'] }]"

[[test.results]]
value = "[{ id: image:['apple', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['apple', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['apple', u'22222222-3333-4444-5555-666666666666'] }, { id: image:['banana', u'00000000-1111-2222-3333-444444444444'] }, { id: image:['banana', u'11111111-2222-3333-4444-555555555555'] }, { id: image:['cherry', u'00000000-1111-2222-3333-444444444444'] }]"

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 6 }]"

*/

-- Create records with mixed-type array IDs combining strings and UUIDs
CREATE image:['apple', u'00000000-1111-2222-3333-444444444444'];
CREATE image:['apple', u'11111111-2222-3333-4444-555555555555'];
CREATE image:['apple', u'22222222-3333-4444-5555-666666666666'];
CREATE image:['banana', u'00000000-1111-2222-3333-444444444444'];
CREATE image:['banana', u'11111111-2222-3333-4444-555555555555'];
CREATE image:['cherry', u'00000000-1111-2222-3333-444444444444'];

-- Test basic range query with partial array matching
SELECT * FROM image:['apple', NONE]..['apple', ..] ORDER BY id;

-- Test inclusive range with specific bounds
SELECT * FROM image:['apple', u'00000000-1111-2222-3333-444444444444']..=['apple', u'11111111-2222-3333-4444-555555555555'] ORDER BY id;

-- Test exclusive range
SELECT * FROM image:['apple', u'00000000-1111-2222-3333-444444444444']>..['apple', u'22222222-3333-4444-5555-666666666666'] ORDER BY id;

-- Test open-ended range from specific point
SELECT * FROM image:['apple', u'11111111-2222-3333-4444-555555555555'].. ORDER BY id;

-- Test range ending at specific point
SELECT * FROM image:..['banana', u'00000000-1111-2222-3333-444444444444'] ORDER BY id;

-- Test range across different first elements
SELECT * FROM image:['apple', NONE]..['cherry', ..] ORDER BY id;

-- Count records with 'apple' begin to cherry begin
SELECT count() FROM image:['apple', NONE]..['cherry', NONE] GROUP ALL;

-- Count records with 'apple' begin to cherry end
SELECT count() FROM image:['apple', NONE]..['cherry', ..] GROUP ALL;
