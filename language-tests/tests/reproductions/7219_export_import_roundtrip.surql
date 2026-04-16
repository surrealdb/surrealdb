/**
[env]
namespace = true
database = true

[test]
reason = "Export/import roundtrip should handle compound record IDs, backslash escaping, and multi-line content"
issue = 7219

[[test.results]]
value = "[{ content: 'Introduction to quantum computing...', id: block:[document:`a1b2c3d4-e5f6-7890-abcd-ef1234567890`, '/page/0/Text/13'], type: 'paragraph' }]"

[[test.results]]
value = "[{ content: 'Introduction to quantum computing...', id: block:[document:`a1b2c3d4-e5f6-7890-abcd-ef1234567890`, '/page/0/Text/13'], type: 'paragraph' }]"

[[test.results]]
value = '''[{ content: 'The equation uses \\boldsymbol{x} and \\sum_{i=0}', id: document:abc }]'''

[[test.results]]
value = '''[{ content: 'The equation uses \\boldsymbol{x} and \\sum_{i=0}', id: document:abc }]'''

[[test.results]]
value = '''[{ body: 'Line 1\nLine 2\nLine 3', id: article:1 }]'''

[[test.results]]
value = '''[{ body: 'Line 1\nLine 2\nLine 3', id: article:1 }]'''

[[test.results]]
value = "[{ content: 'Second block', id: block:[document:def456, '/page/1/Text/0'], type: 'heading' }]"

[[test.results]]
value = "[{ content: 'Introduction to quantum computing...', id: block:[document:`a1b2c3d4-e5f6-7890-abcd-ef1234567890`, '/page/0/Text/13'], type: 'paragraph' }, { content: 'Second block', id: block:[document:def456, '/page/1/Text/0'], type: 'heading' }]"

*/

-- Test 1: Compound Array Record IDs
CREATE block:[document:⟨a1b2c3d4-e5f6-7890-abcd-ef1234567890⟩, '/page/0/Text/13'] SET content = 'Introduction to quantum computing...', type = 'paragraph';
SELECT * FROM block;

-- Test 2: Backslash escaping in string content
CREATE document:⟨abc⟩ SET content = 'The equation uses \\boldsymbol{x} and \\sum_{i=0}';
SELECT * FROM document WHERE id = document:⟨abc⟩;

-- Test 3: Multi-line string content
CREATE article:1 SET body = 'Line 1
Line 2
Line 3';
SELECT * FROM article;

-- Test 4: INSERT with compound array IDs
INSERT { id: block:[document:⟨def456⟩, '/page/1/Text/0'], content: 'Second block', type: 'heading' };
SELECT * FROM block ORDER BY id;
