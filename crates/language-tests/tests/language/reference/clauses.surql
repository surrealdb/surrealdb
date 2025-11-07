/**
[test]

# 0, 1
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"

# 2, 3, 4
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

# 5, 6, 7
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

# 8
[[test.results]]
value = "[message:1, message:2, message:3]"

# 9
[[test.results]]
value = "[{ id: message:1 }, { id: message:2 }, { id: message:3 }]"

# 10
[[test.results]]
value = "[message:1, message:3]"

# 11
[[test.results]]
value = "[{ text: 'World' }, { text: 'Hello' }, { text: 'Goodbye' }]"

# 12
[[test.results]]
value = "[{ id: message:1 }, { id: message:2 }]"

# 13
[[test.results]]
value = "[{ id: message:2 }, { id: message:3 }]"

# 14
[[test.results]]
value = "[{ id: message:2 }]"

# 15
[[test.results]]
value = "[{ id: message:1, tags: 'urgent' }, { id: message:2, tags: 'info' }, { id: message:2, tags: 'update' }, { id: message:3, tags: 'urgent' }]"

# 16
[[test.results]]
value = "[{ author: user:alice, count: 2 }, { author: user:bob, count: 1 }]"

# 17
[[test.results]]
value = "[{ id: message:3, priority: 3 }, { id: message:1, priority: 1 }]"

*/

-- 0, 1
DEFINE FIELD author ON message TYPE record<user> REFERENCE;
DEFINE FIELD chat ON message TYPE record<chat> REFERENCE;

-- 2, 3, 4
CREATE user:alice SET name = "Alice" RETURN NONE;
CREATE user:bob SET name = "Bob" RETURN NONE;
CREATE chat:general RETURN NONE;

-- 5
CREATE message:1 SET 
    chat = chat:general, 
    author = user:alice, 
    text = "Hello",
    tags = ["urgent"],
    priority = 1
RETURN NONE;

-- 6
CREATE message:2 SET 
    chat = chat:general, 
    author = user:bob, 
    text = "World",
    tags = ["info", "update"],
    priority = 2
RETURN NONE;

-- 7
CREATE message:3 SET 
    chat = chat:general, 
    author = user:alice, 
    text = "Goodbye",
    tags = ["urgent"],
    priority = 3
RETURN NONE;

-- 8
chat:general<~(message FIELD chat);

-- 9
chat:general<~(SELECT id FROM message FIELD chat);

-- 10
chat:general<~(message FIELD chat WHERE author = user:alice);

-- 11
chat:general<~(SELECT text FROM message FIELD chat ORDER BY text DESC);

-- 12
chat:general<~(SELECT id FROM message FIELD chat ORDER BY id LIMIT 2);

-- 13
chat:general<~(SELECT id FROM message FIELD chat ORDER BY id START 1);

-- 14
chat:general<~(SELECT id FROM message FIELD chat ORDER BY id START 1 LIMIT 1);

-- 15
chat:general<~(SELECT id, tags FROM message FIELD chat SPLIT tags);

-- 16
chat:general<~(SELECT author, count() AS count FROM message FIELD chat GROUP BY author ORDER BY author);

-- 17
chat:general<~(SELECT id, priority FROM message FIELD chat WHERE priority IN [1, 3] ORDER BY priority DESC LIMIT 2);

-- TODO alias on lookups seem to be useless...
-- user:alice<~(message FIELD author AS chat_messages);