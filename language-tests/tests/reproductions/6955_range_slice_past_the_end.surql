/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A range slice whose bound lies past the end selects what the array has, on a plain array
and on a graph traversal alike, rather than returning NONE.
"""
issue = 6955

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ memories: [{ content: 'a', id: memory:1 }, { content: 'b', id: memory:2 }] }]"

[[test.results]]
value = "[{ memories: [memory:1, memory:2] }]"

[[test.results]]
value = "[{ memories: [memory:2] }]"

[[test.results]]
value = "[{ memories: [memory:1] }]"
*/

RETURN [1, 2][0..10];
RETURN [1, 2][0..=10];
RETURN [1, 2][1..10];
RETURN [1, 2][5..10];
RETURN [1, 2][..10];

CREATE project:1 RETURN NONE;
CREATE memory:1 SET content = 'a' RETURN NONE;
CREATE memory:2 SET content = 'b' RETURN NONE;
RELATE project:1->has_memory:1->memory:1 RETURN NONE;
RELATE project:1->has_memory:2->memory:2 RETURN NONE;

SELECT ->has_memory->memory[0..10].{id, content} AS memories FROM project:1;
SELECT ->has_memory->memory[0..10] AS memories FROM project:1;
SELECT ->has_memory->memory[1..10] AS memories FROM project:1;
SELECT ->has_memory->memory[0..1] AS memories FROM project:1;
