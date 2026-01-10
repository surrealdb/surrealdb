/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test possible mistaken idiom usage."
wip = true

# 0
[[test.results]]
value = """[{ id: person:alice, name: 'Alice Zhang', "{  }": {  } }]"""

# 1
[[test.results]]
value = "[{ empty_object: {  }, id: person:alice, name: 'Alice Zhang' }]"

# 2, 3, 4: Questionable behavior.
[[test.results]]
value = "[{  }]"
[[test.results]]
value = "[{ name: 'Alice Zhang' }]"
[[test.results]]
value = "[{ id: person:alice, name: 'Alice Zhang' }]"

# 5
[[test.results]]
value = "[{ a: {  }, id: person:alice, name: 'Alice Zhang' }]"

# 6, 7, 8: Questionable behavior.
[[test.results]]
value = "[NONE]"
[[test.results]]
value = "[{ name: 'Alice Zhang' }]"
[[test.results]]
value = "[{ id: person:alice, name: 'Alice Zhang' }]"

# 9, 10
[[test.results]]
value = "[[]]"
[[test.results]]
value = "[[[person:alice]]]"

# 11, 12
[[test.results]]
value = "[[]]"
[[test.results]]
value = "[[]]"

*/

-- 0: Empty object shows up as a string key.
SELECT id, name, {} FROM person WHERE id = person:alice;
-- 1: Instead you probably wanted an alias.
SELECT id, name, {} as empty_object FROM person WHERE id = person:alice;

-- 2, 3, 4: An empty recursion block clobbers anything before it.
SELECT id, name, @.{} FROM person WHERE id = person:alice;
SELECT id, @.{}, name FROM person WHERE id = person:alice;
SELECT @.{}, id, name FROM person WHERE id = person:alice;

-- 5: An empty recursion block with an alias makes `a` an empty object
--    and does not affect anything before it.
SELECT id, name, @.{} AS a FROM person WHERE id = person:alice;

-- 6, 7, 8: An empty recursion block with a repeat recurse symbol clobbers with NONE.
SELECT id, name, @.{..} FROM person WHERE id = person:alice;
SELECT id, @.{..}, name FROM person WHERE id = person:alice;
SELECT @.{..}, id, name FROM person WHERE id = person:alice;

-- 9: Empty path clobbers with empty array.
SELECT id, name, @.{..+path} FROM person WHERE id = person:alice;
-- 10: Empty path with inclusive clobbers with [[person:alice]] array.
SELECT id, name, @.{..+path+inclusive} FROM person WHERE id = person:alice;

-- 11: Empty path with shortest=person:ceo clobbers with [] array.
SELECT id, name, @.{..+shortest=person:ceo} FROM person WHERE id = person:alice;
-- 12: Empty path with shortest=person:ceo+inclusive clobbers with [] array.
SELECT id, name, @.{..+shortest=person:ceo+inclusive} FROM person WHERE id = person:alice;
