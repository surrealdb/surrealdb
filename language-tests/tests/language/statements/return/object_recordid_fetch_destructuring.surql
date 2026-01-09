/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests UPSERT with multiple records, LET with objects containing record IDs, RETURN with FETCH and object destructuring"

[[test.results]]
value = "[{ id: test:1, test: 'some text' }, { id: test:2, test: 'some text' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ one: test:1, two: test:2 }"

[[test.results]]
value = "{ one: { id: test:1, test: 'some text' }, two: test:2 }"

[[test.results]]
value = "{ one: { id: test:1, test: 'some text' }, two: test:2 }"

[[test.results]]
value = "[{ age: 25, id: person:alice, name: 'test user' }, { age: 25, id: person:bob, name: 'test user' }, { age: 25, id: person:charlie, name: 'test user' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ admin: person:alice, count: 3, editor: person:bob, viewer: person:charlie }"

[[test.results]]
value = "{ admin: { age: 25, id: person:alice, name: 'test user' }, count: 3, editor: { age: 25, id: person:bob, name: 'test user' }, viewer: person:charlie }"

[[test.results]]
value = "{ admin: { age: 25, id: person:alice, name: 'test user' }, count: 3, editor: { age: 25, id: person:bob, name: 'test user' }, viewer: { age: 25, id: person:charlie, name: 'test user' } }"

[[test.results]]
value = "{ admin: { age: 25, id: person:alice, name: 'test user' }, count: 3, editor: 'test user', viewer: person:charlie }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ meta: { active: true, total: 2 }, primary: { role: 'admin', user: person:alice }, secondary: { role: 'editor', user: person:bob } }"

[[test.results]]
value = "{ meta: { active: true, total: 2 }, primary: { role: 'admin', user: { age: 25, id: person:alice, name: 'test user' } }, secondary: { role: 'editor', user: { age: 25, id: person:bob, name: 'test user' } } }"

[[test.results]]
value = "{ meta: { active: true, total: 2 }, primary: { role: 'admin', user: { age: 25, id: person:alice, name: 'test user' } }, secondary: 'test user' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ member: person:alice, position: 'lead' }, { member: person:bob, position: 'developer' }, { member: person:charlie, position: 'designer' }]"

[[test.results]]
value = "[{ member: { age: 25, id: person:alice, name: 'test user' }, position: 'lead' }, { member: { age: 25, id: person:bob, name: 'test user' }, position: 'developer' }, { member: { age: 25, id: person:charlie, name: 'test user' }, position: 'designer' }]"

[[test.results]]
value = "[{ member: 'test user', position: 'lead' }, { member: 'test user', position: 'developer' }, { member: 'test user', position: 'designer' }]"

[[test.results]]
value = "[{ id: test:1, test: 'some text' }, { id: test:2, test: 'some text' }]"

[[test.results]]
value = "[{ age: 25, id: person:alice, name: 'test user' }, { age: 25, id: person:bob, name: 'test user' }, { age: 25, id: person:charlie, name: 'test user' }]"

*/

-- UPSERT multiple records in a single statement
UPSERT test:1, test:2 SET test = "some text";

-- Create an object containing record IDs as field values
LET $temp = { one: test:1, two: test:2 };

-- Return the object containing record IDs
RETURN $temp;

-- Return the object with FETCH on one field
RETURN $temp FETCH one;

-- Return with object destructuring and field expansion
RETURN $temp.{ one: one.*, two };

-- Additional test patterns for completeness

-- Test with different field names and more records
UPSERT person:alice, person:bob, person:charlie SET name = "test user", age = 25;

-- Create complex object with multiple record ID fields
LET $users = {
    admin: person:alice,
    editor: person:bob,
    viewer: person:charlie,
    count: 3
};

-- Test basic object return
RETURN $users;

-- Test FETCH on multiple fields
RETURN $users FETCH admin, editor;

-- Test FETCH on all record fields
RETURN $users FETCH admin, editor, viewer;

-- Test object destructuring with multiple expansions
RETURN $users.{
    admin: admin.*,
    editor: editor.name,
    viewer,
    count
};

-- Test nested object with record IDs
LET $nested = {
    primary: { user: person:alice, role: "admin" },
    secondary: { user: person:bob, role: "editor" },
    meta: { total: 2, active: true }
};

RETURN $nested;
RETURN $nested FETCH primary.user, secondary.user;
RETURN $nested.{
    primary: primary.{ user: user.*, role },
    secondary: secondary.user.name,
    meta
};

-- Test array of objects with record IDs
LET $team = [
    { member: person:alice, position: "lead" },
    { member: person:bob, position: "developer" },
    { member: person:charlie, position: "designer" }
];

RETURN $team;
RETURN $team FETCH member;
RETURN $team.{ member: member.name, position };

-- Final verification - check all created records
SELECT * FROM test;
SELECT * FROM person;
