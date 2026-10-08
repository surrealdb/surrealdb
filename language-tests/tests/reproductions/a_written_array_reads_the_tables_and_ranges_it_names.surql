/**
[env]
namespace = true
database = true

[test]
reason = """
An array written in `FROM` is, by the intended contract, a list of sources as
`FROM a, b` is: an element that is a table reads the table, and one that is a
record range reads the range. It is read as one value source instead: a record
id element is fetched and a nested array contributes its elements, but a table
element is yielded as the table's name and a range element yields nothing. This
file pins that current answer.

Reading a table or a range where a value source yields its elements needs the
scan such a source is planned with, under the table's permissions and computed
fields, which the value source does not carry.
"""

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:carol, name: 'Carol' }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[]"

*/
CREATE person:bob SET name = 'Bob';
CREATE person:carol SET name = 'Carol';
SELECT VALUE name FROM [person];
SELECT VALUE name FROM [type::table('person')];
SELECT VALUE name FROM [person:bob..];
